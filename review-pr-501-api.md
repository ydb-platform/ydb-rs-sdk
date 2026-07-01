# Ревью PR #501 с точки зрения пользовательского API

[PR #501](https://github.com/ydb-platform/ydb-rs-sdk/pull/501) — сильный шаг к «driver-first» модели, близкой к ydb-go-sdk. Ниже — оценка по трём осям: ожидаемость, удобство сейчас/в будущем, и запас на рефакторинг (в т.ч. разделение пулов).

---

## Краткий вердикт

| Критерий | Оценка |
|----------|--------|
| Ожидаемость | **7/10** — логика «пул на драйвере» правильная, но есть сюрпризы |
| Удобство сейчас | **6/10** — проще для смешанных workload, сложнее для table-only и миграции |
| Удобство в будущем | **8/10** — driver-level `SessionPoolSettings` + sub-clients без pool API дают хороший запас на внутренний рефакторинг |
| Запас на split пулов без BC (API) | **8/10** — сигнатуры можно не менять; split = внутренняя деталь `with_session_pool` |
| Запас на split пулов без BC (поведение) | **6/10** — смена семантики `limit` (shared → per-client) без смены API всё равно surprise для пользователей |

---

## 1. Насколько API ожидаемое

### Что совпадает с интуицией

**Централизация на `Client`** — правильное направление. Пул — ресурс соединения/сессии, а не свойство `TableClient` / `QueryClient`:

```rust
/// Replace the driver session pool (CreateSession + AttachSession) and optionally warm it up.
///
/// Table and query clients created from this driver share the same pool.
pub async fn with_session_pool(self, settings: SessionPoolSettings) -> YdbResult<Self>
```

**Переименование** `QuerySessionPoolSettings` → `SessionPoolSettings` логично: пул больше не «query-специфичный».

**Симметрия с go-sdk на уровне «настраивай на драйвере»** — узнаваемо для мигрирующих с Go.

**Убран `QuerySessionMode` на уровне клиента** — разумно: режим сессии стал per-call через `.with_implicit_session()`, а не глобальным флагом клиента. Это ближе к «явный выбор на каждый запрос».

### Что неожиданно / ломает ожидания

**1. Один общий пул ≠ go-sdk.**

В go-sdk `WithSessionPoolSizeLimit(50)` задаёт лимит **отдельно** для table и query (до 50+50 = 100 сессий). В rs-sdk `limit: 50` — это **один семафор на всё**. Это важное расхождение, его нет в PR description как явное отличие от go-sdk.

**2. Дефолт 1000 → 50** — самый болезненный сюрприз для table-only пользователей:

> The built-in session pool defaults to a limit of **50** concurrent sessions (shared by table and query clients). The legacy table-only pool used **1000**; use `with_session_pool` with an explicit limit when migrating high-concurrency workloads.

Документация есть, но это **поведенческий breaking change без compile-time сигнала**. Пользователь, который никогда не вызывал `with_max_active_sessions`, получит деградацию под нагрузкой.

**3. Порядок `with_timeouts` → `with_session_pool`** — неочевидный footgun. В Rust builder-цепочках порядок обычно не важен; здесь важен, и это нужно кричать в migration guide.

**4. PR description обещает deprecated alias `QuerySessionPoolSettings`, в коде его нет** — grep по репо не находит. Для миграции это минус: compile errors вместо deprecation warnings.

**5. Убран `with_implicit_session_pool` на клиенте**, implicit остался только per-call. Для пользователей, которые настраивали concurrency через implicit pool, путь миграции менее очевиден, чем для explicit pool.

---

## 2. Насколько API удобное сейчас

### Плюсы

| Сценарий | Почему удобно |
|----------|---------------|
| Table + Query на одном `Client` | Один `with_session_pool`, один `session_pool_stats()`, нет случайных split pools |
| Смешанный workload | Общий лимит = честное распределение ресурсов, нельзя «съесть» 1000 table-сессий и задушить query |
| Настройки пула | Богатый `SessionPoolSettings`: limit, warm_up, usage limits, idle_ttl, create/delete timeouts — паритет с go-sdk pool |
| Query implicit sessions | `.with_implicit_session()` на builder — тонкая настройка без отдельного режима клиента |
| Table client API | `TableClient` остался про транзакции/retry/idempotent — пул «под капотом», не отвлекает |

Типичный happy path после миграции читается хорошо:

```rust
let client = ClientBuilder::new_from_connection_string(url)?
    .client()?
    .with_timeouts(timeouts)  // до with_session_pool!
    .with_session_pool(SessionPoolSettings::new()
        .with_limit(200)
        .with_warm_up(50))
    .await?;

let table = client.table_client();
let query = client.query_client();
```

### Минусы

**Двухшаговая инициализация.** `ClientBuilder` → sync `client()` → async `with_session_pool()`. Для table-only раньше хватало sync `table_client().with_max_active_sessions(100)` — теперь async на драйвере. Для Rust это терпимо, но шумнее.

**Нет конфигурации пула в `ClientBuilder`.** SLO-тесты делают 4 вызова подряд; в production это повторяется везде. Удобнее было бы `ClientBuilder::with_session_pool_settings(...)` с lazy init при первом `client()`.

**Нельзя настроить table и query по-разному (разные лимиты).** Один `SessionPoolSettings` на драйвер — это нормально, если в будущем под капотом появятся два пула с **одинаковыми** настройками (как в go-sdk). Но сценарий «table — 900, query — 50» сейчас недоступен и потребует расширения `SessionPoolSettings` или отдельного API.

**`with_session_pool` заменяет пул целиком** (consume `self`, новый `SessionPool`). Нельзя «докрутить» лимит у уже созданного `TableClient` — нужно пересоздавать драйвер и заново брать sub-clients.

**Статистика только агрегированная** — `client.session_pool_stats()`. Нельзя понять, кто «съел» пул: table или query. Для отладки contention это неудобно.

---

## 3. Удобство в будущем и запас на split пулов

### Внутренняя архитектура — хороший фундамент

Разделение слоёв удачное:

```
Client
  └── SessionPool (сейчас один; в будущем может быть два с одинаковыми settings)
        ├── QueryClient  → acquire_explicit()
        └── TableSessionPool → обёртка → Session → node-pinned Table RPCs
```

`TableSessionPool` — тонкий адаптер над источником сессий, не дублирует логику пула. Sub-clients (`table_client()`, `query_client()`) **не** принимают pool settings — это ключевое: внутренняя схема «один пул vs два пула» остаётся за `Client`.

### Позволяет ли API в будущем разделить пулы без изменения сигнатур?

**Да, в основном вы правы.** Текущий внешний API для этого подходит:

```rust
let client = client
    .with_session_pool(
        SessionPoolSettings::new()
            .with_limit(pool_limit)
            .with_warm_up(pool_limit)
            .with_session_create_timeout(session_rpc_timeout)
            .with_session_delete_timeout(session_rpc_timeout),
    )
    .await?;

let table = client.table_client();
let query = client.query_client();
```

Пользователь передаёт **один** `SessionPoolSettings` на драйвер. В будущем `with_session_pool` может без смены сигнатуры:

- создать **два** независимых `SessionPool` с **одинаковыми** настройками (модель go-sdk: `limit` per client);
- отдать query-клиенту один пул, table-клиенту другой;
- агрегировать `session_pool_stats()` как сумму двух пулов — сигнатура `Client::session_pool_stats()` тоже может остаться.

То есть split — **implementation detail** внутри `Client`, а не обязательное расширение публичного API. Убрать pool config с `TableClient` / `QueryClient` как раз и было нужно, чтобы не привязывать пользователя к «один пул на sub-client».

#### Что при этом **не** требует смены API

| Изменение под капотом | Публичный API |
|-----------------------|---------------|
| 1 shared pool → 2 pools с одинаковыми `SessionPoolSettings` | `with_session_pool(settings)` без изменений |
| Table снова на native Table CreateSession | `table_client()` / `query_client()` без изменений |
| Другой keepalive / attach path | без изменений на уровне sub-clients |
| `session_pool_stats()` = sum двух пулов | та же функция, другая агрегация |

#### Где остаются риски (не API, а поведение и docs)

1. **Семантика `limit` сейчас vs в будущем.** Сейчас `limit: 50` = один семафор на table+query вместе. Если позже те же `settings` породят два пула по 50 (как go-sdk), **сигнатура та же**, но фактическая ёмкость вырастет (~2×). Это behavioral change, не compile-time break — его нужно явно описать в changelog/docs при таком переходе.

2. **Разные настройки для table и query** — split с **одинаковыми** settings API не блокирует; split с **разными** лимитами (900 table / 50 query) потребует расширения `SessionPoolSettings` или нового метода. Это отдельный, более редкий сценарий.

3. **Документация «always share one pool».** Формулировка в rustdoc сейчас описывает **текущую** реализацию, но звучит как вечный контракт. Лучше: «currently shares one pool; settings apply at driver level» — без «always».

4. **Контракт сессий Query vs Table** (замечание rekby) — по-прежнему implementation detail: table на query session ID сейчас не закреплён в типах API и может смениться без смены `with_session_pool`.

5. **`#[non_exhaustive]`** на settings/stats — nice-to-have для *разных* per-client лимитов и детальной статистики, но **не обязателен** для split с дублированием одних и тех же settings.

### Рекомендация по контракту (в духе rekby)

В публичной документации `Client` лучше зафиксировать:

- **Стабильно (API):** pool настраивается только на `Client` через `SessionPoolSettings`; sub-clients лишь берут сессии из драйвера.
- **Текущее поведение:** table и query делят один пул; `limit` — общий потолок concurrent sessions.
- **Implementation detail (может меняться):** как именно создаются/привязываются сессии (query attach vs table create), один пул vs два с одинаковыми settings.

Так split пулов остаётся совместимым с текущим API, а rekby-вопрос отделяется от формы `with_session_pool`.

---

## 4. Сравнение «до / после» для типичных пользователей

| Пользователь | До | После | Удобство |
|--------------|-----|-------|----------|
| Только table, дефолтный пул | Неявно 1000 сессий | Неявно 50 shared | **Хуже** без миграции |
| Только table, явный лимит | `table_client().with_max_active_sessions(n)` sync | `client.with_session_pool(...).await?` async | **Нейтрально** |
| Только query, implicit mode | Default, без пула | Default = pooled; implicit per-call | **Нейтрально** (если знать про `.with_implicit_session()`) |
| Table + Query | Два независимых пула, легко «разъехаться» | Один пул, предсказуемо | **Лучше** |
| Observability | `query_client().session_pool_stats()` | `client.session_pool_stats()` | **Лучше** (единая точка) |
| Go-sdk миграция | — | Похожий driver-level config, но **другая семантика лимита** | **Путаница** |

---

## 5. Конкретные рекомендации по улучшению API (без изменения архитектуры)

1. **Добавить deprecated aliases**, как обещано в PR: `pub type QuerySessionPoolSettings = SessionPoolSettings` и т.д.
2. **`#[non_exhaustive]`** на `SessionPoolSettings` и `SessionPoolStats` — опционально, для будущих *разных* per-client лимитов и детальной stats; для split с одинаковыми settings не обязателен.
3. **В migration guide** — таблица «rs-sdk shared limit vs go-sdk per-client limit».
4. **`ClientBuilder::with_session_pool_settings`** — убрать async boilerplate из каждого примера.
5. **Changelog entry** с bold warning про default 1000→50.
6. **Документировать** `.with_implicit_session()` как замену `QuerySessionMode::Implicit` + `with_implicit_session_pool`.
7. **Опционально:** поля в stats (`table_in_use` / `query_in_use`) — для observability после внутреннего split; агрегированный `session_pool_stats()` может остаться без новых методов.

---

## Итог

PR делает API **более цельным и предсказуемым** для основного сценария «один драйвер — table и query вместе». Это совпадает с духом go-sdk и убирает класс багов со split pools.

Для **table-only high-concurrency** и для **миграции без сюрпризов** API сейчас жёстче, чем нужно: смена дефолта, async setup, отсутствие deprecated aliases.

Для **будущего split пулов** текущий API **не мешает**: один `Client::with_session_pool(SessionPoolSettings)` может внутри создать два пула с теми же настройками — как в SLO (`storage.rs`), без смены сигнатур и без pool API на sub-clients. Главное — не зашить в docs «always share one pool» как вечный инвариант и явно описать **текущую** семантику `limit` (shared total), чтобы при переходе на per-client pools (go-sdk-style) не было тихого behavioral surprise.
