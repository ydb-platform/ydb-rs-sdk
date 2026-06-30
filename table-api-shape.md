# Table API — целевой облик (черновик для согласования)

Документ фиксирует, **где** должна жить каждая Table Service RPC в публичном API Rust SDK.
Текущая реализация может расходиться — это целевое состояние для рефакторинга.

## Принципы

| Слой | Роль |
|------|------|
| **`TableClient`** | Retry, sessionless RPC, DDL, describe, copy, explain/scheme, read rows, bulk upsert, autocommit, `retry_transaction`. **Без** prepare/prepared execute и stream RPC — они только на сессии. |
| **`Session`** | Узкий «исполнитель» на привязанной к ноде сессии: prepare / execute data query (в т.ч. prepared), стримы, lazy begin tx внутри retrier. |
| **`Transaction`** (на сессии) | Жизненный цикл интерактивной транзакции: commit / rollback. Запросы идут через сессию (`ExecuteDataQuery`). |

**`BeginTransaction`:** явного RPC в публичном API нет — только lazy `BeginTx` в `ExecuteDataQuery`, orchestration в retrier (autocommit / `retry_transaction` на клиенте).

**`KeepAlive`:** не нужен — idle-сессии держит attach stream в Query API / пул.

**`CreateSession` / `DeleteSession`:** не публичный API пользователя; внутри пула.

---

## Сводная таблица: RPC → слой API

| RPC | Client | Session | Transaction | Комментарий |
|-----|:------:|:-------:|:-----------:|-------------|
| CreateSession | (pool) | — | — | Внутренний пул, не публичный API |
| DeleteSession | (pool) | — | — | Внутренний пул |
| KeepAlive | — | — | — | Не реализуем |
| **CreateTable** | ✓ | ✗ | ✗ | Только клиент |
| **DropTable** | ✓ | ✗ | ✗ | |
| **AlterTable** | ✓ | ✗ | ✗ | |
| **CopyTable** | ✓ | ✗ | ✗ | |
| **CopyTables** | ✓ | ✗ | ✗ | |
| **RenameTables** | ✓ (цель) | ✗ | ✗ | Сейчас ✗ везде |
| **DescribeTable** | ✓ | ✗ | ✗ | |
| **ExplainDataQuery** | ✓ | ✗ | ✗ | С сессии убрать полностью (в т.ч. `pub(crate)`) |
| **PrepareDataQuery** | ✗ | ✓ | ✗ | Только сессия |
| **ExecuteDataQuery** | (autocommit)* | ✓ | (via tx) | *Autocommit на клиенте — OK; публичный RPC-метод только на сессии |
| **ExecuteSchemeQuery** | ✓ | ✗ | ✗ | С сессии убрать полностью (в т.ч. `pub(crate)`) |
| **BeginTransaction** | lazy | lazy | ✗ | Lazy `BeginTx` в retrier, без явного RPC |
| **CommitTransaction** | ✗ | ✗ | ✓ | Только транзакция |
| **RollbackTransaction** | ✗ | ✗ | ✓ | Только транзакция |
| **DescribeTableOptions** | ✓ | ✗ | ✗ | |
| **StreamReadTable** | ✗ | ✓ | ✗ | Только сессия; `retry_stream_read_table` на клиенте убрать |
| **ReadRows** | ✓ | ✗ | ✗ | Sessionless (`session_id=""`) |
| **BulkUpsert** | ✓ | ✗ | ✗ | Sessionless |
| **StreamExecuteScanQuery** | ✗ | ✓ | ✗ | Только сессия; `retry_execute_scan_query` на клиенте убрать |
| **DescribeExternalDataSource** | ✓ (цель) | ✗ | ✗ | Сейчас ✗ |
| **DescribeExternalTable** | ✓ (цель) | ✗ | ✗ | Сейчас ✗ |
| **DescribeSystemView** | ✓ (цель) | ✗ | ✗ | Сейчас ✗ |

---

## Публичные методы по слоям (целевой облик)

### `TableClient`

| Группа | Методы |
|--------|--------|
| Sessionless | `retry_bulk_upsert`, `retry_read_rows` / `retry_read_rows_request` |
| DDL | `retry_create_table`, `retry_drop_table`, `retry_alter_table`, `retry_rename_tables` (цель) |
| Copy | `copy_table`, `copy_tables` |
| Describe | `describe_table`, `retry_describe_table_options`, `retry_describe_external_*`, `retry_describe_system_view` (часть — цель) |
| Explain / scheme | `retry_explain_data_query`, `retry_execute_scheme_query` |
| Retry orchestration | `retry_transaction`, autocommit (`create_autocommit_transaction` + one-shot `ExecuteDataQuery`) |

**Не должно быть на клиенте:** `retry_prepare_data_query`, `retry_execute_prepared_query`, `retry_stream_read_table`, `retry_execute_scan_query`.

### `Session` — только

| Метод | RPC |
|-------|-----|
| `prepare_data_query` | PrepareDataQuery |
| `execute_prepared_query` | ExecuteDataQuery (prepared) |
| `execute_data_query` (или аналог) | ExecuteDataQuery |
| `stream_read_table` | StreamReadTable |
| `execute_scan_query` | StreamExecuteScanQuery |

`BeginTransaction` — не отдельный метод; lazy begin внутри retrier при первом `ExecuteDataQuery`.

### `Transaction` (интерактивная, на сессии)

| Метод | RPC |
|-------|-----|
| `query` / `execute` | делегирует в `Session::execute_data_query` |
| `commit` | CommitTransaction |
| `rollback` | RollbackTransaction |

---

## Миграция (выполнено)

### Убрано с `Session` (публичный API)

- `create_table`, `drop_table`, `alter_table`
- `copy_table`, `copy_tables`
- `describe_table`, `describe_table_options`
- `explain_data_query`, `execute_schema_query` — полностью

### На `Session` (публичный API)

- `prepare_data_query`, `execute_prepared_query`
- `execute_data_query` — `pub(crate)` (tx / autocommit)
- `stream_read_table`, `execute_scan_query`

### Убрано с `TableClient`

- `retry_prepare_data_query`, `retry_execute_prepared_query`
- `retry_stream_read_table`, `retry_execute_scan_query`

### Добавлено

- `TableClient::create_session()` — публичный, для session-only RPC
- `Session` — экспортирован из crate root

---

## Согласовано

| Вопрос | Решение |
|--------|---------|
| Prepare / prepared execute | **Только сессия.** На клиенте `retry_prepare_data_query` / `retry_execute_prepared_query` не нужны. |
| StreamReadTable / StreamExecuteScanQuery | **Только сессия.** На клиенте `retry_stream_read_table` / `retry_execute_scan_query` не нужны. |
| Autocommit | **OK:** one-shot `ExecuteDataQuery` через retrier на клиенте, без объекта `Transaction`. |
| Explain / Scheme | **Только клиент.** С сессии убираем полностью (в т.ч. `pub(crate)`). |

---

## Легенда

- **✓** — метод/RPC должен быть на этом слое (публично или как единственное место вызова RPC).
- **✗** — не должно быть на этом слое.
- **(pool)** — внутренний механизм, не пользовательский API.
- **✓ (цель)** — ещё не реализовано, но должно быть только на клиенте.
- **(autocommit)*** — клиент orchestrates one-shot retry, RPC выполняется на сессии; публичного метода на клиенте нет.
