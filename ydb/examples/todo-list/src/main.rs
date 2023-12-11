use std::env;
use std::fmt::{self, Debug};
use std::num::ParseIntError;
use std::str::FromStr;
use ydb::{
    ydb_params, ClientBuilder, Query, StaticToken, TableClient, YdbError, YdbOrCustomerError,
};

const USAGE: &str = "
Usage: <command> <cmd_args>

Available commands:

initdb
    Initialises todo list database
add <id> my todo
    Adds a todo item with the provided id
list
    Prints upto 200 todo items
cleardb
    Deinitializes todo list database
";

enum TodoListError {
    Usage,
    Ydb(YdbError),
    YdbOrCustomer(YdbOrCustomerError),
    IdRequired,
    ParseInt(ParseIntError),
    TextRequired,
}

impl From<YdbError> for TodoListError {
    fn from(other: YdbError) -> Self {
        Self::Ydb(other)
    }
}

impl From<YdbOrCustomerError> for TodoListError {
    fn from(other: YdbOrCustomerError) -> Self {
        Self::YdbOrCustomer(other)
    }
}

impl From<ParseIntError> for TodoListError {
    fn from(other: ParseIntError) -> Self {
        Self::ParseInt(other)
    }
}

impl Debug for TodoListError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(f, "{}", USAGE),
            Self::Ydb(ydb_err) => write!(f, "{}", ydb_err),
            Self::YdbOrCustomer(ydb_err) => write!(f, "{}", ydb_err),
            Self::IdRequired => write!(f, "argument id is required"),
            Self::ParseInt(err) => write!(f, "id is a nonnegative integer: {}", err),
            Self::TextRequired => write!(f, "argument text is required"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), TodoListError> {
    // create the driver
    let conn_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136?database=/local".to_string());
    let client = ClientBuilder::from_str(&conn_string)?
        .with_credentials(StaticToken::from("asd"))
        .client()?;

    // wait until the background initialization of the driver finishes
    client.wait().await?;

    let mut args = env::args().skip(1);
    let cmd = args.next().ok_or_else(|| TodoListError::Usage)?;

    match cmd.as_str() {
        "add" => {
            let todo = parse_add(args)?;
            let mut table_client = client.table_client(); // create table client
            add_todo(&mut table_client, todo).await?;
        }
        "list" => {
            let mut table_client = client.table_client(); // create table client
            list(&mut table_client).await?
        }
        "initdb" => {
            let mut table_client = client.table_client(); // create table client
            init_db(&mut table_client).await?
        }
        "cleardb" => {
            let mut table_client = client.table_client(); // create table client
            clear_db(&mut table_client).await?
        }
        // "done" => println!("done"),
        // "delete" => println!("delete"),
        _ => {
            eprintln!("{}", USAGE);
            std::process::exit(1);
        }
    }

    return Ok(());
}

struct TodoItem {
    id: u64,
    text: String,
    _done: bool,
}

fn parse_add<I>(mut args: I) -> Result<TodoItem, TodoListError>
where
    I: Iterator<Item = String> + ExactSizeIterator,
{
    let id_raw = args.next().ok_or_else(|| TodoListError::IdRequired)?;
    let id = id_raw.parse::<u64>()?;
    if args.len() == 0 {
        return Err(TodoListError::TextRequired);
    }
    let arguments = args.collect::<Vec<String>>();
    Ok(TodoItem {
        id,
        text: arguments.join(" "),
        _done: false,
    })
}

async fn add_todo(table_client: &mut TableClient, todo: TodoItem) -> Result<(), TodoListError> {
    let text = todo.text.as_str();
    table_client
        .retry_transaction(|mut tx| async move {
            // the code in transaction can retry a few times if there was a retriable error
            tx.query(
                ydb::Query::from(
                    "
                    DECLARE $id as UInt64;
                    DECLARE $text as Utf8;

                    INSERT INTO todo_items
                        (id, item, done)
                    VALUES
                        ($id, $text, false);
                ",
                )
                .with_params(ydb_params!("$id" => todo.id, "$text" => text)),
            )
            .await?;
            tx.commit().await?;
            Ok(())
        })
        .await?;
    Ok(())
}

async fn list(table_client: &mut TableClient) -> Result<(), TodoListError> {
    let table_client = table_client.clone_with_transaction_options(
        ydb::TransactionOptions::new()
            .with_autocommit(true)
            .with_mode(ydb::Mode::OnlineReadonly),
    );
    table_client
        .retry_transaction(|tx| async {
            let mut tx = tx; // move tx lifetime into code block
            let rows = tx
                .query(Query::from(
                    "
                    SELECT
                        id, item, done
                    FROM
                        todo_items
                    LIMIT 200;
                ",
                ))
                .await?
                .into_only_result()?
                .rows();
            println!("id\ttext\tdone");
            for mut row in rows {
                let id: Option<u64> = row.remove_field_by_name("id")?.try_into()?;
                let text: Option<String> = row.remove_field_by_name("item")?.try_into()?;
                let done: Option<bool> = row.remove_field_by_name("done")?.try_into()?;

                println!(
                    "{}\t{}\t{}",
                    id.expect("not null"),
                    text.expect("not null"),
                    done.expect("not null")
                );
            }
            Ok(())
        })
        .await?;
    Ok(())
}

async fn init_db(table_client: &mut TableClient) -> Result<(), TodoListError> {
    table_client
        .retry_execute_scheme_query(
            "CREATE TABLE todo_items (
                id UInt64,
                item Utf8,
                done Bool,
                PRIMARY KEY (id)
            );
",
        )
        .await?;

    Ok(())
}

async fn clear_db(table_client: &mut TableClient) -> Result<(), TodoListError> {
    table_client
        .retry_execute_scheme_query("DROP TABLE todo_items;")
        .await?;
    Ok(())
}
