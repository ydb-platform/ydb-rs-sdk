/// Returns the first YQL statement after skipping leading `DECLARE` blocks.
fn statement_start(text: &str) -> &str {
    let mut rest = text;
    loop {
        rest = rest.trim_start();
        let upper: String = rest.chars().take(8).collect::<String>().to_uppercase();
        if upper.starts_with("DECLARE") {
            let Some(semi) = rest.find(';') else {
                return rest;
            };
            rest = &rest[semi + 1..];
        } else {
            return rest;
        }
    }
}

/// Whether `text` is a scheme (DDL) statement that YDB rejects inside a transaction.
///
/// Matches Go SDK usage of [`query.NoTx()`] for `CREATE TABLE` / `DROP TABLE` / `ALTER TABLE`.
pub(crate) fn is_scheme_query(text: &str) -> bool {
    let stmt = statement_start(text);
    let mut words = stmt.split(|c: char| c.is_whitespace()).filter(|w| !w.is_empty());
    let Some(first) = words.next() else {
        return false;
    };
    let Some(second) = words.next() else {
        return false;
    };
    let first = first.to_ascii_uppercase();
    let second = second.trim_matches('`').to_ascii_uppercase();
    matches!(first.as_str(), "CREATE" | "DROP" | "ALTER")
        && matches!(
            second.as_str(),
            "TABLE" | "INDEX" | "VIEW" | "COLUMN" | "GROUP" | "OBJECT" | "TOPIC" | "ASYNC"
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheme_statements() {
        assert!(is_scheme_query("CREATE TABLE t (id Int64, PRIMARY KEY(id))"));
        assert!(is_scheme_query("  drop table if exists t"));
        assert!(is_scheme_query("ALTER TABLE t ADD COLUMN x Int64"));
        assert!(is_scheme_query(
            "DECLARE $x AS Int64; CREATE TABLE t (id Int64, PRIMARY KEY(id))"
        ));
    }

    #[test]
    fn data_statements() {
        assert!(!is_scheme_query("SELECT 1"));
        assert!(!is_scheme_query("UPSERT INTO t (id) VALUES (1)"));
        assert!(!is_scheme_query(
            "DECLARE $id AS Int64; UPSERT INTO t (id) VALUES ($id)"
        ));
        assert!(!is_scheme_query(
            "REPLACE INTO t SELECT * FROM AS_TABLE($rows)"
        ));
    }
}
