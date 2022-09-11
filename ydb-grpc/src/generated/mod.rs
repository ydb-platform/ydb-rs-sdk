pub mod ydb {
    pub mod table_stats {
        include!("ydb.table_stats.rs");
    }
    pub mod scheme {
        pub mod v1 {
            include!("ydb.scheme.v1.rs");
        }
        include!("ydb.scheme.rs");
    }
    pub mod discovery {
        pub mod v1 {
            include!("ydb.discovery.v1.rs");
        }
        include!("ydb.discovery.rs");
    }
    pub mod formats {
        include!("ydb.formats.rs");
    }
    pub mod operations {
        include!("ydb.operations.rs");
    }
    pub mod issue {
        include!("ydb.issue.rs");
    }
    pub mod table {
        pub mod v1 {
            include!("ydb.table.v1.rs");
        }
        include!("ydb.table.rs");
    }
    include!("ydb.rs");
}
pub mod google {
    pub mod protobuf {
        include!("google.protobuf.rs");
    }
}