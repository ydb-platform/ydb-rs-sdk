pub mod google {
    pub mod protobuf {
        include!("google.protobuf.rs");
    }
}
pub mod ydb {
    pub mod auth {
        pub mod v1 {
            include!("ydb.auth.v1.rs");
        }
        include!("ydb.auth.rs");
    }
    pub mod coordination {
        pub mod v1 {
            include!("ydb.coordination.v1.rs");
        }
        include!("ydb.coordination.rs");
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
    pub mod issue {
        include!("ydb.issue.rs");
    }
    pub mod operations {
        include!("ydb.operations.rs");
    }
    pub mod scheme {
        pub mod v1 {
            include!("ydb.scheme.v1.rs");
        }
        include!("ydb.scheme.rs");
    }
    pub mod table {
        pub mod v1 {
            include!("ydb.table.v1.rs");
        }
        include!("ydb.table.rs");
    }
    pub mod table_stats {
        include!("ydb.table_stats.rs");
    }
    pub mod topic {
        pub mod v1 {
            include!("ydb.topic.v1.rs");
        }
        include!("ydb.topic.rs");
    }
    include!("ydb.rs");
}
