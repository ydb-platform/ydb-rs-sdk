pub mod ydb {
    include!("ydb.rs");

    pub mod import {
        include!("ydb.import.rs");

        pub mod v1 {
            include!("ydb.import.v1.rs");
        }
    }

    pub mod issue {
        include!("ydb.issue.rs");
    }

    pub mod operations {
        include!("ydb.operations.rs");
    }
}
