mod google {
  mod protobuf {
    include!("google.protobuf.rs");
  }
}
mod ydb {
  include!("ydb.rs");
  mod coordination {
    include!("ydb.coordination.rs");
    mod v1 {
      include!("ydb.coordination.v1.rs");
    }
  }
  mod discovery {
    include!("ydb.discovery.rs");
    mod v1 {
      include!("ydb.discovery.v1.rs");
    }
  }
  mod experimental {
    include!("ydb.experimental.rs");
    mod v1 {
      include!("ydb.experimental.v1.rs");
    }
  }
  mod export {
    include!("ydb.export.rs");
    mod v1 {
      include!("ydb.export.v1.rs");
    }
  }
  mod import {
    include!("ydb.import.rs");
    mod v1 {
      include!("ydb.import.v1.rs");
    }
  }
  mod issue {
    include!("ydb.issue.rs");
  }
  mod monitoring {
    include!("ydb.monitoring.rs");
    mod v1 {
      include!("ydb.monitoring.v1.rs");
    }
  }
  mod operation {
    mod v1 {
      include!("ydb.operation.v1.rs");
    }
  }
  mod operations {
    include!("ydb.operations.rs");
  }
  mod pers_queue {
    mod cluster_discovery {
      include!("ydb.pers_queue.cluster_discovery.rs");
    }
    mod error_code {
      include!("ydb.pers_queue.error_code.rs");
    }
    mod v1 {
      include!("ydb.pers_queue.v1.rs");
    }
  }
  mod rate_limiter {
    include!("ydb.rate_limiter.rs");
    mod v1 {
      include!("ydb.rate_limiter.v1.rs");
    }
  }
  mod scheme {
    include!("ydb.scheme.rs");
    mod v1 {
      include!("ydb.scheme.v1.rs");
    }
  }
  mod scripting {
    include!("ydb.scripting.rs");
    mod v1 {
      include!("ydb.scripting.v1.rs");
    }
  }
  mod table {
    include!("ydb.table.rs");
    mod v1 {
      include!("ydb.table.v1.rs");
    }
  }
  mod table_stats {
    include!("ydb.table_stats.rs");
  }
}
