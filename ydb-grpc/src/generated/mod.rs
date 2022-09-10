pub mod google {
  pub mod protobuf {
    include!("google.protobuf.rs");
  }
}
pub mod ydb {
  include!("ydb.rs");
  pub mod cms {
    include!("ydb.cms.rs");
    pub mod v1 {
      include!("ydb.cms.v1.rs");
    }
  }
  pub mod coordination {
    include!("ydb.coordination.rs");
    pub mod v1 {
      include!("ydb.coordination.v1.rs");
    }
  }
  pub mod discovery {
    include!("ydb.discovery.rs");
    pub mod v1 {
      include!("ydb.discovery.v1.rs");
    }
  }
  pub mod export {
    include!("ydb.export.rs");
    pub mod v1 {
      include!("ydb.export.v1.rs");
    }
  }
  pub mod formats {
    include!("ydb.formats.rs");
  }
  pub mod import {
    include!("ydb.import.rs");
    pub mod v1 {
      include!("ydb.import.v1.rs");
    }
  }
  pub mod issue {
    include!("ydb.issue.rs");
  }
  pub mod monitoring {
    include!("ydb.monitoring.rs");
    pub mod v1 {
      include!("ydb.monitoring.v1.rs");
    }
  }
  pub mod operation {
    pub mod v1 {
      include!("ydb.operation.v1.rs");
    }
  }
  pub mod operations {
    include!("ydb.operations.rs");
  }
  pub mod pers_queue {
    pub mod cluster_discovery {
      include!("ydb.pers_queue.cluster_discovery.rs");
    }
    pub mod v1 {
      include!("ydb.pers_queue.v1.rs");
    }
  }
  pub mod rate_limiter {
    include!("ydb.rate_limiter.rs");
    pub mod v1 {
      include!("ydb.rate_limiter.v1.rs");
    }
  }
  pub mod scheme {
    include!("ydb.scheme.rs");
    pub mod v1 {
      include!("ydb.scheme.v1.rs");
    }
  }
  pub mod scripting {
    include!("ydb.scripting.rs");
    pub mod v1 {
      include!("ydb.scripting.v1.rs");
    }
  }
  pub mod table {
    include!("ydb.table.rs");
    pub mod v1 {
      include!("ydb.table.v1.rs");
    }
  }
  pub mod table_stats {
    include!("ydb.table_stats.rs");
  }
}
