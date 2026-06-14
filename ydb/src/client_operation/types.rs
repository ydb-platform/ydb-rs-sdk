use crate::YdbIssue;

/// Long-running operation snapshot returned by Operation Service.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct OperationInfo {
    pub id: String,
    pub ready: bool,
    /// YDB status code (`Ydb.StatusIds.StatusCode` protobuf value).
    pub status: i32,
    pub issues: Vec<YdbIssue>,
    /// Consumed units from `cost_info`, when reported by the server.
    pub consumed_units: Option<f64>,
}

/// Filter and pagination parameters for [`OperationClient::list_operations`].
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct ListOperationsRequest {
    pub kind: String,
    pub page_size: u64,
    pub page_token: String,
}

impl ListOperationsRequest {
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            page_size: 0,
            page_token: String::new(),
        }
    }

    pub fn with_page_size(mut self, page_size: u64) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn with_page_token(mut self, page_token: impl Into<String>) -> Self {
        self.page_token = page_token.into();
        self
    }
}

/// Page of operations from [`OperationClient::list_operations`].
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct ListOperationsResult {
    pub operations: Vec<OperationInfo>,
    pub next_page_token: String,
}

/// Well-known `kind` values for [`ListOperationsRequest`].
pub struct OperationKind;

impl OperationKind {
    pub const EXECUTE_QUERY: &'static str = "scriptexec";
    pub const BUILD_INDEX: &'static str = "buildindex";
    pub const IMPORT_FROM_S3: &'static str = "import/s3";
    pub const EXPORT_TO_S3: &'static str = "export/s3";
    pub const EXPORT_TO_YT: &'static str = "export/yt";
}
