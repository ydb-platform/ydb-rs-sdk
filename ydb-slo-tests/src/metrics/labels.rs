use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};

#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
pub struct ErrorLabel {
    pub error_type: String,
}

#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
pub struct OperationLabel {
    pub operation_type: String,
}

#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
pub struct OperationLatencyLabels {
    pub operation_type: String,
    pub operation_status: OperationStatus,
}

#[derive(Clone, Debug, EncodeLabelValue, Eq, Hash, PartialEq)]
pub enum OperationStatus {
    Success,
    Failure,
}
