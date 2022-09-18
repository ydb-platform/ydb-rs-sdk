use std::sync::Arc;
use std::time::Duration;

use crate::errors::{YdbError, YdbIssue, YdbResult};
use crate::grpc_wrapper;
use crate::trait_operation::Operation;
use http::Uri;

use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::trace;
use ydb_grpc::ydb_proto::issue::IssueMessage;
use ydb_grpc::ydb_proto::operations::operation_params::OperationMode;
use ydb_grpc::ydb_proto::operations::OperationParams;

#[tracing::instrument()]
async fn create_grpc_channel(uri: Uri) -> YdbResult<Channel> {
    trace!("start work");
    let tls = if let Some(scheme) = uri.scheme_str() {
        scheme == "https" || scheme == "grpcs"
    } else {
        false
    };

    let mut endpoint = Endpoint::from(uri.clone());
    if tls {
        endpoint = endpoint.tls_config(ClientTlsConfig::new())?
    };
    endpoint = endpoint.tcp_keepalive(Some(Duration::from_secs(15))); // tcp keepalive similar to default in golang lib

    trace!("endpoint: {:?}", endpoint);
    match endpoint.connect().await {
        Ok(channel) => {
            trace!("ok");
            Ok(channel)
        }
        Err(err) => {
            trace!("error: {:?}", err);
            Err(YdbError::TransportDial(Arc::new(err)))
        }
    }
}

pub(crate) fn grpc_read_operation_result<TOp, T>(resp: tonic::Response<TOp>) -> YdbResult<T>
where
    TOp: Operation,
    T: Default + prost::Message,
{
    Ok(grpc_wrapper::grpc::grpc_read_operation_result(resp)?)
}

pub(crate) fn proto_issues_to_ydb_issues(proto_issues: Vec<IssueMessage>) -> Vec<YdbIssue> {
    grpc_wrapper::grpc::proto_issues_to_ydb_issues(proto_issues)
}

pub(crate) fn operation_params(timeout: Duration) -> Option<OperationParams> {
    Some(OperationParams {
        operation_mode: OperationMode::Sync.into(),
        operation_timeout: Some(timeout.into()),
        ..OperationParams::default()
    })
}
