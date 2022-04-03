use std::sync::Arc;
use std::time::Duration;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::client_common::DBCredentials;
use crate::errors;
use crate::errors::{YdbError, YdbIssue, YdbResult};
use crate::middlewares::AuthService;
use crate::trait_operation::Operation;
use http::Uri;
use tokio::sync::mpsc;

use crate::channel_pool::{ChannelErrorInfo, ChannelProxy, ChannelProxyErrorSender};
use tonic::transport::{ClientTlsConfig, Endpoint};
use tower::ServiceBuilder;
use tracing::trace;
use ydb_grpc::ydb_proto::issue::IssueMessage;
use ydb_grpc::ydb_proto::operations::operation_params::OperationMode;
use ydb_grpc::ydb_proto::operations::OperationParams;

#[tracing::instrument(skip(new_func, cred))]
pub(crate) async fn create_grpc_client<T, CB>(
    uri: Uri,
    cred: DBCredentials,
    new_func: CB,
) -> YdbResult<T>
where
    CB: FnOnce(AuthService) -> T,
{
    return create_grpc_client_with_error_sender(uri, cred, None, new_func).await;
}

pub(crate) async fn create_grpc_client_with_error_sender<T, CB>(
    uri: Uri,
    cred: DBCredentials,
    error_sender: ChannelProxyErrorSender,
    new_func: CB,
) -> YdbResult<T>
where
    CB: FnOnce(AuthService) -> T,
{
    let channel = create_grpc_channel(uri, error_sender).await?;
    return create_client_on_channel(channel, cred, new_func);
}

fn create_client_on_channel<NewFuncT, ClientT>(
    channel: ChannelProxy,
    cred: DBCredentials,
    new_func: NewFuncT,
) -> YdbResult<ClientT>
where
    NewFuncT: FnOnce(AuthService) -> ClientT,
{
    let auth_service_create = |ch| {
        return AuthService::new(ch, cred.clone());
    };
    let auth_ch = ServiceBuilder::new()
        .layer_fn(auth_service_create)
        .service(channel);
    return Ok(new_func(auth_ch));
}

#[tracing::instrument(skip(error_sender))]
async fn create_grpc_channel(
    uri: Uri,
    error_sender: Option<mpsc::Sender<ChannelErrorInfo>>,
) -> YdbResult<ChannelProxy> {
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
    return match endpoint.connect().await {
        Ok(channel) => {
            trace!("ok");
            Ok(ChannelProxy::new(uri, channel, error_sender))
        }
        Err(err) => {
            trace!("error: {:?}", err);
            if let Some(sender) = error_sender {
                // ignore notify error
                let _ = sender.send(ChannelErrorInfo { endpoint: uri }).await;
            };
            Err(YdbError::TransportDial(Arc::new(err)))
        }
    };
}

#[tracing::instrument]
pub(crate) fn grpc_read_operation_result<TOp, T>(resp: tonic::Response<TOp>) -> errors::YdbResult<T>
where
    TOp: Operation,
    T: Default + prost::Message,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner
        .operation()
        .ok_or(YdbError::Custom("no operation object in result".into()))?;
    if op.status() != StatusCode::Success {
        return Err(create_operation_error(op));
    }
    let opres = op
        .result
        .ok_or(YdbError::Custom("no result data in operation".into()))?;
    let res: T = T::decode(opres.value)?;
    return Ok(res);
}

pub(crate) fn grpc_read_void_operation_result<TOp>(
    resp: tonic::Response<TOp>,
) -> errors::YdbResult<()>
where
    TOp: Operation,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner
        .operation()
        .ok_or(YdbError::Custom("no operation object in result".into()))?;
    if op.status() != StatusCode::Success {
        return Err(create_operation_error(op));
    }
    return Ok(());
}

pub(crate) fn proto_issues_to_ydb_issues(proto_issues: Vec<IssueMessage>) -> Vec<YdbIssue> {
    proto_issues
        .into_iter()
        .map(|proto_issue| YdbIssue {
            issue_code: proto_issue.issue_code,
            message: proto_issue.message,
            issues: proto_issues_to_ydb_issues(proto_issue.issues),
            severity: proto_issue.severity,
        })
        .collect()
}

pub(crate) fn create_operation_error(op: ydb_grpc::ydb_proto::operations::Operation) -> YdbError {
    return YdbError::YdbStatusError(crate::errors::YdbStatusError {
        message: format!("{:?}", &op),
        operation_status: op.status,
        issues: proto_issues_to_ydb_issues(op.issues),
    });
}

pub(crate) fn operation_params(timeout: Duration) -> Option<OperationParams> {
    return Some(OperationParams {
        operation_mode: OperationMode::Sync.into(),
        operation_timeout: Some(timeout.into()),
        ..OperationParams::default()
    });
}
