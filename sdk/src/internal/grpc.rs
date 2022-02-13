use std::sync::Arc;
use std::time::Duration;
use ydb_protobuf::ydb_proto::status_ids::StatusCode;

use crate::errors;
use crate::errors::{YdbError, YdbIssue, YdbResult};
use crate::internal::client_common::DBCredentials;
use crate::internal::middlewares::AuthService;
use crate::internal::trait_operation::Operation;
use http::Uri;
use tokio::sync::mpsc;

use crate::internal::channel_pool::{ChannelErrorInfo, ChannelProxy, ChannelProxyErrorSender};
use tonic::transport::{ClientTlsConfig, Endpoint};
use tower::ServiceBuilder;
use ydb_protobuf::ydb_proto::issue::IssueMessage;

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

async fn create_grpc_channel(
    uri: Uri,
    error_sender: Option<mpsc::Sender<ChannelErrorInfo>>,
) -> YdbResult<ChannelProxy> {
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

    return match endpoint.connect().await {
        Ok(channel) => Ok(ChannelProxy::new(uri, channel, error_sender)),
        Err(err) => {
            if let Some(sender) = error_sender {
                // ignore notify error
                let _ = sender.send(ChannelErrorInfo { endpoint: uri }).await;
            };
            Err(YdbError::TransportDial(Arc::new(err)))
        }
    };
}

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
    let res: T = T::decode(opres.value.as_slice())?;
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
            code: proto_issue.issue_code,
            message: proto_issue.message,
            issues: proto_issues_to_ydb_issues(proto_issue.issues),
        })
        .collect()
}

pub(crate) fn create_operation_error(
    op: ydb_protobuf::ydb_proto::operations::Operation,
) -> YdbError {
    return YdbError::YdbStatusError(crate::errors::YdbStatusError {
        message: format!("{:?}", &op),
        operation_status: op.status,
        issues: proto_issues_to_ydb_issues(op.issues),
    });
}
