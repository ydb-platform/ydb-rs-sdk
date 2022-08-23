use std::sync::Arc;
use std::time::Duration;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::client_common::DBCredentials;
use crate::errors::{YdbError, YdbIssue, YdbResult};
use crate::trait_operation::Operation;
use crate::{errors, grpc_wrapper};
use http::Uri;
use tokio::sync::mpsc;

use crate::channel_pool::{ChannelErrorInfo, ChannelErrorSender};
use crate::dicovery_pessimization_interceptor::DiscoveryPessimizationInterceptor;
use crate::grpc_wrapper::auth::AuthGrpcInterceptor;
use crate::grpc_wrapper::runtime_interceptors::{InterceptedChannel, MultiInterceptor};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
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
    CB: FnOnce(InterceptedChannel) -> T,
{
    create_grpc_client_with_error_sender(uri, cred, None, new_func).await
}

pub(crate) async fn create_grpc_client_with_error_sender<T, CB>(
    uri: Uri,
    cred: DBCredentials,
    error_sender: ChannelErrorSender,
    new_func: CB,
) -> YdbResult<T>
where
    CB: FnOnce(InterceptedChannel) -> T,
{
    match create_grpc_channel(uri.clone()).await {
        Ok(channel) => create_client_on_channel(channel, cred, error_sender, new_func),
        Err(err) => {
            if let Some(sender) = error_sender {
                let _ = sender.send(ChannelErrorInfo {
                    endpoint: uri.clone(),
                });
            };
            Err(err)
        }
    }
}

fn create_client_on_channel<NewFuncT, ClientT>(
    channel: Channel,
    cred: DBCredentials,
    error_sender: ChannelErrorSender,
    new_func: NewFuncT,
) -> YdbResult<ClientT>
where
    NewFuncT: FnOnce(InterceptedChannel) -> ClientT,
{
    let mut interceptor = MultiInterceptor::new().with_interceptor(AuthGrpcInterceptor::new(cred)?);

    if let Some(sender) = error_sender {
        interceptor =
            interceptor.with_interceptor(DiscoveryPessimizationInterceptor::new_with_sender(sender))
    };

    let auth_ch = InterceptedChannel::new(channel, interceptor);
    Ok(new_func(auth_ch))
}

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

pub(crate) fn grpc_read_void_operation_result<TOp>(
    resp: tonic::Response<TOp>,
) -> errors::YdbResult<()>
where
    TOp: Operation,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner
        .operation()
        .ok_or_else(|| YdbError::Custom("no operation object in result".into()))?;
    if op.status() != StatusCode::Success {
        return Err(create_operation_error(op));
    }
    Ok(())
}

pub(crate) fn proto_issues_to_ydb_issues(proto_issues: Vec<IssueMessage>) -> Vec<YdbIssue> {
    grpc_wrapper::grpc::proto_issues_to_ydb_issues(proto_issues)
}

pub(crate) fn create_operation_error(op: ydb_grpc::ydb_proto::operations::Operation) -> YdbError {
    grpc_wrapper::grpc::create_operation_error(op).into()
}

pub(crate) fn operation_params(timeout: Duration) -> Option<OperationParams> {
    Some(OperationParams {
        operation_mode: OperationMode::Sync.into(),
        operation_timeout: Some(timeout.into()),
        ..OperationParams::default()
    })
}
