use std::collections::HashMap;

use crate::errors::YdbResult;
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::types::Value;

use super::exec::{
    client_begin_materialized_stream, client_begin_stream, transaction_begin_materialized_stream,
    transaction_begin_stream, CallOptions, ClientExecContext, TransactionExecContext,
};

pub(crate) enum ExecCoreRef<'a> {
    Client(&'a mut ClientExecContext),
    Transaction(&'a mut TransactionExecContext),
}

impl ExecCoreRef<'_> {
    pub(crate) async fn begin_stream(
        &mut self,
        text: String,
        params: HashMap<String, Value>,
        opts: CallOptions,
    ) -> YdbResult<ExecuteQueryStream> {
        match self {
            ExecCoreRef::Client(ctx) => client_begin_stream(ctx, text, params, opts).await,
            ExecCoreRef::Transaction(ctx) => {
                transaction_begin_stream(ctx, text, params, opts).await
            }
        }
    }

    pub(crate) async fn begin_materialized_stream(
        &mut self,
        text: String,
        params: HashMap<String, Value>,
        opts: CallOptions,
    ) -> YdbResult<ExecuteQueryStream> {
        match self {
            ExecCoreRef::Client(ctx) => {
                client_begin_materialized_stream(ctx, text, params, opts).await
            }
            ExecCoreRef::Transaction(ctx) => {
                transaction_begin_materialized_stream(ctx, text, params, opts).await
            }
        }
    }
}

pub(crate) trait HasCore {
    fn core_mut(&mut self) -> ExecCoreRef<'_>;
}
