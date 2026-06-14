use std::collections::HashMap;

use crate::errors::YdbResult;
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::result::ResultSet;
use crate::types::Value;

use super::exec::{
    client_begin_stream, client_run, transaction_begin_stream, transaction_run, CallOptions,
    ClientExecContext, TransactionExecContext,
};

pub(crate) enum ExecCoreRef<'a> {
    Client(&'a mut ClientExecContext),
    Transaction(&'a mut TransactionExecContext),
}

impl ExecCoreRef<'_> {
    pub(crate) async fn run(
        &mut self,
        text: &str,
        params: &HashMap<String, Value>,
        opts: &CallOptions,
    ) -> YdbResult<Vec<ResultSet>> {
        match self {
            ExecCoreRef::Client(ctx) => client_run(ctx, text, params, opts).await,
            ExecCoreRef::Transaction(ctx) => transaction_run(ctx, text, params, opts).await,
        }
    }

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
}

pub(crate) trait HasCore {
    fn core_mut(&mut self) -> ExecCoreRef<'_>;
}
