use std::collections::HashMap;

use futures_util::future::BoxFuture;

use crate::errors::YdbResult;
use crate::types::Value;

use super::exec::{
    CallOptions, ClientExecContext, OpenedQueryStream, TransactionExecContext, client_begin_stream,
    transaction_begin_stream,
};

pub(crate) enum ExecCoreRef<'a> {
    Client(&'a mut ClientExecContext),
    Transaction(&'a mut TransactionExecContext),
}

impl ExecCoreRef<'_> {
    pub(crate) fn begin_stream(
        &mut self,
        text: String,
        params: HashMap<String, Value>,
        opts: CallOptions,
        concurrent_result_sets: bool,
    ) -> BoxFuture<'_, YdbResult<OpenedQueryStream>> {
        Box::pin(async move {
            match self {
                ExecCoreRef::Client(ctx) => {
                    client_begin_stream(ctx, text, params, opts, concurrent_result_sets).await
                }
                ExecCoreRef::Transaction(ctx) => {
                    transaction_begin_stream(ctx, text, params, opts, concurrent_result_sets).await
                }
            }
        })
    }
}
