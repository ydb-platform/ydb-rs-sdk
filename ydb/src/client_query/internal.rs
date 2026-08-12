use super::exec::ClientExecContext;
use super::transaction::TransactionExecContext;

pub(crate) enum ExecCoreRef<'a> {
    Client(&'a mut ClientExecContext),
    Transaction(&'a mut TransactionExecContext),
}
