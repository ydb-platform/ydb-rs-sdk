use super::exec::{ClientExecContext, TransactionExecContext};

pub(crate) enum ExecCoreRef<'a> {
    Client(&'a mut ClientExecContext),
    Transaction(&'a mut TransactionExecContext),
}
