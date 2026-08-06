use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_query_service::status::check_status;
use ydb_grpc::ydb_proto::query::SessionState;
use ydb_grpc::ydb_proto::query::session_state::SessionHint;

/// Meaning of one successful `AttachSession` stream message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RawAttachSessionEvent {
    Active,
    SessionShutdown,
    NodeShutdown,
}

impl TryFrom<SessionState> for RawAttachSessionEvent {
    type Error = RawError;

    fn try_from(message: SessionState) -> RawResult<Self> {
        check_status(message.status, &message.issues)?;

        match message.session_hint {
            None => Ok(Self::Active),
            Some(SessionHint::SessionShutdown(_)) => Ok(Self::SessionShutdown),
            Some(SessionHint::NodeShutdown(_)) => Ok(Self::NodeShutdown),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ydb_grpc::ydb_proto::query::{NodeShutdownHint, SessionShutdownHint};
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    fn message(status: StatusCode, session_hint: Option<SessionHint>) -> SessionState {
        SessionState {
            status: status as i32,
            issues: Vec::new(),
            session_hint,
        }
    }

    #[test]
    fn decodes_successful_attach_events() {
        assert_eq!(
            RawAttachSessionEvent::try_from(message(StatusCode::Success, None))
                .expect("successful state without a hint must be active"),
            RawAttachSessionEvent::Active
        );
        assert_eq!(
            RawAttachSessionEvent::try_from(message(
                StatusCode::Success,
                Some(SessionHint::SessionShutdown(SessionShutdownHint {})),
            ))
            .expect("successful session shutdown hint must be decoded"),
            RawAttachSessionEvent::SessionShutdown
        );
        assert_eq!(
            RawAttachSessionEvent::try_from(message(
                StatusCode::Success,
                Some(SessionHint::NodeShutdown(NodeShutdownHint {})),
            ))
            .expect("successful node shutdown hint must be decoded"),
            RawAttachSessionEvent::NodeShutdown
        );
    }

    #[test]
    fn failed_status_takes_precedence_over_shutdown_hint() {
        let err = RawAttachSessionEvent::try_from(message(
            StatusCode::BadSession,
            Some(SessionHint::NodeShutdown(NodeShutdownHint {})),
        ))
        .expect_err("failed status must not be decoded as a successful attach event");

        assert!(matches!(
            err,
            RawError::YdbStatus(status)
                if status.operation_status == StatusCode::BadSession as i32
        ));
    }
}
