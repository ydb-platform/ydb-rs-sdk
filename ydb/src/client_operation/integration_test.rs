use std::time::Duration;

use tokio::time::sleep;
use tracing_test::traced_test;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::client_operation::script_test_support::start_execute_script_operation;
use crate::client_operation::{ListOperationsRequest, OperationInfo, OperationKind};
use crate::errors::YdbResult;
use crate::test_integration_helper::create_client;

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn list_operations_execute_query_kind() -> YdbResult<()> {
    let client = create_client().await?;
    let op_client = client.operation_client();

    op_client
        .list_operations(ListOperationsRequest::new(OperationKind::EXECUTE_QUERY))
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn get_operation_unknown_id_returns_non_success() -> YdbResult<()> {
    let client = create_client().await?;
    let op_client = client.operation_client();

    let op = op_client.get_operation("nonexistent-operation-id").await?;
    assert!(op.id.is_empty());
    assert!(!op.is_success());

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn cancel_get_forget_script_operation() -> YdbResult<()> {
    let client = create_client().await?;
    let connection_manager = client.connection_manager_for_test();
    let op_client = client.operation_client();

    let operation_id = start_execute_script_operation(&connection_manager)
        .await
        .map_err(crate::errors::YdbError::from)?;

    let first = op_client.get_operation(&operation_id).await?;
    if !first.ready {
        op_client.cancel_operation(&operation_id).await?;
    }

    let mut last: Option<OperationInfo> = None;
    for _ in 0..60 {
        let op = op_client.get_operation(&operation_id).await?;
        if op.ready {
            last = Some(op);
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }

    let op = last.expect("operation did not become ready within timeout");
    assert!(op.ready);

    op_client.forget_operation(&operation_id).await?;

    let listed = op_client
        .list_operations(ListOperationsRequest::new(OperationKind::EXECUTE_QUERY))
        .await?;
    assert!(!listed.operations.iter().any(|item| item.id == operation_id));

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn cancel_unknown_operation_id_errors() -> YdbResult<()> {
    let client = create_client().await?;
    let op_client = client.operation_client();

    let err = op_client
        .cancel_operation("nonexistent-operation-id")
        .await
        .unwrap_err();

    match &err {
        crate::errors::YdbError::YdbStatusError(status) => {
            assert_ne!(status.operation_status, StatusCode::Success as i32);
        }
        other => panic!("expected YdbStatusError, got {:?}", other),
    }

    Ok(())
}
