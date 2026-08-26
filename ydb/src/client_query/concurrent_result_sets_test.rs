use std::collections::HashMap;

use crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryRequest;

use super::exec::{CallOptions, build_client_execute_request_for_test};

#[test]
fn new_execute_request_defaults_concurrent_result_sets_to_false() {
    let req = RawExecuteQueryRequest::new("", "SELECT 1", HashMap::new(), None, false)
        .expect("valid request");
    assert!(!req.concurrent_result_sets);
    let proto = ydb_grpc::ydb_proto::query::ExecuteQueryRequest::from(req);
    assert!(!proto.concurrent_result_sets);
}

#[test]
fn build_execute_request_sets_concurrent_result_sets_on_request_and_proto() {
    for want in [true, false] {
        let req = build_client_execute_request_for_test(&CallOptions::default(), want);
        assert_eq!(req.concurrent_result_sets, want);
        let proto = ydb_grpc::ydb_proto::query::ExecuteQueryRequest::from(req);
        assert_eq!(proto.concurrent_result_sets, want);
    }
}
