use crate::grpc_wrapper::raw_errors::RawResult;
use strum::IntoEnumIterator;

#[test]
fn test_primitive_types() -> RawResult<()> {
    let primitive_id_iterator = ydb_grpc::ydb_proto::r#type::PrimitiveTypeId::iter();
    for type_id in primitive_id_iterator {
        let proto_type = ydb_grpc::ydb_proto::Type {
            r#type: Some(ydb_grpc::ydb_proto::r#type::Type::TypeId(type_id as i32)),
        };
        let internal_type: crate::grpc_wrapper::raw_table_service::value_type::Type =
            proto_type.clone().try_into()?;
        let reverse_proto: ydb_grpc::ydb_proto::Type = internal_type.into();
        assert_eq!(proto_type, reverse_proto);
    }

    Ok(())
}
