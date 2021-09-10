fn main() -> Result<(), Box<dyn std::error::Error>>{
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src/")
        .compile(&["../ydb-api-protos/ydb_table_v1.proto"], &["../ydb-api-protos"])
        .expect("failed to compile protobuf");

    Ok(())
}