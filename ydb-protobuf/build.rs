use std::fs;
use std::io::Write;

const COMPILE_DIR: &str = "../ydb-api-protos";
const INCLUDE_DIRS: &[&str] = &["../ydb-api-protos", "../ydb-api-protos/protos"];
const DEST_DIR: &str = "src";

fn main() -> Result<(), Box<dyn std::error::Error>>{
    let mut compile_files = Vec::default();
    for file in fs::read_dir(COMPILE_DIR).unwrap() {
        let f_name = file.unwrap().file_name().to_str().unwrap().to_owned();
        if f_name.to_lowercase().ends_with(".proto") {
            compile_files.push(f_name);
        }
    }

    println!("Compile files: {:?}", compile_files);

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(DEST_DIR)
        .compile(&["../ydb-api-protos/ydb_table_v1.proto"],
                 &INCLUDE_DIRS)
        .expect("failed to compile protobuf");

    // generate lib.rs
    let mut mod_content = String::default();
    for file in fs::read_dir(DEST_DIR).unwrap() {
        let fname = file.unwrap().file_name().to_str().unwrap().to_owned();
        if fname == "lib.rs" {
            continue
        }

        let mod_name = fname.strip_suffix(".rs").unwrap().replace(".", "_");
        mod_content += format!("#[path = \"{}\"]\npub mod {};\n", fname, mod_name).as_str();
    }

    let mut lib_file = fs::File::create("src/lib.rs").unwrap();
    lib_file.write_all(mod_content.as_bytes()).unwrap();

    Ok(())
}