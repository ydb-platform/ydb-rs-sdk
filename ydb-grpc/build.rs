use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::{fs, io};
use std::path::PathBuf;
use walkdir::WalkDir;

const DST_FOLDER: &str="src/generated";

const COMPILE_FILES: &[&str] = &[
    "ydb_scheme_v1.proto",
    "ydb_discovery_v1.proto",
    "ydb_table_v1.proto",
];

const INCLUDE_DIRS: &[&str] = &[
    "ydb-api-protos",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("CARGO_FEATURE_REGENERATE_SOURCES").unwrap_or_else(|_| "0".into()) != "1" {
        println!("skip regenerate sources");
        return Ok(());
    };
    println!("cargo:rerun-if-changed=ydb-api-protos");

    clean_dst_dir(DST_FOLDER)?;

    let descriptor_file = PathBuf::from(DST_FOLDER).join("descriptors.bin");

    let mut cfg = prost_build::Config::default();
    cfg.compile_well_known_types()
        .type_attribute(".Ydb", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(".google.protobuf", "::pbjson_types")
        .file_descriptor_set_path(&descriptor_file)
    ;

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(DST_FOLDER)
        .include_file("mod.rs")
        .compile_with_config(
            cfg,
            COMPILE_FILES,
            INCLUDE_DIRS,
        )?;

    let descriptor_bytes = std::fs::read(descriptor_file).unwrap();
    pbjson_build::Builder::new()
        .out_dir(DST_FOLDER)
        .register_descriptors(&descriptor_bytes)
        .unwrap()
        .build(&[".google", ".ydb"])
        .unwrap();

    fix_generated_files("src/generated")?;
    Ok(())
}

fn clean_dst_dir(dst: &str) -> Result<(), Box<dyn std::error::Error>> {
    for file in fs::read_dir(dst)? {
        let fname = file?.file_name().to_str().unwrap().to_owned();
        let fpath = format!("{}/{}", dst, fname);
        if fname == "lib.rs" || fname == "mod.rs" {
            println!("truncate file: {}", &fpath);
            fs::File::create(&fpath)?;
            continue;
        }
        println!("remove file: {}", &fpath);
        fs::remove_file(fpath)?;
    }

    Ok(())
}

fn fix_generated_files(dir: &str) -> io::Result<()> {
    for item in WalkDir::new(dir) {
        let item = item?;
        let item_path = item.path().to_str().unwrap_or("<empty>");
        if !item.metadata()?.is_file() {
            println!("skip not file: '{}'", item_path);
            continue;
        }
        fix_generated_file(item.path())?;
    }

    Ok(())
}

fn fix_generated_file(fpath: &std::path::Path) -> io::Result<()> {
    if fpath.as_os_str().to_str().unwrap_or("").ends_with(".rs") {
        println!("rewrite file: '{}'", fpath.to_str().unwrap_or("<empty>"));
    } else {
        println!(
            "skip rewrite file: '{}'",
            fpath.to_str().unwrap_or("<empty>"),
        );
        return Ok(());
    }
    let mut f = OpenOptions::new().read(true).write(true).open(fpath)?;
    let mut contents = String::new();
    let _ = f.read_to_string(&mut contents).unwrap();

    let lines: Vec<&str> = contents
        .split_terminator('\n')
        .filter(|line| line.trim() != "///")
        .collect();

    let contents = lines.join("\n");
    f.seek(SeekFrom::Start(0))?;
    f.set_len(0)?;
    f.write_all(contents.as_bytes())?;
    Ok(())
}
