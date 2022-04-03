use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::{fs, io};
use walkdir::WalkDir;
use ydb_grpc_helpers::ProtoModule;

const COMPILE_DIRS: &[(&str, &str)] = &[
    // src, dst
    ("ydb-api-protos", "src/generated"),
];

const INCLUDE_DIRS: &[&str] = &[
    "ydb-api-protos",
    "ydb-api-protos/protos",
    "ydb-api-protos/protos/validation",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=ydb-api-protos");

    if std::env::var("CARGO_FEATURE_REGENERATE_SOURCES").unwrap_or("0".into()) != "1" {
        println!("skip regenerate sources");
        return Ok(());
    }

    for (src, dst) in COMPILE_DIRS {
        clean_dst_dir(dst)?;
        compile_proto_dir(src, INCLUDE_DIRS, dst)?;
        rewrite_generated_files(dst)?;
        generate_mod_file(dst)?;
    }

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

    return Ok(());
}

fn compile_files(files: &[&str], include_dirs: &[&str], dst_dir: &str) {
    if files.is_empty() {
        return;
    }

    println!("compile files: {:?}", files);

    // let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    // let descriptor_file = out.join("descriptors.bin");
    let out = PathBuf::from(dst_dir);
    let descriptor_file = out.join("descriptors.bin");
    println!(
        "descriptor: '{}'",
        descriptor_file.as_os_str().to_str().unwrap()
    );

    let mut cfg = prost_build::Config::default();
    cfg.compile_well_known_types()
        .type_attribute(".Ydb", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(".google.protobuf", "::pbjson_types")
        .file_descriptor_set_path(&descriptor_file);

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(dst_dir)
        .compile_with_config(cfg, files, include_dirs)
        .expect("failed to compile protobuf");

    let descriptor_bytes = std::fs::read(descriptor_file).unwrap();
    pbjson_build::Builder::new()
        .out_dir(dst_dir)
        .register_descriptors(&descriptor_bytes)
        .unwrap()
        .build(&[".mypackage"])
        .unwrap();
}

fn rewrite_generated_file(fpath: &std::path::Path) -> io::Result<()> {
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
        .split_terminator("\n")
        .filter(|line| line.trim() != "///")
        .collect();

    let contents = lines.join("\n");
    f.seek(SeekFrom::Start(0))?;
    f.set_len(0)?;
    f.write_all(contents.as_bytes())?;
    return Ok(());
}

fn rewrite_generated_files(dir: &str) -> io::Result<()> {
    for item in WalkDir::new(dir) {
        let item = item?;
        let item_path = item.path().to_str().unwrap_or("<empty>");
        if !item.metadata()?.is_file() {
            println!("skip not file: '{}'", item_path);
            continue;
        }
        rewrite_generated_file(item.path())?;
    }

    return Ok(());
}

fn compile_proto_dir(
    src_dir: &str,
    include_dirs: &[&str],
    dst_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // read src files
    let mut files_for_compile: HashMap<String, Vec<String>> = HashMap::default();

    for item in walkdir::WalkDir::new(src_dir) {
        let item = item?;
        let f_path = item.path().to_str().unwrap();
        println!("read fpath for compile: {}", f_path);

        if !item.metadata()?.is_file() {
            println!("skip dir");
            continue;
        }

        if !f_path.to_lowercase().ends_with(".proto") {
            println!("skip non .proto");
            continue;
        }

        let mut f = fs::File::open(item.path())?;
        let mut f_content = String::new();
        f.read_to_string(&mut f_content)?;
        if let Some(package_name) = ydb_grpc_helpers::get_proto_package(f_content.as_str()) {
            if let Some(vec) = files_for_compile.get_mut(package_name) {
                vec.push(f_path.to_string());
            } else {
                files_for_compile.insert(package_name.to_string(), vec![f_path.to_string()]);
            }
        } else {
            println!("Unknown package name for: {}", f_path);
        }
    }

    let mut packages: Vec<_> = files_for_compile.keys().collect();

    // hack for full compile Ydb package
    // and overwrite parially compiled.
    //
    // Source of problem: few files have package Ydb, but not always
    // all included by import and during compile ydb package overwrited few times
    // for different dependencies.
    // We need for last time ydb package will compile with full list of files.
    packages.sort_by(|a, b| {
        let (a, b) = (a.as_str(), b.as_str());

        if a == b {
            return Ordering::Equal;
        }
        if a == "Ydb" {
            return Ordering::Greater;
        }
        if b == "Ydb" {
            return Ordering::Less;
        }

        a.cmp(b)
    });

    for package in packages {
        let files = files_for_compile.get(package).unwrap();
        println!("Compile proto package \"{}\": {:?}", package, files);
        let files: Vec<_> = files.iter().map(|s| s.as_str()).collect();
        compile_files(files.as_slice(), include_dirs, dst_dir);
    }

    return Ok(());
}

fn generate_mod_file(dst_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut pm = ProtoModule::default();
    for file in fs::read_dir(dst_dir)? {
        let file = file?;
        let fname = file.file_name().to_str().unwrap().to_owned();
        let fpath = format!("{}/{}", dst_dir, fname);
        if !fs::metadata(&fpath)?.is_file() {
            continue;
        }
        if fname == "mod.rs" || !fname.ends_with(".rs") {
            continue;
        }
        pm.add_file(fname.as_str());
    }

    let mod_path = format!("{}/mod.rs", dst_dir);
    let mut mod_f = fs::File::create(mod_path)?;
    mod_f.write_all(pm.to_string().as_bytes())?;
    return Ok(());
}
