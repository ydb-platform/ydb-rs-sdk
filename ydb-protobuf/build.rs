use build_helpers::ProtoModule;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};

const COMPILE_DIRS: &[(&str, &str)] = &[
    // src, dst
    ("../ydb-api-protos", "src/generated"),
];
const INCLUDE_DIRS: &[&str] = &[
    "../ydb-api-protos",
    "../ydb-api-protos/protos",
    "../ydb-api-protos/protos/validation",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../ydb-api-protos");

    for (src, dst) in COMPILE_DIRS {
        clean_dst_dir(dst)?;
        compile_proto_dir(src, INCLUDE_DIRS, dst)?;
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
    println!("compile files: {:?}", files);
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(dst_dir)
        .compile(files, include_dirs)
        .expect("failed to compile protobuf");
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
        if let Some(package_name) = build_helpers::get_proto_package(f_content.as_str()) {
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
        if fname == "mod.rs" {
            continue;
        }
        pm.add_file(fname.as_str());
    }

    let mod_path = format!("{}/mod.rs", dst_dir);
    let mut mod_f = fs::File::create(mod_path)?;
    mod_f.write_all(pm.to_string().as_bytes())?;
    return Ok(());
}
