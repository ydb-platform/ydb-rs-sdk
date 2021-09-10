use std::fs;
use std::io::Write;
use std::collections::HashSet;
use std::cmp::{min, Ordering};

const COMPILE_DIRS: &[&str] = &["../ydb-api-protos", "../ydb-api-protos/protos"];
const INCLUDE_DIRS: &[&str] = &["../ydb-api-protos", "../ydb-api-protos/protos", "../ydb-api-protos/protos/validation"];
const DEST_DIR: &str = "src";


fn main() -> Result<(), Box<dyn std::error::Error>>{

    for file in fs::read_dir(DEST_DIR).unwrap() {
        let fname = file.unwrap().file_name().to_str().unwrap().to_owned();
        if fname == "lib.rs" {
            continue
        }
        fs::remove_file(DEST_DIR.to_string() + "/" + fname.as_str());
    }

        let mut compile_files = Vec::default();
    for dir in COMPILE_DIRS {
        for file in fs::read_dir(dir).unwrap() {
            let f_name = file.unwrap().file_name().to_str().unwrap().to_owned();
            if f_name.to_lowercase().ends_with(".proto") {
                compile_files.push(dir.to_string() + "/" + f_name.as_str());
            }
        }
    }

    println!("Compile files: {:?}", compile_files);

    let mut include_dirs = Vec::with_capacity(INCLUDE_DIRS.len());
    for d in INCLUDE_DIRS {
        include_dirs.push(d.to_string());
    }

    for file in compile_files {
        println!("Compile: {}", file);

        tonic_build::configure()
            .build_server(false)
            .build_client(true)
            .out_dir(DEST_DIR)
            .compile(&[file],
                     &include_dirs)
            .expect("failed to compile protobuf");

    }

    // generate lib.rs
    let mut modules = HashSet::<String>::new();
    let mut mod_content = String::default();
    for file in fs::read_dir(DEST_DIR).unwrap() {
        let fname = file.unwrap().file_name().to_str().unwrap().to_owned();
        if fname == "lib.rs" {
            continue
        }

        let mod_name = match fname.strip_suffix( ".rs"){
            Some(mod_name) => mod_name,
            None => continue,
        };

        modules.insert(mod_name.to_string());
        for (i, ch) in mod_name.char_indices() {
            if ch == '.' {
                modules.insert(mod_name[..i].to_string());
            }
        }
    }

    let mut modules:Vec<String> = modules.iter().cloned().collect();
    modules.sort_unstable_by(|a,b| {
        let mut a_chars = a.chars();
        let mut b_chars = b.chars();
        loop {
            let a_ch = a_chars.next();
            let b_ch = b_chars.next();

            if a_ch == b_ch {
                if a_ch.is_none() {
                    return Ordering::Equal
                }
                continue
            };
            let a_ch = match a_ch {
                Some(ch)=>ch,
                None => return Ordering::Less
            };
            let b_ch = match b_ch {
                Some(ch)=>ch,
                None => return Ordering::Greater
            };

            return if a_ch == '.' {
                Ordering::Less
            } else if b_ch == '.' {
                Ordering::Greater
            } else {
                a_ch.cmp(&b_ch)
            };
        }
    });

    let mut mod_content = String::new();
    for mod_name in modules {
        let parts  = mod_name.split(".").collect_vec();
        let last_part = parts.last().unwrap();
        mod_content += format!("mod {} {")
    }

    let mut lib_file = fs::File::create("src/lib.rs").unwrap();
    lib_file.write_all(mod_content.as_bytes()).unwrap();

    Ok(())
}

fn main_off() -> Result<(), Box<dyn std::error::Error>> {
    let mut include_dirs = Vec::with_capacity(INCLUDE_DIRS.len());
    for d in INCLUDE_DIRS {
        include_dirs.push(d.to_string());
    }

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(DEST_DIR)
        .compile(&["../ydb-api-protos/ydb_import_v1.proto".to_string()],
                 &include_dirs)
        .expect("failed to compile protobuf");


    Ok(())
}