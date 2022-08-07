//! Crate contain helpers for generate grpc imports in ydb-grpc crate.
//! End customers should use crate ydb.

use std::collections::HashMap;

pub fn get_proto_package(s: &str) -> Option<&str> {
    for logic_line in s.split(';') {
        let line = logic_line.trim();
        if !line.to_lowercase().starts_with("package ") {
            continue;
        }
        let package_name = line["package ".len()..].trim();
        return Some(package_name);
    }
    None
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProtoModule {
    file_name: Option<String>,
    submodules: HashMap<String, ProtoModule>,
}

impl ProtoModule {
    pub fn add_file(&mut self, fname: &str) {
        let parts: Vec<_> = fname.split('.').collect();
        let mut current = self;
        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                current.file_name = Some(fname.to_string());
                continue;
            }
            let part = part.to_string();
            if current.submodules.get(&part).is_none() {
                current
                    .submodules
                    .insert(part.clone(), ProtoModule::default());
            }
            current = current.submodules.get_mut(&part).unwrap();
        }
    }

    pub fn to_string(&self) -> String {
        self.to_string_with_indent("")
    }

    fn to_string_with_indent(&self, indent: &str) -> String {
        let mut res = String::new();

        if let Some(file_name) = &self.file_name {
            res += format!("{}include!(\"{}\");\n", indent, file_name).as_str();
        }

        let mut keys: Vec<_> = self.submodules.keys().collect();
        keys.sort();

        for key in keys {
            res += format!("{}pub mod {} {{\n", indent, key).as_str();

            let m = self.submodules.get(key).unwrap();
            res += m
                .to_string_with_indent(format!("{}  ", indent).as_str())
                .as_str();

            res += format!("{}}}\n", indent).as_str()
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_get_proto_package() {
        assert_eq!(get_proto_package(""), None);
        assert_eq!(get_proto_package("asd"), None);
        assert_eq!(get_proto_package("package asd"), Some("asd"));
        assert_eq!(get_proto_package("package asd;"), Some("asd"));
        assert_eq!(
            get_proto_package(
                "saldjhfsalkdjf;\
        package \
        asd;\
        "
            ),
            Some("asd")
        );
    }

    #[test]
    fn test_add_file() {
        let mut m = ProtoModule::default();
        m.add_file("asd.rs");
        let asd = "asd".to_string();
        let mut expected_m = ProtoModule::default();
        expected_m
            .submodules
            .insert(asd, ProtoModule::default());
        expected_m.submodules.get_mut("asd").unwrap().file_name = Some("asd.rs".to_string());
        assert_eq!(m, expected_m);

        m.add_file("asd.sub.v1.rs");
        let sub = "sub".to_string();
        let v1 = "v1".to_string();
        expected_m
            .submodules
            .get_mut("asd")
            .unwrap()
            .submodules
            .insert(sub.clone(), ProtoModule::default());
        expected_m
            .submodules
            .get_mut("asd")
            .unwrap()
            .submodules
            .get_mut(&sub)
            .unwrap()
            .submodules
            .insert(v1.clone(), ProtoModule::default());
        expected_m
            .submodules
            .get_mut("asd")
            .unwrap()
            .submodules
            .get_mut(&sub)
            .unwrap()
            .submodules
            .get_mut(&v1)
            .unwrap()
            .file_name = Some("asd.sub.v1.rs".to_string());
        assert_eq!(m, expected_m);
    }

    #[test]
    fn test_to_string() {
        let m = ProtoModule {
            file_name: None,
            submodules: [(
                "asd".to_string(),
                ProtoModule {
                    file_name: None,
                    submodules: [(
                        "v1".to_string(),
                        ProtoModule {
                            file_name: Some("asd.v1.rs".to_string()),
                            submodules: HashMap::default(),
                        },
                    )]
                    .iter()
                    .cloned()
                    .collect(),
                },
            )]
            .iter()
            .cloned()
            .collect(),
        };

        let s = m.to_string();
        let expected = "pub mod asd {
  pub mod v1 {
    include!(\"asd.v1.rs\");
  }
}
";
        assert_eq!(s, expected);
    }
}
