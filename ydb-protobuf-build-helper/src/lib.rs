use std::collections::HashMap;

#[derive(Debug, Default, PartialEq)]
pub struct ProtoModule {
    file_name: Option<String>,
    submodules: HashMap<String, ProtoModule>,
}

impl ProtoModule {
    pub fn add_file(self: &mut Self, fname: &str){
        let parts : Vec<_>= fname.split(".").collect();
        let mut current = self;
        for (i, part) in parts.iter().enumerate() {
            if i == parts.len()-1 {
                current.file_name = Some(fname.to_string());
                continue
            }
            let part = part.to_string();
            if current.submodules.get(&part).is_none() {
                current.submodules.insert(part.clone(), ProtoModule::default());
            }
            current = current.submodules.get_mut(&part).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_add_file(){
        let mut m = ProtoModule::default();
        m.add_file("asd.rs");
        let asd = "asd".to_string();
        let mut expected_m = ProtoModule::default();
        expected_m.submodules.insert(asd.clone(), ProtoModule::default());
        expected_m.submodules.get_mut("asd").unwrap().file_name = Some("asd.rs".to_string());
        assert_eq!(m, expected_m);

        m.add_file("asd.sub.v1.rs");
        let sub = "sub".to_string();
        let v1 = "v1".to_string();
        expected_m.submodules.get_mut("asd").unwrap().submodules.insert(sub.clone(), ProtoModule::default());
        expected_m.submodules.get_mut("asd").unwrap().submodules
            .get_mut(&sub).unwrap().submodules.insert(v1.clone(), ProtoModule::default());
        expected_m.submodules.get_mut("asd").unwrap().submodules
            .get_mut(&sub).unwrap().submodules
            .get_mut(&v1).unwrap().file_name = Some("asd.sub.v1.rs".to_string());
        assert_eq!(m, expected_m);
    }
}
