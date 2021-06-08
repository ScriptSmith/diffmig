use serde_json::{Value};
use std::collections::{HashMap, HashSet};
use std::error::Error;

pub struct RegistryDefinition {
    pub forms: HashMap<String, FormDefinition>,
    pub sections: HashMap<String, SectionDefinition>,
}

impl RegistryDefinition {
    pub fn new(forms: &Vec<Value>, sections: &Vec<Value>) -> Result<RegistryDefinition, Box<dyn Error>> {
        let forms = Self::get_forms(forms)?;
        let sections = Self::get_sections(sections)?;

        Ok(RegistryDefinition { forms, sections })
    }

    fn get_forms(values: &Vec<Value>) -> Result<HashMap<String, FormDefinition>, Box<dyn Error>> {
        values.iter().map(|value| {
            let fields = value.as_object()
                .ok_or("Invalid data")?
                .get("fields")
                .ok_or("Missing fields")?;
            let name = fields.get("name")
                .ok_or("Missing form name")?
                .as_str()
                .ok_or("Invalid form name")?
                .to_string();
            let sections = fields.get("sections")
                .ok_or("Missing form sections")?
                .as_str()
                .ok_or("Invalid form sections")?
                .split(",")
                .map(|s| s.trim().to_string())
                .collect();

            Ok((name.clone(), FormDefinition { name, sections }))
        }).collect()
    }

    fn get_sections(values: &Vec<Value>) -> Result<HashMap<String, SectionDefinition>, Box<dyn Error>> {
        values.iter().map(|value| {
            let fields = value.as_object()
                .ok_or("Invalid data")?
                .get("fields")
                .ok_or("Missing fields")?;
            let code = fields.get("code")
                .ok_or("Missing section code")?
                .as_str()
                .ok_or("Invalid section code")?
                .to_string();
            let cdes = fields.get("elements")
                .ok_or("Missing section cdes")?
                .as_str()
                .ok_or("Invalid section cdes")?
                .split(",")
                .map(|s| s.trim().to_string())
                .collect();

            Ok((code.clone(), SectionDefinition { code, cdes }))
        }).collect()
    }
}

pub struct FormDefinition {
    pub name: String,
    pub sections: HashSet<String>,
}

pub struct SectionDefinition {
    pub code: String,
    pub cdes: Vec<String>,
}