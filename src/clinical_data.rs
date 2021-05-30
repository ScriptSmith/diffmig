use std::error::Error;
use serde_json::Value;
use std::mem::discriminant;
use crate::diff::{Diff, eq_diff, variant_diff};
use std::collections::{HashMap, HashSet, BTreeSet};
use itertools::Itertools;

#[derive(Debug)]
pub struct CDEFileValue<'a> {
    file_name: &'a str,
    django_file_id: u32,
}

#[derive(Debug)]
pub enum CDEValue<'a> {
    Null,
    Bool(bool),
    EmptyString,
    String(&'a str),
    Number(f64),
    EmptyRange,
    Range(HashSet<&'a str>),
    File(CDEFileValue<'a>),
}

#[derive(Debug)]
pub struct CDE<'a> {
    code: &'a str,
    value: CDEValue<'a>,
}

#[derive(Debug)]
pub enum CDEDifferenceType<'a> {
    Missing(Option<&'a CDE<'a>>, Option<&'a CDE<'a>>),
    Variant(&'a CDEValue<'a>, &'a CDEValue<'a>),
    Equality(&'a CDEValue<'a>, &'a CDEValue<'a>),
}

#[derive(Debug)]
pub struct CDEDifference<'a> {
    code: &'a str,
    diff: CDEDifferenceType<'a>,
}

impl<'a> Diff<'a> for CDE<'a> {
    type Difference = CDEDifference<'a>;

    fn diff(&'a self, comp: &'a Self) -> Option<Vec<Self::Difference>> {
        let mut diffs = vec![];

        variant_diff!(&self.value, &comp.value, diffs, CDEDifferenceType::Variant);

        match (&self.value, &comp.value) {
            (CDEValue::Null, CDEValue::Null) => {}
            (CDEValue::EmptyString, CDEValue::EmptyString) => {}
            (CDEValue::EmptyRange, CDEValue::EmptyRange) => {}
            (CDEValue::Bool(b1), CDEValue::Bool(b2)) => {
                eq_diff!(b1 != b2, &self.value, &comp.value, diffs, CDEDifferenceType::Equality);
            }
            (CDEValue::String(s1), CDEValue::String(s2)) => {
                eq_diff!(s1 != s2, &self.value, &comp.value, diffs, CDEDifferenceType::Equality);
            }
            (CDEValue::Number(n1), CDEValue::Number(n2)) => {
                eq_diff!((n1 - n2).abs() > 0.01, &self.value, &comp.value, diffs, CDEDifferenceType::Equality);
            }
            (CDEValue::Range(r1), CDEValue::Range(r2)) => {
                eq_diff!(r1 != r2, &self.value, &comp.value, diffs, CDEDifferenceType::Equality);
            }
            (CDEValue::File(f1), CDEValue::File(f2)) => {
                eq_diff!(f1.file_name != f2.file_name || f1.django_file_id != f2.django_file_id,
                    &self.value, &comp.value, diffs, CDEDifferenceType::Equality);
            }
            (_, _) => {}
        }

        match diffs.is_empty() {
            true => None,
            false => Some(diffs.into_iter().map(|d| CDEDifference { code: self.code, diff: d }).collect())
        }
    }
}

type CDEMap<'a> = HashMap<&'a str, CDE<'a>>;

#[derive(Debug)]
pub enum CDESVariant<'a> {
    Single(CDEMap<'a>),
    Multiple(Vec<CDEMap<'a>>),
}

#[derive(Debug)]
pub struct Section<'a> {
    code: &'a str,
    allow_multiple: bool,
    cdes: CDESVariant<'a>,
}

#[derive(Debug)]
pub enum SectionDifferenceType<'a> {
    Missing(Option<&'a Section<'a>>, Option<&'a Section<'a>>),
    Code(&'a str, &'a str),
    AllowMultiple(bool, bool),
    Variant(&'a CDESVariant<'a>, &'a CDESVariant<'a>),
    CDEs(Vec<CDEDifference<'a>>),
}

#[derive(Debug)]
pub struct SectionDifference<'a> {
    code: &'a str,
    diff: SectionDifferenceType<'a>,
}

impl<'a> Diff<'a> for Section<'a> {
    type Difference = SectionDifference<'a>;

    fn diff(&'a self, comp: &'a Self) -> Option<Vec<Self::Difference>> {
        let mut diffs = vec![];

        eq_diff!(self.code, comp.code, diffs, SectionDifferenceType::Code);
        eq_diff!(self.allow_multiple, comp.allow_multiple, diffs, SectionDifferenceType::AllowMultiple);
        variant_diff!(&self.cdes, &comp.cdes, diffs, SectionDifferenceType::Variant);

        fn diff_cdes<'a>(c1: &'a CDEMap, c2: &'a CDEMap) -> Option<Vec<CDEDifference<'a>>> {
            let mut diffs = vec![];

            c1.iter().for_each(|(k, v1)| {
                match c2.get(k) {
                    None => diffs.push(CDEDifference { code: k, diff: CDEDifferenceType::Missing(Some(v1), None) }),
                    Some(v2) => match v1.diff(v2) {
                        None => {}
                        Some(cde_diffs) => diffs.extend(cde_diffs)
                    }
                }
            });

            c2.iter().for_each(|(k, v)| {
                match c1.get(k) {
                    None => diffs.push(CDEDifference { code: k, diff: CDEDifferenceType::Missing(None, Some(v)) }),
                    Some(_) => {}
                }
            });

            match diffs.is_empty() {
                true => None,
                false => Some(diffs)
            }
        }

        match (&self.cdes, &comp.cdes) {
            (CDESVariant::Single(c1), CDESVariant::Single(c2)) => {
                match diff_cdes(c1, c2) {
                    None => {}
                    Some(d) => diffs.push(SectionDifferenceType::CDEs(d))
                }
            }
            (CDESVariant::Multiple(v1), CDESVariant::Multiple(v2)) => {
                v1.iter().zip(v2.iter()).for_each(|(c1, c2)| {
                    match diff_cdes(c1, c2) {
                        None => {}
                        Some(d) => diffs.push(SectionDifferenceType::CDEs(d))
                    }
                })
            }
            (_, _) => {}
        }

        match diffs.is_empty() {
            true => None,
            false => Some(diffs.into_iter().map(|d| SectionDifference { code: self.code, diff: d }).collect())
        }
    }
}

#[derive(Debug)]
pub struct Form<'a> {
    name: &'a str,
    sections: HashMap<&'a str, Section<'a>>,
}

#[derive(Debug)]
pub enum FormDifferenceType<'a> {
    Missing(Option<&'a Form<'a>>, Option<&'a Form<'a>>),
    Name(&'a str, &'a str),
    Sections(Vec<SectionDifference<'a>>),
}

#[derive(Debug)]
pub struct FormDifference<'a> {
    name: &'a str,
    diff: FormDifferenceType<'a>,
}

impl<'a> Diff<'a> for Form<'a> {
    type Difference = FormDifference<'a>;

    fn diff(&'a self, comp: &'a Self) -> Option<Vec<Self::Difference>> {
        let mut diffs = vec![];

        eq_diff!(self.name, comp.name, diffs, FormDifferenceType::Name);

        let mut section_diffs = vec![];
        self.sections.iter().for_each(|(k, v1)| {
            match comp.sections.get(k) {
                None => section_diffs.push(SectionDifference { code: k, diff: SectionDifferenceType::Missing(Some(v1), None) }),
                Some(v2) => {
                    match v1.diff(v2) {
                        None => {}
                        Some(d) => section_diffs.extend(d)
                    }
                }
            }
        });

        comp.sections.iter().for_each(|(k, v)| {
            match self.sections.get(k) {
                None => section_diffs.push(SectionDifference { code: k, diff: SectionDifferenceType::Missing(None, Some(v)) }),
                Some(_) => {}
            }
        });

        match section_diffs.is_empty() {
            true => {}
            false => diffs.push(FormDifferenceType::Sections(section_diffs))
        }

        match diffs.is_empty() {
            true => None,
            false => Some(diffs.into_iter().map(|d| FormDifference { name: self.name, diff: d }).collect())
        }
    }
}

#[derive(Debug)]
pub enum ClinicalDatumVariant { History, CDEs }

#[derive(Debug)]
pub struct ClinicalDatum<'a> {
    pub id: u32,
    pub patient: u32,
    variant: ClinicalDatumVariant,
    forms: HashMap<&'a str, Form<'a>>,
}

#[derive(Debug)]
pub enum ClinicalDatumDifferenceType<'a> {
    Patient(u32, u32),
    Variant(&'a ClinicalDatumVariant, &'a ClinicalDatumVariant),
    Forms(Vec<FormDifference<'a>>),
}

#[derive(Debug)]
pub struct ClinicalDatumDifference<'a> {
    proto_context: BTreeSet<String>,
    diff: ClinicalDatumDifferenceType<'a>,
}

impl<'a> Diff<'a> for ClinicalDatum<'a> {
    type Difference = ClinicalDatumDifference<'a>;

    fn diff(&'a self, comp: &'a Self) -> Option<Vec<Self::Difference>> {
        let mut diffs = vec![];

        eq_diff!(self.patient, comp.patient, diffs, ClinicalDatumDifferenceType::Patient);
        variant_diff!(&self.variant, &comp.variant, diffs, ClinicalDatumDifferenceType::Variant);

        let mut form_diffs = vec![];

        self.forms.iter().for_each(|(k, v1)| {
            match comp.forms.get(k) {
                None => form_diffs.push(FormDifference { name: k, diff: FormDifferenceType::Missing(Some(v1), None) }),
                Some(v2) => {
                    match v1.diff(v2) {
                        None => {}
                        Some(d) => form_diffs.extend(d)
                    }
                }
            }
        });

        comp.forms.iter().for_each(|(k, v)| {
            match self.forms.get(k) {
                None => form_diffs.push(FormDifference { name: k, diff: FormDifferenceType::Missing(None, Some(v)) }),
                Some(_) => {}
            }
        });

        match form_diffs.is_empty() {
            true => {}
            false => diffs.push(ClinicalDatumDifferenceType::Forms(form_diffs))
        }

        match diffs.is_empty() {
            true => None,
            false => Some(diffs.into_iter().map(|d| ClinicalDatumDifference { proto_context: self.forms.keys().map(|k| String::from(*k)).collect(), diff: d }).collect())
        }
    }
}

impl<'a> ClinicalDatum<'a> {
    pub fn from(datum: &'a serde_json::Value) -> Result<Option<ClinicalDatum>, Box<dyn Error>> {
        let map = datum.as_object()
            .ok_or("Not an object")?;
        let fields = map.get("fields")
            .ok_or("Missing fields")?;
        let data = fields.get("data")
            .ok_or("Missing data")?;

        let id = map.get("pk")
            .ok_or("Missing PK")?
            .as_i64().ok_or("Invalid PK")? as u32;
        let patient = fields.get("django_id")
            .ok_or("Missing patient")?
            .as_i64().ok_or("Invalid patient")? as u32;
        let variant = fields.get("collection")
            .ok_or("Missing collection")?
            .as_str().ok_or("Invalid collection")?;
        let variant = match variant {
            "cdes" => ClinicalDatumVariant::CDEs,
            "history" => ClinicalDatumVariant::History,
            _ => return Ok(None) // Ignore non history & cdes entries
        };

        let forms = match variant {
            ClinicalDatumVariant::CDEs => data.get("forms"),
            ClinicalDatumVariant::History => data.get("record")
                .ok_or("Missing record")?
                .get("forms")
        };
        let forms = Self::get_forms(forms
            .ok_or("Missing forms")?
            .as_array().ok_or("Invalid forms")?
        )?;

        Ok(Some(ClinicalDatum { id, patient, variant, forms }))
    }

    pub fn proto_context(&self) -> BTreeSet<String> {
        self.forms.keys().map(|k| String::from(*k)).collect()
    }

    fn get_forms(forms: &Vec<serde_json::Value>) -> Result<HashMap<&str, Form>, Box<dyn Error>> {
        let forms_map = forms.iter().map(|data| {
            let form = data.as_object().ok_or("Invalid form")?;
            let name = form.get("name")
                .ok_or("Missing form name")?
                .as_str().ok_or("Invalid form name")?;
            let sections = Self::get_sections(form.get("sections")
                .ok_or("Missing form sections")?
                .as_array().ok_or("Invalid form sections")?
            )?;

            Ok((name, Form { name, sections }))
        }).collect::<Result<HashMap<&str, Form>, Box<dyn Error>>>()?;

        match forms.len() != forms_map.len() {
            true => Err("List of forms contains duplicates".into()),
            false => Ok(forms_map)
        }
    }

    fn get_sections(sections: &Vec<serde_json::Value>) -> Result<HashMap<&str, Section>, Box<dyn Error>> {
        let sections_map = sections.iter().map(|data| {
            let section = data.as_object().ok_or("Invalid section")?;
            let code = section.get("code")
                .ok_or("Missing section code")?
                .as_str().ok_or("Invalid section code")?;
            let allow_multiple = section.get("allow_multiple")
                .ok_or("Missing section allow_multiple")?
                .as_bool().ok_or("Invalid section allow_multiple")?;
            let cdes = section.get("cdes")
                .ok_or("Missing section cdes")?
                .as_array().ok_or("Invalid section cdes")?;
            let cdes = match allow_multiple {
                false => CDESVariant::Single(Self::get_cdes(cdes)?),
                true => CDESVariant::Multiple(cdes.iter().map(|l| {
                    Ok(Self::get_cdes(l.as_array().ok_or("Invalid section cdes list")?)?)
                }).collect::<Result<Vec<HashMap<&str, CDE>>, Box<dyn Error>>>()?),
            };

            Ok((code, Section { code, allow_multiple, cdes }))
        }).collect::<Result<HashMap<&str, Section>, Box<dyn Error>>>()?;

        match sections.len() != sections_map.len() {
            true => Err("List of sections contains duplicates".into()),
            false => Ok(sections_map)
        }
    }

    fn get_cdes(cdes: &Vec<serde_json::Value>) -> Result<HashMap<&str, CDE>, Box<dyn Error>> {
        let cde_map = cdes.iter().map(|data| {
            let cde = data.as_object().ok_or("Invalid cde")?;
            let code = cde.get("code")
                .ok_or("Missing cde code")?
                .as_str().ok_or("Invalid cde code")?;
            let value = cde.get("value")
                .ok_or("Missing cde value")?;
            let value = Self::get_cde_value(value)?.ok_or("Invalid cde value")?;

            Ok((code, CDE { code, value }))
        }).collect::<Result<HashMap<&str, CDE>, Box<dyn Error>>>()?;

        if cde_map.len() != cdes.len() {
            Err("List of CDEs contains duplicates".into())
        } else {
            Ok(cde_map)
        }
    }

    fn get_cde_value(value: &serde_json::Value) -> Result<Option<CDEValue>, Box<dyn Error>> {
        let cde_value = match value {
            Value::Bool(b) => Some(CDEValue::Bool(*b)),
            Value::Object(o) => {
                let file_name = o.get("file_name");
                let django_file_id = o.get("django_file_id");

                match (file_name, django_file_id) {
                    (Some(Value::String(file_name)), Some(Value::Number(django_file_id))) => {
                        let django_file_id = django_file_id.as_u64().unwrap() as u32;
                        Some(CDEValue::File(CDEFileValue { file_name, django_file_id }))
                    }
                    _ => None,
                }
            }
            Value::Null => Some(CDEValue::Null),
            Value::Number(n) => Some(CDEValue::Number(n.as_f64().unwrap())),
            Value::String(s) => match s.as_str() {
                "" => Some(CDEValue::EmptyString),
                s => Some(CDEValue::String(s))
            },
            Value::Array(a) => {
                let range = a.iter().map(|s| {
                    Ok(s.as_str().ok_or("Invalid range cde value")?)
                }).collect::<Result<HashSet<&str>, Box<dyn Error>>>()?;

                match range.is_empty() {
                    true => Some(CDEValue::EmptyRange),
                    false => Some(CDEValue::Range(range))
                }
            }
        };

        Ok(cde_value)
    }
}

#[derive(Debug)]
pub struct ClinicalDatumWrapper {
    value: Value,
}

impl ClinicalDatumWrapper {
    pub fn from(value: Value) -> ClinicalDatumWrapper {
        ClinicalDatumWrapper { value }
    }

    pub fn clinical_datum(&self) -> Result<Option<ClinicalDatum>, Box<dyn Error>> {
        ClinicalDatum::from(&self.value)
    }

    pub fn cd(&self) -> ClinicalDatum {
        self.clinical_datum().unwrap().unwrap()
    }
}

#[derive(Debug)]
pub struct PatientSlice {
    patient: u32,
    clinical_data: HashMap<BTreeSet<String>, ClinicalDatumWrapper>,
}

impl<'a> PatientSlice {
    pub fn from(patient: u32) -> PatientSlice {
        PatientSlice { patient, clinical_data: HashMap::new() }
    }

    pub fn can_add(&mut self, datum: &ClinicalDatum) -> bool {
        let proto_context = datum.proto_context();
        !self.clinical_data.contains_key(&proto_context) && datum.patient == self.patient
    }

    pub fn add(&mut self, wrapper: ClinicalDatumWrapper) {
        let proto_context = wrapper.clinical_datum().unwrap().unwrap().proto_context();
        self.clinical_data.insert(proto_context, wrapper);
    }
}

impl PatientSlice {
    pub fn print_diffs(&self, comp: &Self) -> Option<usize> {
        let mut diffs = 0;

        if self.patient != comp.patient {
            eprintln!("Patient {} doesn't match {}", self.patient, comp.patient);
            diffs += 1;
        }

        self.clinical_data.iter().for_each(|(k, v1)| {
            match comp.clinical_data.get(k) {
                None => {
                    eprintln!("New missing proto-context: [{:#?}]", k.iter().join(", "));
                    eprintln!("[{}]", self.clinical_data.values().map(|cdw| cdw.cd().id).sorted().join(","));
                    diffs += 1;
                }
                Some(v2) => match v1.cd().diff(&v2.cd()) {
                    None => {}
                    Some(d) => {
                        eprintln!("({}) ({}):", v1.cd().id, v2.cd().id);
                        eprintln!("{:#?}", d);
                        diffs += 1;
                    }
                }
            }
        });

        comp.clinical_data.iter().for_each(|(k, v)| {
            match self.clinical_data.get(k) {
                None => {
                    eprintln!("Old missing proto-context: [{:#?}]", k.iter().join(", "));
                    eprintln!("[{}]", comp.clinical_data.values().map(|cdw| cdw.cd().id).sorted().join(","));
                    diffs += 1;
                }
                Some(_) => {}
            }
        });

        match diffs == 0 {
            true => None,
            false => {
                eprintln!();
                Some(diffs)
            }
        }
    }
}
