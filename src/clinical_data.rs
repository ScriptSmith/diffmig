use itertools::Itertools;
use serde_json::Value;
use std::collections::{HashMap, HashSet, BTreeSet};
use std::error::Error;
use std::mem::discriminant;

use crate::diff::{Diff, eq_diff, variant_diff};
use crate::registry_definition::{RegistryDefinition};

#[derive(Debug)]
pub struct CDEFileValue {
    file_name: String,
    django_file_id: u32,
}

#[derive(Debug)]
pub enum CDEValue {
    Null,
    Bool(bool),
    EmptyString,
    String(String),
    Number(f64),
    EmptyRange,
    Range(HashSet<String>),
    File(CDEFileValue),
}

#[derive(Debug)]
pub struct CDE {
    code: String,
    value: CDEValue,
}

type CDEMap = HashMap<String, CDE>;

#[derive(Debug)]
pub enum CDESVariant {
    Single(CDEMap),
    Multiple(Vec<CDEMap>),
}

#[derive(Debug)]
pub struct Section {
    code: String,
    allow_multiple: bool,
    cdes: CDESVariant,
}

#[derive(Debug)]
pub struct Form {
    name: String,
    sections: HashMap<String, Section>,
}

#[derive(Debug)]
pub enum ClinicalDatumVariant { History, CDEs }

#[derive(Debug)]
pub struct ClinicalDatum {
    pub id: u32,
    pub patient: u32,
    pub variant: ClinicalDatumVariant,
    forms: HashMap<String, Form>,
}

type ProtoContext = BTreeSet<String>;

impl<'a> ClinicalDatum {
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

    pub fn validate(&self, definition: &RegistryDefinition) -> Vec<String> {
        let mut errors = vec![];
        self.forms.iter().for_each(|(form_name, form)| match definition.forms.get(form_name) {
            None => errors.push(format!("Clinical datum contains extra form: {}", form_name)),
            Some(form_definition) => {
                form.sections.iter().for_each(|(section_code, section)| match form_definition.sections.get(section_code) {
                    None => errors.push(format!("Clinical datum's form {} contains extra section: {}", form_name, section_code)),
                    Some(_) => match definition.sections.get(section_code) {
                        None => errors.push(format!("Section definition {} doesn't exist", section_code)),
                        Some(section_definition) => {
                            let mut validate_cde_map = |cdes: &CDEMap| {
                                cdes.iter().for_each(|(cde_code, _)| match section_definition.cdes.contains(cde_code) {
                                    true => {},
                                    false => errors.push(format!("Clinical datum's form {} section {} contains extra cde: {}", form_name, section_code, cde_code))
                                });

                                section_definition.cdes.iter().for_each(|cde_code| match cdes.get(cde_code) {
                                    None => errors.push(format!("Clinical datum's form {} section {} is missing cde: {}", form_name, section_code, cde_code)),
                                    Some(_) => {}
                                });
                            };

                            match &section.cdes {
                                CDESVariant::Single(cde_map) => validate_cde_map(cde_map),
                                CDESVariant::Multiple(cde_maps) => cde_maps.iter().for_each(|cde_map| validate_cde_map(cde_map)),
                            }
                        }
                    }
                });

                form_definition.sections.iter().for_each(|section_code| match form.sections.get(section_code) {
                    None => errors.push(format!("Clinical datum's form {} is missing section: {}", form_name, section_code)),
                    Some(_) => {}
                });
            }
        });

        errors
    }

    pub fn proto_context(&self) -> ProtoContext {
        self.forms.keys().map(|k| k.to_string()).collect()
    }

    fn get_forms(forms: &[serde_json::Value]) -> Result<HashMap<String, Form>, Box<dyn Error>> {
        let forms_map = forms.iter().map(|data| {
            let form = data.as_object().ok_or("Invalid form")?;
            let name = form.get("name")
                .ok_or("Missing form name")?
                .as_str().ok_or("Invalid form name")?
                .to_string();
            let sections = Self::get_sections(form.get("sections")
                .ok_or("Missing form sections")?
                .as_array().ok_or("Invalid form sections")?
            )?;

            Ok((name.clone(), Form { name, sections }))
        }).collect::<Result<HashMap<String, Form>, Box<dyn Error>>>()?;

        match forms.len() != forms_map.len() {
            true => Err("List of forms contains duplicates".into()),
            false => Ok(forms_map)
        }
    }

    fn get_sections(sections: &[serde_json::Value]) -> Result<HashMap<String, Section>, Box<dyn Error>> {
        let sections_map = sections.iter().map(|data| {
            let section = data.as_object().ok_or("Invalid section")?;
            let code = section.get("code")
                .ok_or("Missing section code")?
                .as_str().ok_or("Invalid section code")?
                .to_string();
            let allow_multiple = section.get("allow_multiple")
                .ok_or("Missing section allow_multiple")?
                .as_bool().ok_or("Invalid section allow_multiple")?;
            let cdes = section.get("cdes")
                .ok_or("Missing section cdes")?
                .as_array().ok_or("Invalid section cdes")?;
            let cdes = match allow_multiple {
                false => CDESVariant::Single(Self::get_cdes(cdes)?),
                true => CDESVariant::Multiple(cdes.iter().map(|l| {
                    Self::get_cdes(l.as_array().ok_or("Invalid section cdes list")?)
                }).collect::<Result<Vec<HashMap<String, CDE>>, Box<dyn Error>>>()?),
            };

            Ok((code.clone(), Section { code, allow_multiple, cdes }))
        }).collect::<Result<HashMap<String, Section>, Box<dyn Error>>>()?;

        match sections.len() != sections_map.len() {
            true => Err("List of sections contains duplicates".into()),
            false => Ok(sections_map)
        }
    }

    fn get_cdes(cdes: &[serde_json::Value]) -> Result<HashMap<String, CDE>, Box<dyn Error>> {
        let cde_map = cdes.iter().map(|data| {
            let cde = data.as_object().ok_or("Invalid cde")?;
            let code = cde.get("code")
                .ok_or("Missing cde code")?
                .as_str().ok_or("Invalid cde code")?
                .to_string();
            let value = cde.get("value")
                .ok_or("Missing cde value")?;
            let value = Self::get_cde_value(value)?.ok_or("Invalid cde value")?;

            Ok((code.clone(), CDE { code, value }))
        }).collect::<Result<HashMap<String, CDE>, Box<dyn Error>>>()?;

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
                let gridfs_file_id = o.get("gridfs_file_id");

                match (file_name, django_file_id, gridfs_file_id) {
                    (Some(Value::String(file_name)), Some(Value::Number(django_file_id)), _) => {
                        let django_file_id = django_file_id.as_u64().unwrap() as u32;
                        Some(CDEValue::File(CDEFileValue { file_name: file_name.to_string(), django_file_id }))
                    }
                    (Some(Value::String(file_name)), _, Some(Value::String(_))) => {
                        Some(CDEValue::File(CDEFileValue { file_name: file_name.to_string(), django_file_id: 0 }))
                    }
                    _ => None,
                }
            }
            Value::Null => Some(CDEValue::Null),
            Value::Number(n) => Some(CDEValue::Number(n.as_f64().unwrap())),
            Value::String(s) => match s.as_str() {
                "" => Some(CDEValue::EmptyString),
                s => Some(CDEValue::String(s.to_string()))
            },
            Value::Array(a) => {
                let range = a.iter().map(|s| {
                    Ok(s.as_str().ok_or("Invalid range cde value")?.to_string())
                }).collect::<Result<HashSet<String>, Box<dyn Error>>>()?;

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
pub struct PatientSlice {
    patient: u32,
    clinical_data: HashMap<ProtoContext, ClinicalDatum>,
}

impl<'a> PatientSlice {
    pub fn from(patient: u32) -> PatientSlice {
        PatientSlice { patient, clinical_data: HashMap::new() }
    }

    pub fn can_add(&mut self, datum: &ClinicalDatum) -> bool {
        let proto_context = datum.proto_context();
        !self.clinical_data.contains_key(&proto_context) && datum.patient == self.patient
    }

    pub fn add(&mut self, datum: ClinicalDatum) {
        let proto_context = datum.proto_context();
        self.clinical_data.insert(proto_context, datum);
    }
}

#[derive(Debug)]
pub enum CDEDifferenceType<'a> {
    Missing(Option<&'a CDE>, Option<&'a CDE>),
    Variant(&'a CDEValue, &'a CDEValue),
    Equality(&'a CDEValue, &'a CDEValue),
}

#[derive(Debug)]
pub struct CDEDifference<'a> {
    code: &'a str,
    diff: CDEDifferenceType<'a>,
}

impl<'a> Diff<'a> for CDE {
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
            false => Some(diffs.into_iter().map(|d| CDEDifference { code: self.code.as_str(), diff: d }).collect())
        }
    }
}

#[derive(Debug)]
pub enum SectionDifferenceType<'a> {
    Missing(Option<&'a Section>, Option<&'a Section>),
    Code(&'a str, &'a str),
    AllowMultiple(bool, bool),
    Variant(&'a CDESVariant, &'a CDESVariant),
    CDEs(Vec<CDEDifference<'a>>),
}

#[derive(Debug)]
pub struct SectionDifference<'a> {
    code: &'a str,
    diff: SectionDifferenceType<'a>,
}

impl<'a> Diff<'a> for Section {
    type Difference = SectionDifference<'a>;

    fn diff(&'a self, comp: &'a Self) -> Option<Vec<Self::Difference>> {
        let mut diffs = vec![];

        eq_diff!(self.code.as_str(), comp.code.as_str(), diffs, SectionDifferenceType::Code);
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
            false => Some(diffs.into_iter().map(|d| SectionDifference { code: self.code.as_str(), diff: d }).collect())
        }
    }
}

#[derive(Debug)]
pub enum FormDifferenceType<'a> {
    Missing(Option<&'a Form>, Option<&'a Form>),
    Name(&'a str, &'a str),
    Sections(Vec<SectionDifference<'a>>),
}

#[derive(Debug)]
pub struct FormDifference<'a> {
    name: &'a str,
    diff: FormDifferenceType<'a>,
}

impl<'a> Diff<'a> for Form {
    type Difference = FormDifference<'a>;

    fn diff(&'a self, comp: &'a Self) -> Option<Vec<Self::Difference>> {
        let mut diffs = vec![];

        eq_diff!(self.name.as_str(), comp.name.as_str(), diffs, FormDifferenceType::Name);

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

        if !section_diffs.is_empty() {
            diffs.push(FormDifferenceType::Sections(section_diffs));
        }

        match diffs.is_empty() {
            true => None,
            false => Some(diffs.into_iter().map(|d| FormDifference { name: self.name.as_str(), diff: d }).collect())
        }
    }
}

#[derive(Debug)]
pub enum ClinicalDatumDifferenceType<'a> {
    Missing(Option<&'a ClinicalDatum>, Option<&'a ClinicalDatum>),
    Patient(u32, u32),
    Variant(&'a ClinicalDatumVariant, &'a ClinicalDatumVariant),
    Forms(Vec<FormDifference<'a>>),
}

#[derive(Debug)]
pub struct ClinicalDatumDifference<'a> {
    proto_context: ProtoContext,
    diff: ClinicalDatumDifferenceType<'a>,
}

impl<'a> Diff<'a> for ClinicalDatum {
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

        if !form_diffs.is_empty() {
            diffs.push(ClinicalDatumDifferenceType::Forms(form_diffs));
        }

        match diffs.is_empty() {
            true => None,
            false => Some(diffs.into_iter().map(|d| ClinicalDatumDifference { proto_context: self.forms.keys().map(|k| k.to_string()).collect(), diff: d }).collect())
        }
    }
}

#[derive(Debug)]
pub enum PatientSliceDifferenceType<'a> {
    Patient(u32, u32),
    ClinicalData(Vec<ClinicalDatumDifference<'a>>),
}

#[derive(Debug)]
pub struct PatientSliceDifference<'a> {
    patient: u32,
    ids: String,
    diff: PatientSliceDifferenceType<'a>,
}

impl<'a> Diff<'a> for PatientSlice {
    type Difference = PatientSliceDifference<'a>;

    fn diff(&'a self, comp: &'a Self) -> Option<Vec<Self::Difference>> {
        let mut diffs = vec![];

        eq_diff!(self.patient, comp.patient, diffs, PatientSliceDifferenceType::Patient);

        let mut clinical_data_diffs = vec![];

        self.clinical_data.iter().for_each(|(k, v1)| {
            match comp.clinical_data.get(k) {
                None => clinical_data_diffs.push(ClinicalDatumDifference { proto_context: v1.proto_context(), diff: ClinicalDatumDifferenceType::Missing(Some(v1), None) }),
                Some(v2) => match v1.diff(&v2) {
                    None => {}
                    Some(d) => clinical_data_diffs.extend(d)
                }
            }
        });

        comp.clinical_data.iter().for_each(|(k, v)| {
            match self.clinical_data.get(k) {
                None => clinical_data_diffs.push(ClinicalDatumDifference { proto_context: v.proto_context(), diff: ClinicalDatumDifferenceType::Missing(None, Some(v)) }),
                Some(_) => {}
            }
        });

        if !clinical_data_diffs.is_empty() {
            diffs.push(PatientSliceDifferenceType::ClinicalData(clinical_data_diffs));
        }

        match diffs.is_empty() {
            true => None,
            false => Some(diffs.into_iter().map(|d| PatientSliceDifference { patient: self.patient, ids: self.clinical_data.values().map(|k| k.id).sorted().join(","), diff: d }).collect())
        }
    }
}
