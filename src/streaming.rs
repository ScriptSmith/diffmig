use std::io::{BufReader, Read, BufRead};
use serde_json::{Value, from_str};
use crate::clinical_data::{PatientSlice, ClinicalDatumWrapper};
use std::iter::Peekable;

/// Takes a reader of a large JSON array, and returns an iterator that
/// reads each element sequentially
///
/// serde_json won't read a large array of arbitrary values sequentially
/// (ie. one at a time rather than all at once).
///
/// https://github.com/serde-rs/json/issues/404
/// https://github.com/serde-rs/json/pull/760
/// https://serde.rs/stream-array.html
///
/// It does work for LD-JSON and similar
///
/// https://docs.serde.rs/serde_json/de/struct.StreamDeserializer.html
///
/// Reading sequentially reduces the memory usage for large migrations
///
/// This function only works with JSON arrays structured the same
/// way as in registry exports, so won't support other large arrays
/// with different indentation etc.
pub fn read_array_file_to_values<'a>(reader: impl Read + 'a) -> impl Iterator<Item=Value> + 'a {
    let reader = BufReader::new(reader);
    let mut partial = Vec::<String>::new();
    reader.lines().scan(Option::<Value>::None, move |_complete, line| {
        match line.expect("Failed reading line from file").as_str() {
            "[" => Some(None),
            "]" => None,
            "    }" | "    }," => {
                partial.push("}".to_string());
                let value = from_str::<Value>(&partial.join("\n"))
                    .expect("Failed parsing JSON array entry");
                partial.clear();
                Some(Some(value))
            }
            l => {
                partial.push(l.to_string());
                Some(None)
            }
        }
    }).filter_map(|v| v)
}

pub fn map_values_to_clinical_data(values: impl Iterator<Item=Value>) -> impl Iterator<Item=ClinicalDatumWrapper> {
    values.map(|value| ClinicalDatumWrapper::from(value))
}

pub struct RegistryData<'a> {
    clinical_data: Box<Peekable<Box<dyn Iterator<Item=ClinicalDatumWrapper> + 'a>>>,
}

impl<'a> RegistryData<'a> {
    pub fn from(clinical_data: Box<dyn Iterator<Item=ClinicalDatumWrapper> + 'a>) -> RegistryData<'a> {
        RegistryData { clinical_data: Box::new(clinical_data.peekable())}
    }
}

impl<'a> Iterator for RegistryData<'a> {
    type Item = PatientSlice;

    fn next(&mut self) -> Option<Self::Item> {
        match self.clinical_data.next() {
            None => return None,
            Some(cdw) => {
                let cd = cdw.clinical_datum().unwrap();
                if cd.is_none() {
                    return self.next();
                }
                let cd = cd.unwrap();
                let mut slice = PatientSlice::from(cd.patient);

                loop {
                    match self.clinical_data.peek() {
                        None => return None,
                        Some(cdw) => {
                            match cdw.clinical_datum().unwrap() {
                                None => {}
                                Some(cd) => {
                                    match slice.can_add(&cd) {
                                        true => { slice.add(self.clinical_data.next().unwrap()) },
                                        false => return Some(slice),
                                    };
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}