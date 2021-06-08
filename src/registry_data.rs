use serde_json::{Value, from_str, to_string_pretty};
use std::io::{BufReader, Read, BufRead};
use std::iter::Peekable;

use crate::clinical_data::{PatientSlice, ClinicalDatum, ClinicalDatumVariant};
use crate::registry_definition::RegistryDefinition;

pub struct RegistryData<'a> {
    iterator: Box<Peekable<Box<dyn Iterator<Item=ClinicalDatum> + 'a>>>,
}

impl<'a> RegistryData<'a> {
    pub fn from(reader: impl Read + 'a, definition: &'a RegistryDefinition, cdes_only: bool) -> RegistryData<'a> {
        let values = Self::read_array_file_to_values(reader);
        let clinical_data = Self::map_values_to_clinical_data(values, definition, cdes_only);

        let iterator = Box::new(clinical_data.peekable());

        RegistryData { iterator }
    }

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
    pub fn read_array_file_to_values(reader: impl Read + 'a) -> impl Iterator<Item=Value> + 'a {
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
        }).flatten()
    }

    pub fn map_values_to_clinical_data(values: impl Iterator<Item=Value> + 'a, definition: &'a RegistryDefinition, cdes_only: bool) -> Box<dyn Iterator<Item=ClinicalDatum> + 'a> {
        let data = values.filter_map(move |value| match ClinicalDatum::from(&value) {
            Ok(Some(cd)) => {
                if let Err(e) = cd.validate(definition) {
                    println!("Clinical datum doesn't match definition: {}", e);
                }
                Some(cd)
            }
            Ok(None) => None,
            Err(e) => {
                log::error!("Error parsing clinical datum: {:#?}", e);
                log::debug!("Original value: {}", to_string_pretty(&value).unwrap());
                panic!()
            }
        });

        match cdes_only {
            true => Box::new(data.filter_map(|cd| match cd.variant {
                ClinicalDatumVariant::History => None,
                ClinicalDatumVariant::CDEs => Some(cd)
            })),
            false => Box::new(data)
        }
    }
}

impl<'a> Iterator for RegistryData<'a> {
    type Item = PatientSlice;

    fn next(&mut self) -> Option<Self::Item> {
        return match self.iterator.next() {
            None => None,
            Some(first_cd) => {
                let mut slice = PatientSlice::from(first_cd.patient);
                slice.add(first_cd);

                loop {
                    match self.iterator.peek() {
                        None => break,
                        Some(cd) => {
                            match slice.can_add(&cd) {
                                true => slice.add(self.iterator.next().unwrap()),
                                false => break,
                            };
                        }
                    }
                }

                Some(slice)
            }
        };
    }
}
