use serde_json::{Value, from_str, to_string_pretty};
use std::io::{BufReader, Read, BufRead};
use std::iter::Peekable;

use crate::clinical_data::{PatientSlice, ClinicalDatum};

pub struct MigratedRegistry<'a> {
    iterator: Box<Peekable<Box<dyn Iterator<Item=ClinicalDatum> + 'a>>>,
}

impl<'a> MigratedRegistry<'a> {
    pub fn from(reader: impl Read + 'a) -> MigratedRegistry<'a> {
        let values = Self::read_array_file_to_values(reader);
        let clinical_data: Box<dyn Iterator<Item=ClinicalDatum>> = Box::new(Self::map_values_to_clinical_data(values));
        let iterator = Box::new(clinical_data.peekable());

        MigratedRegistry { iterator }
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

    pub fn map_values_to_clinical_data(values: impl Iterator<Item=Value>) -> impl Iterator<Item=ClinicalDatum> {
        values.filter_map(|value| match ClinicalDatum::from(&value) {
            Ok(cd) => cd,
            Err(e) => {
                log::error!("Error parsing clinical datum: {:#?}", e);
                log::debug!("Original value: {}", to_string_pretty(&value).unwrap());
                panic!()
            }
        })
    }
}

impl<'a> Iterator for MigratedRegistry<'a> {
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
                                true => { slice.add(self.iterator.next().unwrap()) }
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
