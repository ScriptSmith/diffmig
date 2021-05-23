mod clinical_data;
mod diff;
mod streaming_serde;

use clap::{App, Arg};
use env_logger;
use indicatif::{ProgressBar, ProgressStyle, ProgressFinish};
use serde_json::Value;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read, Seek, stdin, stdout, Write};
use std::path::Path;
use std::process;
use zip::ZipArchive;
use zip::read::ZipFile;

use crate::clinical_data::{ClinicalDatum};
use crate::diff::Diff;
use crate::streaming_serde::read_array_to_iter;

fn get_zip_archive<'a>(zip_path: &'a str) -> Result<ZipArchive<impl Read + Seek>, Box<dyn Error>> {
    let file = File::open(Path::new(zip_path))?;
    Ok(ZipArchive::new(BufReader::new(file))?)
}

fn get_zip_reader<'a>(archive: &'a mut ZipArchive<impl Read + Seek>, registry_code: &'a str) -> Result<ZipFile<'a>, Box<dyn Error>> {
    let clinical_data_path = format!("{}/registry_data/clinical_data/rdrf_clinicaldata.json", registry_code);
    Ok(archive.by_name(clinical_data_path.as_str())?)
}

enum PromptResponse {
    All,
    Yes,
    No
}

fn prompt_input() -> PromptResponse {
    let mut input = String::new();
    loop {
        print!("\x1b[1;34mContinue [(Y)es|(n)o|(a)ll]? \x1b[0m");
        stdout().flush().ok();
        stdin().read_line(&mut input).expect("Failed reading input");

        match input.to_ascii_lowercase().trim() {
            "y" | "yes" | "" => return PromptResponse::Yes,
            "n" | "no" => return PromptResponse::No,
            "a" | "all" => return PromptResponse::All,
            _ => input.clear()
        }

    }
}

fn zip_diff(old_iter: impl Iterator<Item=Value>, new_iter: impl Iterator<Item=Value>) -> Result<usize, Box<dyn Error>> {
    let mut skip_input = false;

    let counts = old_iter.zip(new_iter).map(|(v1, v2)| {
        let old_data = ClinicalDatum::from(&v1)?;
        let new_data = ClinicalDatum::from(&v2)?;

        match (old_data, new_data) {
            (Some(old), Some(new)) => {
                match old.diff(&new) {
                    None => Ok(None),
                    Some(diffs) => {
                        diffs.iter().for_each(|d| {
                            eprintln!("{:#?}", d);
                        });

                        if !skip_input {
                            match prompt_input() {
                                PromptResponse::All => skip_input = true,
                                PromptResponse::Yes => {}
                                PromptResponse::No => process::exit(0)
                            }
                        }

                        Ok(Some(diffs.len()))
                    }
                }
            }
            (None, None) => Ok(None),
            (None, Some(_)) => {
                Err("Old entry skipped but new entry wasn't".into())
            }
            (Some(_), None) => {
                Err("New entry skipped but old entry wasn't".into())
            }
        }
    }).collect::<Result<Vec<Option<usize>>, Box<dyn Error>>>()?;

    Ok(counts.into_iter().filter_map(|v| v).sum())
}

fn diff_clinical_data(old_path: &str, new_path: &str, registry_code: &str) -> Result<usize, Box<dyn Error>> {
    let mut old_archive = get_zip_archive(old_path)?;
    let mut new_archive = get_zip_archive(new_path)?;

    let old_reader = get_zip_reader(&mut old_archive, registry_code)?;
    let new_reader = get_zip_reader(&mut new_archive, registry_code)?;

    let pb = ProgressBar::new(old_reader.size());
    let old_reader = pb.wrap_read(old_reader);
    pb.set_style(ProgressStyle::default_bar()
        .template("Reading [{elapsed_precise} / {duration_precise} ({eta})] {wide_bar:.cyan/blue} {bytes}/{total_bytes}")
        .progress_chars("##-")
        .on_finish(ProgressFinish::AtCurrentPos)
    );

    let old_iter = read_array_to_iter(old_reader);
    let new_iter = read_array_to_iter(new_reader);

    Ok(zip_diff(old_iter, new_iter)?)
}


fn main() -> Result<(), Box<dyn Error>>{
    env_logger::init();

    let args = App::new("diffmig")
        .version("0.1.0")
        .about("Find differences between two registry migrations of the same data")
        .arg(Arg::with_name("old_zip")
            .help("The path of the old zip file")
            .required(true)
        )
        .arg(Arg::with_name("new_zip")
            .help("The path of the new zip file")
            .required(true)
        )
        .arg(Arg::with_name("registry_code")
            .help("The registry code")
            .required(true)
        )
        .get_matches();

    let registry_code = args.value_of("registry_code").unwrap();
    let old_zip = args.value_of("old_zip").unwrap();
    let new_zip = args.value_of("new_zip").unwrap();

    let total = diff_clinical_data(old_zip, new_zip, registry_code)?;
    println!("Found {} differences", total);

    Ok(())
}
