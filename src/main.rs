mod clinical_data;
mod diff;
mod streaming;

use clap::{App, Arg};
use env_logger;
use indicatif::{ProgressBar, ProgressStyle, ProgressFinish};
use itertools::{Itertools, EitherOrBoth};
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read, Seek, stdin, stdout, Write};
use std::path::Path;
use std::process;
use zip::ZipArchive;
use zip::read::ZipFile;

use crate::clinical_data::{PatientSlice};
use crate::diff::Diff;
use crate::streaming::{read_array_file_to_values, map_values_to_clinical_data, RegistryData};

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
    No,
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

fn zip_diff(old_iter: impl Iterator<Item=PatientSlice>, new_iter: impl Iterator<Item=PatientSlice>) -> usize {
    let mut skip_input = false;

    old_iter.zip_longest(new_iter).map(|pair| {
        match pair {
            EitherOrBoth::Both(old, new) => {
                match old.diff(&new) {
                    None => None,
                    Some(diffs) => {
                        diffs.iter().for_each(|d| eprintln!("{:#?}", d));
                        if !skip_input {
                            match prompt_input() {
                                PromptResponse::All => skip_input = true,
                                PromptResponse::Yes => {}
                                PromptResponse::No => process::exit(0)
                            }
                        }
                        Some(diffs.len())
                    }
                }
            }
            EitherOrBoth::Left(_) => {
                panic!("New ran out of slices!")
            }
            EitherOrBoth::Right(_) => {
                panic!("Old ran out of slices!")
            }
        }
    }).flatten().sum()
}

fn diff_clinical_data(old_path: String, new_path: String, registry_code: String) -> Result<usize, Box<dyn Error>> {
    let mut old_archive = get_zip_archive(old_path.as_str())?;
    let mut new_archive = get_zip_archive(new_path.as_str())?;

    let old_reader = get_zip_reader(&mut old_archive, registry_code.as_str())?;
    let new_reader = get_zip_reader(&mut new_archive, registry_code.as_str())?;

    let pb = ProgressBar::new(old_reader.size());
    let old_reader = pb.wrap_read(old_reader);
    pb.set_style(ProgressStyle::default_bar()
        .template("Reading [{elapsed_precise} / {duration_precise} ({eta})] {wide_bar:.cyan/blue} {bytes}/{total_bytes}")
        .progress_chars("##-")
        .on_finish(ProgressFinish::AtCurrentPos)
    );

    let old_iter = read_array_file_to_values(old_reader);
    let new_iter = read_array_file_to_values(new_reader);

    let old_iter = map_values_to_clinical_data(old_iter);
    let new_iter = map_values_to_clinical_data(new_iter);

    let old_iter = RegistryData::from(Box::new(old_iter));
    let new_iter = RegistryData::from(Box::new(new_iter));

    Ok(zip_diff(old_iter, new_iter))
}


fn main() -> Result<(), Box<dyn Error>> {
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

    let total = diff_clinical_data(old_zip.into(), new_zip.into(), registry_code.into())?;
    println!("Found {} differences", total);

    Ok(())
}
