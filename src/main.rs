mod clinical_data;
mod diff;
mod prompt;
mod migrated_registry;

use clap::{App, Arg};
use indicatif::{ProgressBar, ProgressStyle, ProgressFinish};
use itertools::{Itertools, EitherOrBoth};
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::path::Path;
use std::process;
use zip::ZipArchive;
use zip::read::ZipFile;

use crate::clinical_data::{PatientSlice};
use crate::diff::Diff;

fn get_zip_archive(zip_path: &str) -> Result<ZipArchive<impl Read + Seek>, Box<dyn Error>> {
    let file = File::open(Path::new(zip_path))?;

    Ok(ZipArchive::new(BufReader::new(file))?)
}

fn get_zip_reader<'a>(archive: &'a mut ZipArchive<impl Read + Seek>) -> Result<(String, ZipFile<'a>), Box<dyn Error>> {
    let clinical_data_path = archive.file_names().find(|p| {
        let path_split = p.split("/").collect::<Vec<&str>>();
        match &path_split[..] {
            [_, "registry_data", "clinical_data", "rdrf_clinicaldata.json"] => true,
            _ => false,
        }
    }).ok_or("rdrf_clinicaldata.json file not found in zip")?.to_string();

    Ok((clinical_data_path.clone(), archive.by_name(clinical_data_path.as_str())?))
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
                            match prompt::input() {
                                prompt::Response::All => skip_input = true,
                                prompt::Response::Yes => {}
                                prompt::Response::No => process::exit(0)
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

fn diff_clinical_data(old_path: String, new_path: String, cdes_only: bool) -> Result<usize, Box<dyn Error>> {
    let mut old_archive = get_zip_archive(old_path.as_str())?;
    let mut new_archive = get_zip_archive(new_path.as_str())?;

    let (old_path, old_reader) = get_zip_reader(&mut old_archive)?;
    let (new_path, new_reader) = get_zip_reader(&mut new_archive)?;

    if old_path != new_path {
        log::error!("Registry clinical data paths don't match");
        log::debug!("Old path: {}", old_path);
        log::debug!("New path: {}", new_path);
        panic!()
    }

    let pb = ProgressBar::new(old_reader.size());
    let old_reader = pb.wrap_read(old_reader);
    pb.set_style(ProgressStyle::default_bar()
        .template("Reading [{elapsed_precise} / {duration_precise} ({eta})] {wide_bar:.cyan/blue} {bytes}/{total_bytes}")
        .progress_chars("##-")
        .on_finish(ProgressFinish::AtCurrentPos)
    );

    let old_iter = migrated_registry::MigratedRegistry::from(old_reader, cdes_only);
    let new_iter = migrated_registry::MigratedRegistry::from(new_reader, cdes_only);

    Ok(zip_diff(old_iter, new_iter))
}


fn main() -> Result<(), Box<dyn Error>> {
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
        .arg(Arg::with_name("cdes_only")
            .help("Only compare 'cdes' clinical datum variants")
            .long("cdes")
            .takes_value(false)
            .required(false)
        )
        .arg(Arg::with_name("debug")
            .help("Print debug output")
            .long("debug")
            .takes_value(false)
            .required(false)
        )
        .get_matches();

    let old_zip = args.value_of("old_zip").unwrap();
    let new_zip = args.value_of("new_zip").unwrap();
    let cdes_only = args.is_present("cdes_only");

    env_logger::builder()
        .filter_level(match args.is_present("debug") {
            true => log::LevelFilter::Debug,
            false => log::LevelFilter::Error
        })
        .init();

    let total = diff_clinical_data(old_zip.into(), new_zip.into(), cdes_only)?;
    println!("Found {} differences", total);

    Ok(())
}
