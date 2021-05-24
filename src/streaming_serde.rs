use std::io::{BufReader, Read, BufRead};
use serde_json::{Value, from_str};

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
pub fn read_array_to_iter<'a>(reader: impl Read + 'a) -> impl Iterator<Item=Value> + 'a {
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
    }).filter_map(|v| v).into_iter()
}