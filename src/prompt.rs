use std::io::{Write, stdin, stdout};

pub enum Response {
    All,
    Yes,
    No,
}

pub fn input() -> Response {
    let mut input = String::new();
    loop {
        print!("\x1b[1;34mContinue [(Y)es|(n)o|(a)ll]? \x1b[0m");
        stdout().flush().ok();
        stdin().read_line(&mut input).expect("Failed reading input");

        match input.to_ascii_lowercase().trim() {
            "y" | "yes" | "" => return Response::Yes,
            "n" | "no" => return Response::No,
            "a" | "all" => return Response::All,
            _ => input.clear()
        }
    }
}