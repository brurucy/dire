use std::fs::File;
use std::io::{BufRead, BufReader};

fn read_file(filename: &str) -> Result<impl Iterator<Item = String>, &'static str> {
    return if let Ok(file) = File::open(filename) {
        if let buffer = BufReader::new(file) {
            Ok(buffer.lines().filter_map(|line| line.ok()))
        } else {
            Err("fail to make buffer")
        }
    } else {
        Err("fail to open file")
    };
}

pub fn load3enc<'a>(
    filename: &str,
) -> Result<impl Iterator<Item = (u32, u32, u32)> + 'a, &'static str> {
    match read_file(filename) {
        Ok(file) => {
            let option_map = file.map(move |line| {
                let mut split_line = line.split(' ');
                let digit_one: u32 = split_line.next().unwrap().parse().unwrap();
                let digit_two: u32 = split_line.next().unwrap().parse().unwrap();
                let digit_three: u32 = split_line.next().unwrap().parse().unwrap();
                (digit_one, digit_two, digit_three)
            });
            Ok(option_map)
        }
        Err(msg) => Err(msg),
    }
}