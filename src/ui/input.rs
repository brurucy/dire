use std::fs::File;
use std::io::{BufRead, BufReader};
use std::string::ParseError;

#[derive(Clone)]
pub enum InputMode {
    Normal,
    TBOX,
    ABOX,
}
