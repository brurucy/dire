use std::fs::File;
use clap::{Arg, Command};
use dire_engine::entrypoint::{entrypoint, Engine};
use dire_parser::load3enc;
use std::thread;
use std::time::Duration;

fn main() {
    let matches = Command::new("differential-reasoner")
        .version("0.2.0")
        .about("Reasons in a differential manner 😎")
        .arg(
            Arg::new("TBOX_PATH")
                .help("Sets the tbox file path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("ABOX_PATH")
                .help("Sets the abox file path")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("EXPRESSIVITY")
                .help("Sets the expressivity")
                .required(true)
                .index(3),
        )
        .arg(
            Arg::new("WORKERS")
                .help("Sets the amount of workers")
                .required(true)
                .index(4),
        )
        .arg(
            Arg::new("BATCH_SIZE")
                .help("Sets the batch size")
                .required(true)
                .index(5),
        )
        .arg(
            Arg::new("UPDATE_PATH")
                .help("Take batch size as update")
                .required(false)
                .index(6),
        )
        .get_matches();

    let t_path: String = matches.value_of("TBOX_PATH").unwrap().to_string();
    let a_path: String = matches.value_of("ABOX_PATH").unwrap().to_string();
    let expressivity: String = matches.value_of("EXPRESSIVITY").unwrap().to_string();
    let update: bool = matches.is_present("UPDATE_PATH");
    let update_file_path: String = matches.value_of("UPDATE_PATH").unwrap().to_string();

    let workers: usize = matches
        .value_of("WORKERS")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let batch_size: usize = matches
        .value_of("BATCH_SIZE")
        .unwrap()
        .parse::<usize>()
        .unwrap();

    let logic = match expressivity.as_str() {
        "rdfspp" => Engine::RDFSpp,
        "rdfs" => Engine::RDFS,
        "owl2rl" => Engine::OWL2RL,
        _ => Engine::Dummy,
    };

    let process = timely::Config::process(workers);
    let (
        tbox_input_sink,
        abox_input_sink,
        tbox_output_source,
        abox_output_source,
        termination_source,
        joinhandle,
    ) = entrypoint(process, batch_size, logic);

    let tbox_iter = load3enc(&t_path);
    if let Ok(parsed_nt) = tbox_iter {
        parsed_nt.for_each(|triple| {
            tbox_input_sink.send((triple, 1)).unwrap();
        })
    }

    let abox_iter = load3enc(&a_path);
    if let Ok(parsed_nt) = abox_iter {
        parsed_nt.for_each(|triple| {
            abox_input_sink.send((triple, 1)).unwrap();
        })
    }

    if update {
        let abox_iter = load3enc(&update_file_path);
        if let Ok(parsed_nt) = abox_iter {
            parsed_nt.for_each(|triple| {
                abox_input_sink.send((triple, -1)).unwrap();
            })
        }
    }

    termination_source.send(()).unwrap();

    joinhandle.join().unwrap();

    println!("materialized tbox triples: {}", tbox_output_source.len());
    println!("materialized abox triples: {}", abox_output_source.len());
}
