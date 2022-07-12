use clap::{Arg, Command};
use dire_engine::entrypoint::{entrypoint, Engine};
use dire_engine::model::types::Triple;
use dire_parser::load3enc;
use serde::Deserialize;
use std::fs::File;
use std::io::Write;
use std::io::{BufWriter, Read};
use std::mem::transmute;
use std::path::Path;
use std::process::id;
use std::string::String;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use timely::CommunicationConfig::{Cluster, Process};
use timely::{Config, WorkerConfig};

#[derive(Deserialize)]
struct Cfg {
    index: usize,
    hosts: Vec<String>,
}

fn parse_hosts_file(filename: &str) -> Cfg {
    let mut file = File::open(filename).unwrap();
    let mut file_contents = String::new();
    file.read_to_string(&mut file_contents).unwrap();
    let config: Cfg = toml::from_str(&file_contents).unwrap();
    return config;
}

fn main() {
    let matches = Command::new("differential-reasoner")
        .version("1.3.1")
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
            Arg::new("HOSTFILE")
                .help("Sets the hostfile")
                .required(false)
                .index(6),
        )
        .get_matches();

    let t_path: String = matches.value_of("TBOX_PATH").unwrap().to_string();
    let a_path: String = matches.value_of("ABOX_PATH").unwrap().to_string();
    let expressivity: String = matches.value_of("EXPRESSIVITY").unwrap().to_string();
    let workers: usize = matches
        .value_of("WORKERS")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let batch_size: f64 = matches
        .value_of("BATCH_SIZE")
        .unwrap()
        .parse::<f64>()
        .unwrap();
    let logic = match expressivity.as_str() {
        "rdfspp" => Engine::RDFSpp,
        "rdfs" => Engine::RDFS,
        "owl2rl" => Engine::OWL2RL,
        _ => Engine::Dummy,
    };
    let distributed: bool = matches.is_present("HOSTFILE");
    let mut cfg: Config = Config {
        communication: Process(workers),
        worker: WorkerConfig::default(),
    };
    if distributed {
        let hostfile = matches.value_of("HOSTFILE").unwrap().to_string();
        let parsed_hostfile = parse_hosts_file(&hostfile);
        cfg = Config {
            worker: WorkerConfig::default(),
            communication: Cluster {
                threads: workers,
                process: parsed_hostfile.index,
                addresses: parsed_hostfile.hosts,
                report: true,
                log_fn: Box::new(|_| None),
            },
        };
    }

    let abox_iter = load3enc(&a_path);
    let abox_vec: Vec<Triple> = abox_iter.unwrap().collect();
    let cutoff: usize = (abox_vec.len() as f64 * batch_size) as usize;

    let mut batch_size: usize = 0;
    if cutoff == 0 {
        batch_size = abox_vec.len();
    } else {
        batch_size = cutoff
    }

    let (
        tbox_input_sink,
        abox_input_sink,
        tbox_output_source,
        abox_output_source,
        done_source,
        terminator_sink,
        logs,
        joinhandle,
    ) = entrypoint(cfg, batch_size, logic);

    let tbox_iter = load3enc(&t_path);
    if let Ok(parsed_nt) = tbox_iter {
        parsed_nt.for_each(|triple| {
            tbox_input_sink.send((triple, 1)).unwrap();
        })
    }

    if cutoff != 0 && cutoff != abox_vec.len() {
        let addition_vec: &[Triple];
        let deletion_vec: &[Triple];
        addition_vec = &abox_vec[cutoff..];
        println!("Addition update size: {}", addition_vec.len());
        deletion_vec = &abox_vec[cutoff..];
        println!("Deletion update size: {}", deletion_vec.len());

        for (idx, triple) in abox_vec.iter().enumerate() {
            if idx == cutoff {
                break;
            }
            abox_input_sink.send((*triple, 1)).unwrap();
        }

        for _ in 0..workers {
            done_source.recv();
            terminator_sink.send("CONTINUE".to_string());
        }

        addition_vec.iter().for_each(|triple| {
            abox_input_sink.send((*triple, 1)).unwrap();
        });

        for _ in 0..workers {
            done_source.recv();
            terminator_sink.send("CONTINUE".to_string());
        }

        deletion_vec.iter().for_each(|triple| {
            abox_input_sink.send((*triple, -1)).unwrap();
        })
    } else {
        abox_vec.iter().for_each(|triple| {
            abox_input_sink.send((*triple, 1)).unwrap();
        });
    }

    for _ in 0..workers {
        done_source.recv();
        terminator_sink.send("STOP".to_string());
    }

    joinhandle.join().unwrap();

    println!("materialized tbox triples: {}", tbox_output_source.len());
    println!("materialized abox triples: {}", abox_output_source.len());

    let a_filename = match Path::new(&a_path).file_stem() {
        Some(file_name) => file_name,
        None => panic!(),
    };

    let log_file = File::create(format!(
        "{}_{}_{}_{}_{}.csv",
        a_filename.to_str().unwrap(),
        expressivity.as_str(),
        batch_size,
        workers,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    ))
    .unwrap();
    let mut log_writer = BufWriter::new(log_file);
    writeln!(&mut log_writer, "{}", "file,latency,added,removed,worker");
    while let Ok(log) = logs.try_recv() {
        writeln!(&mut log_writer, "{}", log);
    }
}
