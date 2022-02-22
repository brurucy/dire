use clap::{Arg, Command};
use dire_engine::entrypoint::{entrypoint, Engine};
use dire_parser::load3enc;

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
        .get_matches();

    let t_path: String = matches.value_of("TBOX_PATH").unwrap().to_string();
    let a_path: String = matches.value_of("ABOX_PATH").unwrap().to_string();
    let expressivity: String = matches.value_of("EXPRESSIVITY").unwrap().to_string();
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
        _ => Engine::Dummy,
    };

    let single_threaded = timely::Config::process(workers);
    let (
        tbox_input_sink,
        abox_input_sink,
        tbox_output_source,
        abox_output_source,
        termination_source,
    ) = entrypoint(single_threaded, batch_size, logic);

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

    termination_source.send(()).unwrap();

    println!("materialized tbox triples: {}", tbox_output_source.len());
    println!("materialized abox triples: {}", abox_output_source.len());
}