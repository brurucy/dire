use differential_dataflow::input::InputSession;
use timely::communication::allocator::Thread;
use timely::worker::Worker;

const TBOX: [(usize, usize, usize); 25] = [
    (7, 4, 8),
    (7, 9, 10),
    (11, 4, 8),
    (11, 9, 12),
    (11, 0, 7),
    (13, 4, 8),
    (13, 9, 14),
    (13, 0, 15),
    (15, 4, 8),
    (15, 9, 16),
    (15, 0, 11),
    (17, 4, 18),
    (17, 9, 19),
    (17, 1, 20),
    (21, 4, 18),
    (21, 9, 22),
    (20, 4, 18),
    (20, 9, 23),
    (20, 1, 21),
    (20, 4, 5),
    (24, 6, 20),
    (25, 4, 18),
    (25, 9, 26),
    (25, 2, 11),
    (25, 3, 27),
];

const ABOX: [(usize, usize, usize); 6] = [
    (28, 17, 29),
    (28, 4, 13),
    (28, 25, 30),
    (28, 20, 31),
    (31, 20, 32),
    (32, 20, 33),
];

type TimelyScopeClosure = fn(worker: &mut Worker<Thread>) -> ();

fn boilerplate(tsc: TimelyScopeClosure) {
    timely::execute_directly(move |worker| tsc(worker))
}

fn inject_encoded_lubm_tbox(mut tbox_input: InputSession<(), (usize, usize, usize), isize>) {
    for tuple in ABOX {
        tbox_input.insert(tuple)
    }
}

fn inject_encoded_lubm_abox(mut abox_input: InputSession<(), (usize, usize, usize), isize>) {
    for tuple in ABOX {
        abox_input.insert(tuple)
    }
}

#[cfg(test)]
mod tests {
    use crate::entrypoint::{
        boilerplate, inject_encoded_lubm_abox, inject_encoded_lubm_tbox, TimelyScopeClosure, ABOX,
    };
    use std::collections::BTreeSet;
    use std::ptr::null;

    use differential_dataflow::input::Input;
    use differential_dataflow::operators::arrange::agent::TraceAgent;
    use differential_dataflow::operators::arrange::arrangement::{ArrangeByKey, ArrangeBySelf};
    use differential_dataflow::operators::join::JoinCore;
    use differential_dataflow::trace::cursor::CursorDebug;
    use differential_dataflow::trace::TraceReader;

    use timely::communication::allocator::Thread;
    use timely::dataflow::operators::probe::Handle;
    use timely::dataflow::operators::{Inspect, Map, ToStream, ToStreamAsync};
    use timely::dataflow::scopes::Child;
    use timely::worker::Worker;

    #[test]
    fn it_works() {
        let computation: TimelyScopeClosure = |worker| {
            let mut probe = Handle::new();

            let (mut source_one, mut source_two, mut trace_one, mut trace_two) = worker
                .dataflow_named::<(), _, _>(
                    "materialization",
                    |outer: &mut Child<Worker<Thread>, ()>| {
                        let (source_one, data_one) =
                            outer.new_collection::<(usize, usize, usize), isize>();
                        let (source_two, data_two) =
                            outer.new_collection::<(usize, usize, usize), isize>();

                        let trace_one = data_one.probe_with(&mut probe).arrange_by_self().trace;
                        let trace_two = data_two.arrange_by_self().trace;

                        (source_one, source_two, trace_one, trace_two)
                    },
                );

            let (mut source_query, mut data_query) = worker.dataflow_named::<(), _, _>(
                "query_one",
                |outer: &mut Child<Worker<Thread>, ()>| {
                    let (source_query, data_query) =
                        outer.new_collection::<(usize, usize), isize>();

                    let data_one = trace_one
                        .import(outer)
                        .as_collection(|(x, y, z), v| (*y, (*x, *z)))
                        .inspect(|x| println!("What happens? {:?}", x))
                        .arrange_by_key();

                    let data_query = data_query
                        .join_core(&data_one, |_key, k, v| Some(*(v)))
                        .probe_with(&mut probe)
                        .inspect(|x| println!("What happened {:?}", x))
                        .arrange_by_key();

                    (source_query, data_query.trace)
                },
            );

            source_one.insert(ABOX[0]);
            source_one.insert(ABOX[1]);
            source_one.insert(ABOX[2]);
            source_one.insert(ABOX[3]);

            let freezing_one = data_query.cursor().0;

            source_query.insert((20, 0));

            while probe.less_than(source_one.time()) {
                worker.step();
            }

            let freezing_two = data_query.cursor().0;

            source_one.insert(ABOX[4]);

            let freezing_three = data_query.cursor().0;

            worker.step();
        };

        boilerplate(computation)
    }
}
