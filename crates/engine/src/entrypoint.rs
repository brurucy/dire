use crate::types::{Triple, TripleSink, UnaryMaterialization, WorkerExecutionClosure};
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use flume::Sender;
use timely::communication::allocator::Generic;
use timely::communication::Allocator;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;
use timely::worker::Worker;

pub fn reason(
    cfg: timely::Config,
    tbox_materialization: &'static UnaryMaterialization,
    tbox_sink: TripleSink,
) -> () {
    timely::execute(cfg, move |worker: &mut Worker<Generic>| {
        let (mut tbox_input_session, tbox_trace) =
            worker.dataflow_named::<usize, _, _>("tbox_materialization", |outer| {
                let (tbox_input_session, tbox_collection) =
                    outer.new_collection::<(usize, usize, usize), isize>();
                let materialization = tbox_materialization(&tbox_collection);
                materialization.inspect(|triple| {
                    tbox_sink.send(*triple).unwrap();
                });
                (tbox_input_session, materialization.arrange_by_self().trace)
            });
        if worker.index() == 0 {
            tbox_input_session.insert((28, 17, 29));
            tbox_input_session.insert((28, 4, 13));
        };
        worker.step();
    });
}

#[cfg(test)]
mod tests {
    use crate::entrypoint::reason;
    use crate::types::{TripleCollection, UnaryMaterialization, WorkerExecutionClosure};
    use differential_dataflow::input::Input;
    use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
    use differential_dataflow::operators::{JoinCore, Threshold};
    use std::borrow::Borrow;
    use timely::communication::{Allocator, Config};
    use timely::worker::Worker;

    fn redundant<'a>(tbox: &TripleCollection<'a>) -> TripleCollection<'a> {
        let tcl = tbox.clone();
        return tcl;
    }

    const REDUNDANT_WRAPPER: &'static UnaryMaterialization = &redundant;

    #[test]
    fn reason_works() {
        // These computations are meaningless.
        // The point of this test is to assert that the closure is indeed executed
        // And for that, we do some random computations and then just send the output to a channel
        // We can be sure that the closure was executed if the channel receiver has the expected computation result.
        let (ttx, trx) = flume::unbounded();
        reason(
            timely::Config {
                communication: Config::Process(2),
                worker: Default::default(),
            },
            REDUNDANT_WRAPPER,
            ttx,
        );

        let mut diffs: Vec<((usize, usize, usize), usize, isize)> = vec![];

        while let Ok(diff) = trx.try_recv() {
            diffs.push(diff)
        }

        let expected_diffs = vec![((28, 17, 29), 0, 1), ((28, 4, 13), 0, 1)];

        assert_eq!(expected_diffs, diffs)
    }
}
