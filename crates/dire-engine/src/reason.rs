use crate::model::types::{
    BinaryMaterialization, Terminator, Triple, TripleInputSource, TripleOutputSink,
    UnaryMaterialization,
};
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::trace::{Trace, TraceReader};
use std::time::Duration;
use timely::communication::allocator::Generic;
use timely::worker::{AsWorker, Worker};
use timely::PartialOrder;

pub fn reason(
    cfg: timely::Config,
    tbox_materialization: UnaryMaterialization,
    abox_materialization: BinaryMaterialization,
    tbox_input_source: TripleInputSource,
    abox_input_source: TripleInputSource,
    tbox_output_sink: TripleOutputSink,
    abox_output_sink: TripleOutputSink,
    terminator: Terminator,
) -> () {
    timely::execute(cfg, move |worker: &mut Worker<Generic>| {
        let (mut tbox_input_session, mut tbox_trace, tbox_probe) = worker
            .dataflow_named::<usize, _, _>("tbox_materialization", |mut scope| {
                let tbox_output_sink = tbox_output_sink.clone();
                let (tbox_input_session, tbox_collection) = scope.new_collection::<Triple, isize>();
                let materialization = tbox_materialization(&tbox_collection);
                (
                    tbox_input_session,
                    materialization.arrange_by_self().trace,
                    materialization
                        .inspect_batch(move |t, xs| {
                            for ((s, p, o), time, diff) in xs {
                                tbox_output_sink.send(((*s, *p, *o), *time, *diff)).unwrap()
                            }
                        })
                        .probe(),
                )
            });
        let (mut abox_input_session, abox_probe) =
            worker.dataflow_named::<usize, _, _>("abox_materialization", |scope| {
                let abox_output_sink = abox_output_sink.clone();
                let (abox_input_session, abox_collection) = scope.new_collection::<Triple, isize>();
                let tbox = tbox_trace
                    .import(scope)
                    .as_collection(|(s, p, o), _v| (*s, *p, *o));
                let materialization = abox_materialization(&tbox, &abox_collection);
                (
                    abox_input_session,
                    materialization
                        .inspect_batch(move |t, xs| {
                            for ((s, p, o), time, diff) in xs {
                                abox_output_sink.send(((*s, *p, *o), *time, *diff)).unwrap()
                            }
                        })
                        .probe(),
                )
            });
        let mut last_run = false;
        let mut last_ts = 0;
        if worker.index() == 0 {
            loop {
                if abox_input_source.is_full() | tbox_input_source.is_full() | last_run {
                    tbox_input_source
                        .drain()
                        .for_each(|triple| tbox_input_session.insert(triple.0));

                    tbox_input_session.advance_to(*tbox_input_session.epoch() + 1);
                    last_ts += 1;
                    tbox_input_session.flush();

                    abox_input_source
                        .drain()
                        .for_each(|triple| abox_input_session.insert(triple.0));

                    abox_input_session.advance_to(*abox_input_session.epoch() + 1);
                    abox_input_session.flush();
                }
                worker.step_or_park_while(Some(Duration::from_millis(5)), || {
                    tbox_probe.less_than(tbox_input_session.time())
                });
                worker.step_or_park_while(Some(Duration::from_millis(5)), || {
                    abox_probe.less_than(abox_input_session.time())
                });

                if last_run {
                    abox_input_session.close();
                    tbox_input_session.close();
                    worker.step_while(|| tbox_probe.less_than(&(last_ts + 1)));
                    worker.step_while(|| abox_probe.less_than(&(last_ts + 1)));
                    break;
                }

                if let Ok(_) = terminator.try_recv() {
                    last_run = true
                }
            }
        }
    })
    .unwrap();
}

#[cfg(test)]
mod tests {
    use crate::materialization::common::{
        dummy_binary_materialization, dummy_unary_materialization,
    };
    use crate::model::types::{RegularScope, TripleCollection};
    use crate::reason::reason;
    use timely::communication::Config;

    #[test]
    fn reason_works() {
        let (tbox_output_sink, tbox_output_source) = flume::unbounded();
        let (tbox_input_sink, tbox_input_source) = flume::bounded(2);
        let (abox_output_sink, abox_output_source) = flume::unbounded();
        let (abox_input_sink, abox_input_source) = flume::bounded(2);
        let (termination_sink, termination_source) = flume::bounded(1);
        tbox_input_sink.send(((28, 17, 29), 1)).unwrap();
        tbox_input_sink.send(((28, 4, 13), 1)).unwrap();
        abox_input_sink.send(((30, 28, 1), 1)).unwrap();
        abox_input_sink.send(((30, 29, 1), 1)).unwrap();
        termination_sink.send(()).unwrap();
        reason(
            timely::Config {
                communication: Config::Process(2),
                worker: Default::default(),
            },
            dummy_unary_materialization,
            dummy_binary_materialization,
            tbox_input_source,
            abox_input_source,
            tbox_output_sink,
            abox_output_sink,
            termination_source,
        );

        let mut actual_tbox_diffs: Vec<(u32, u32, u32)> = vec![];
        let mut actual_abox_diffs = actual_tbox_diffs.clone();

        while let Ok(diff) = tbox_output_source.try_recv() {
            actual_tbox_diffs.push(diff.0)
        }

        while let Ok(diff) = abox_output_source.try_recv() {
            actual_abox_diffs.push(diff.0)
        }

        let expected_tbox_diffs = vec![(28, 17, 29), (28, 4, 13)];
        let expected_abox_diffs = vec![(30, 28, 1), (30, 29, 1)];

        assert_eq!(expected_tbox_diffs, actual_tbox_diffs);
        assert_eq!(expected_abox_diffs, actual_abox_diffs);
    }
}
