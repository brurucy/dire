use crate::model::types::{
    DoneSink, DoneSource, FirstStageMaterialization, LogSink, MasterSource, RuntimeLog,
    SecondStageMaterialization, Triple, TripleInputSource, TripleOutputSink,
};
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::operators::Threshold;
use differential_dataflow::trace::Trace;
use std::time::{Duration, Instant};
use timely::communication::allocator::Generic;
use timely::worker::{AsWorker, Worker};
use timely::PartialOrder;

pub fn reason(
    cfg: timely::Config,
    tbox_materialization: FirstStageMaterialization,
    abox_materialization: SecondStageMaterialization,
    tbox_input_source: TripleInputSource,
    abox_input_source: TripleInputSource,
    tbox_output_sink: TripleOutputSink,
    abox_output_sink: TripleOutputSink,
    done: DoneSink,
    terminator: MasterSource,
    logger: LogSink,
) -> () {
    timely::execute(cfg, move |worker: &mut Worker<Generic>| {
        let (mut tbox_input_session, mut tbox_trace, tbox_probe, mut expanded_lists_trace) = worker
            .dataflow_named::<usize, _, _>("tbox_materialization", |scope| {
                let tbox_output_sink = tbox_output_sink.clone();
                let (tbox_input_session, tbox_collection) = scope.new_collection::<Triple, isize>();
                let (tbox_materialization, expanded_lists) = tbox_materialization(&tbox_collection);
                (
                    tbox_input_session,
                    tbox_materialization.arrange_by_self().trace,
                    tbox_materialization
                        .distinct()
                        .inspect_batch(move |_t, xs| {
                            for ((s, p, o), time, diff) in xs {
                                tbox_output_sink.send(((*s, *p, *o), *time, *diff)).unwrap()
                            }
                        })
                        .probe(),
                    expanded_lists.arrange_by_key().trace,
                )
            });
        let (mut abox_input_session, abox_probe) =
            worker.dataflow_named::<usize, _, _>("abox_materialization", |scope| {
                let abox_output_sink = abox_output_sink.clone();
                let (abox_input_session, abox_collection) = scope.new_collection::<Triple, isize>();
                let tbox_collection = tbox_trace
                    .import(scope)
                    .as_collection(|(s, p, o), _v| (*s, *p, *o));
                let expanded_lists_collection = expanded_lists_trace
                    .import(scope)
                    .as_collection(|head, tail| (*head, tail.clone()));
                let materialization = abox_materialization(
                    &tbox_collection,
                    &expanded_lists_collection,
                    &abox_collection,
                );
                (
                    abox_input_session,
                    materialization
                        .distinct()
                        .inspect_batch(move |_t, xs| {
                            for ((s, p, o), time, diff) in xs {
                                abox_output_sink.send(((*s, *p, *o), *time, *diff)).unwrap()
                            }
                        })
                        .probe(),
                )
            });
        let mut last_run = false;
        let mut last_ts = 0;
        let mut data_ingested = 0;
        let mut current_data_ingested = 0;
        let mut data_regurgitated = 0;
        let mut current_data_regurgitated = 0;
        let mut current_latency = 0;
        let mut total_latency = 0;
        let mut round = 0;
        let mut files_loaded = 0;
        let mut iterate = true;

        loop {
            let now = Instant::now();
            if !abox_input_source.is_empty() || !tbox_input_source.is_empty() {
                let mut current_local_data_ingested = 0;
                tbox_input_source.try_iter().for_each(|triple| {
                    tbox_input_session.update(triple.0, triple.1);
                    current_local_data_ingested += 1;
                });

                tbox_input_session.advance_to(*tbox_input_session.epoch() + 1);
                last_ts += 1;
                tbox_input_session.flush();

                abox_input_source.try_iter().for_each(|triple| {
                    abox_input_session.update(triple.0, triple.1);
                    if triple.1 > 0 {
                        current_local_data_ingested += 1;
                    } else {
                        data_regurgitated += 1;
                        current_data_regurgitated += 1;
                    }
                });

                abox_input_session.advance_to(*abox_input_session.epoch() + 1);
                abox_input_session.flush();

                worker.step_or_park_while(Some(Duration::from_millis(50)), || {
                    tbox_probe.less_than(tbox_input_session.time())
                });
                worker.step_or_park_while(Some(Duration::from_millis(50)), || {
                    abox_probe.less_than(abox_input_session.time())
                });

                round += 1;
                let round_elapsed_time = now.elapsed().as_millis();
                data_ingested += current_local_data_ingested;
                current_data_ingested += current_local_data_ingested;
                current_latency += round_elapsed_time;
                total_latency += round_elapsed_time;
                iterate = false;
            }

            if abox_input_source.is_empty() && tbox_input_source.is_empty() && !last_run && !iterate
            {
                let log = RuntimeLog::new(
                    files_loaded,
                    current_latency,
                    current_data_ingested,
                    current_data_regurgitated,
                    worker.index(),
                );
                logger.send(log.to_string());
                files_loaded += 1;
                done.send(());
                if let Ok(command) = terminator.recv() {
                    match command.as_str() {
                        "STOP" => last_run = true,
                        _ => {
                            current_latency = 0;
                            current_data_ingested = 0;
                            current_data_regurgitated = 0;
                            iterate = true;
                            continue;
                        }
                    }
                }
            }

            worker.step();

            if last_run {
                abox_input_session.close();
                tbox_input_session.close();
                worker.step_while(|| tbox_probe.less_than(&(last_ts + 1)));
                worker.step_while(|| abox_probe.less_than(&(last_ts + 1)));
                break;
            }
        }

        println!(
            "Total latency and triples processed at worker {}:{} ms, {} triples",
            worker.index(),
            total_latency,
            data_ingested,
        );
    })
    .unwrap();
}
