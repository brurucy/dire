use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;

use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::{iterate, Consolidate, JoinCore, Threshold};

use timely::dataflow::Scope;
use timely::order::Product;

use crate::model::consts::constants::owl::{
    allValuesFrom, equivalentClass, equivalentProperty, hasValue, intersectionOf, inverseOf,
    onProperty, oneOf, propertyChainAxiom, sameAs, someValuesFrom, unionOf, Class,
    FunctionalProperty, InverseFunctionalProperty, Nothing, ObjectProperty, SymmetricProperty,
    Thing, TransitiveProperty,
};
use crate::model::consts::constants::rdfs::{
    domain, first, nil, r#type, range, rest, subClassOf, subPropertyOf,
};
use crate::model::types::{ListCollection, Triple, TripleCollection};

pub fn expand_lists<'a>(tbox: &TripleCollection<'a>) -> ListCollection<'a> {
    // "First" indicates the content of the rule
    let first_assertions = tbox
        .filter(|(_s, p, _o)| *p == first)
        .map(|(s, _p, o)| (s, o));
    // "Rest" is either another list, or nil.
    let rest_assertions = tbox
        .filter(|(_s, p, _o)| *p == rest)
        .map(|(s, _p, o)| (s, o));
    let rest_assertions_arr = rest_assertions.arrange_by_key();
    let first_rest_assertions = first_assertions
        .join_core(&rest_assertions_arr, |&head, &content, &tail| {
            Some((head, (content, tail)))
        });
    let first_rest_assertions_by_tail = first_rest_assertions
        .map(|(head, (content, tail))| (tail, (content, head)))
        .arrange_by_key();
    // All list heads that are in the tail of some other list must be discarded
    let first_rest_core_assertions = first_rest_assertions
        .join_core(
            &first_rest_assertions_by_tail,
            |&head, &(head_content, tail), &(_head_content, _some_tail)| {
                Some((head, (head_content, tail)))
            },
        )
        .negate()
        .concat(&first_rest_assertions)
        .consolidate()
        .map(|(head, (content, tail))| (head, content, tail));

    let first_rest_assertions_arr = first_rest_assertions.arrange_by_key();

    let core_lists =
        first_rest_core_assertions.map(|(head, content, tail)| (tail, vec![(head, content)]));

    let mut outer = tbox.scope();
    outer
        .iterative::<usize, _, _>(|inner| {
            let lists_var =
                Variable::new_from(core_lists.enter(inner), Product::new(Default::default(), 1));

            let lists_new = lists_var.consolidate();

            let first_rest_assertions_arr = first_rest_assertions_arr.enter(inner);

            let searching_for_rest = lists_new.join_core(
                &first_rest_assertions_arr,
                |&tail, core_list, &(tail_content, tail_tail)| {
                    Some((
                        tail_tail,
                        [core_list.clone(), vec![(tail, tail_content)]].concat(),
                    ))
                },
            );

            lists_var.set(&core_lists.enter(&inner).concat(&searching_for_rest));

            lists_new.leave()
        })
        // We discard all not-head lists.
        .filter(|(last_tail, _body)| *last_tail == nil)
        // Then re-key the lists by the head, and ditch all other tail references save for their content
        .map(|(_nil, list)| {
            let head: u32 = (list.first().unwrap()).0;
            let contents: Vec<u32> = list.iter().map(|(_tail, contents)| *contents).collect();
            (head, contents)
        })
}

#[cfg(test)]
mod tests {
    use crate::materialization::common::dummy_second_stage_materialization;
    use crate::materialization::owl2rl::expand_lists;
    use crate::model::consts::constants::owl::{intersectionOf, unionOf};
    use crate::model::consts::constants::rdfs::{first, nil, rest, subClassOf};
    use crate::model::consts::constants::MAX_CONST;
    use crate::model::types::{Triple, TripleCollection};
    use crate::reason::reason;
    use differential_dataflow::operators::arrange::ArrangeByKey;
    use differential_dataflow::operators::JoinCore;
    use timely::communication::Config;

    #[test]
    fn list_expansion_works() {
        let (tbox_output_sink, tbox_output_source) = flume::unbounded();
        let (tbox_input_sink, tbox_input_source) = flume::bounded(14);
        let (abox_output_sink, abox_output_source) = flume::unbounded();
        let (abox_input_sink, abox_input_source) = flume::bounded(1);
        let (done_sink, done_source) = flume::bounded(0);
        let (terminate_sink, terminate_source) = flume::bounded(0);
        let (log_sink, log_source) = flume::unbounded();
        let some_class_1 = MAX_CONST + 1;
        let some_class_2 = MAX_CONST + 2;
        let some_class_3 = MAX_CONST + 3;
        let some_class_4 = MAX_CONST + 4;
        let some_class_5 = MAX_CONST + 5;
        let some_class_6 = MAX_CONST + 6;
        let list_1 = MAX_CONST + 7;
        let list_2 = MAX_CONST + 8;
        let list_3 = MAX_CONST + 9;
        let list_4 = MAX_CONST + 10;
        let list_5 = MAX_CONST + 11;
        let list_6 = MAX_CONST + 12;
        let iof_test = MAX_CONST + 13; // 58
        let uof_test = MAX_CONST + 14; // 59
                                       // Chain 1
        tbox_input_sink
            .send(((list_1, first, some_class_1), 1))
            .unwrap();
        tbox_input_sink.send(((list_1, rest, list_2), 1)).unwrap();
        tbox_input_sink
            .send(((list_2, first, some_class_2), 1))
            .unwrap();
        tbox_input_sink.send(((list_2, rest, list_3), 1)).unwrap();
        tbox_input_sink
            .send(((list_3, first, some_class_3), 1))
            .unwrap();
        tbox_input_sink.send(((list_3, rest, nil), 1)).unwrap();
        // Chain 2
        tbox_input_sink
            .send(((list_4, first, some_class_4), 1))
            .unwrap();
        tbox_input_sink.send(((list_4, rest, list_5), 1)).unwrap();
        tbox_input_sink
            .send(((list_5, first, some_class_5), 1))
            .unwrap();
        tbox_input_sink.send(((list_5, rest, list_6), 1)).unwrap();
        tbox_input_sink
            .send(((list_6, first, some_class_6), 1))
            .unwrap();
        tbox_input_sink.send(((list_6, rest, nil), 1)).unwrap();
        //
        tbox_input_sink
            .send(((iof_test, intersectionOf, list_1), 1))
            .unwrap();
        tbox_input_sink
            .send(((uof_test, unionOf, list_4), 1))
            .unwrap();

        terminate_sink.send("STOP".to_string());

        let join_handle = thread::spawn(move || {
            reason(
                timely::Config {
                    communication: Config::Process(1),
                    worker: Default::default(),
                },
                |tbox: &TripleCollection| {
                    let lists = expand_lists(&tbox);

                    let iof_assertions = tbox
                        .filter(|(s, p, o)| *p == intersectionOf)
                        .map(|(s, p, o)| (s, o));
                    let iof_assertions_by_o = iof_assertions.map(|(s, o)| (o, s));

                    let uof_assertions = tbox
                        .filter(|(s, p, o)| *p == unionOf)
                        .map(|(s, p, o)| (s, o));
                    let uof_assertions_by_o = uof_assertions.map(|(s, o)| (o, s));

                    let expanded_lists_arr = lists.arrange_by_key();

                    // scm-int
                    let scm_int = iof_assertions_by_o
                        .join_core(&expanded_lists_arr, |&x, &c, list| Some((c, list.clone())))
                        .flat_map(|(c, list)| {
                            list.iter()
                                .map(|c_x| (c, subClassOf, *c_x))
                                .collect::<Vec<Triple>>()
                        });
                    // scm-uni
                    let scm_uni = uof_assertions_by_o
                        .join_core(&expanded_lists_arr, |&x, &c, list| Some((c, list.clone())))
                        .flat_map(|(c, list)| {
                            list.iter()
                                .map(|c_x| (*c_x, subClassOf, c))
                                .collect::<Vec<Triple>>()
                        });

                    (scm_int.concat(&scm_uni), lists.clone())
                },
                dummy_second_stage_materialization,
                tbox_input_source,
                abox_input_source,
                tbox_output_sink,
                abox_output_sink,
                done_sink,
                terminate_source,
                log_sink,
            );
        });

        join_handle.join().unwrap();

        let mut actual_tbox_diffs: Vec<(u32, u32, u32)> = vec![];
        let mut actual_abox_diffs = actual_tbox_diffs.clone();

        while let Ok(diff) = tbox_output_source.try_recv() {
            actual_tbox_diffs.push(diff.0)
        }

        let expected_tbox_diffs = vec![
            (iof_test, subClassOf, some_class_1),
            (iof_test, subClassOf, some_class_2),
            (iof_test, subClassOf, some_class_3),
            (some_class_4, subClassOf, uof_test),
            (some_class_5, subClassOf, uof_test),
            (some_class_6, subClassOf, uof_test),
        ];

        assert_eq!(expected_tbox_diffs, actual_tbox_diffs);
    }
}

pub fn owl2rl_tbox<'a>(tbox: &TripleCollection<'a>) -> (TripleCollection<'a>, ListCollection<'a>) {
    let mut outer = tbox.scope();

    let lists = expand_lists(&tbox);

    let iof_assertions = tbox
        .filter(|(_s, p, _o)| *p == intersectionOf)
        .map(|(s, _p, o)| (s, o));
    let iof_assertions_by_o = iof_assertions.map(|(s, o)| (o, s));

    let uof_assertions = tbox
        .filter(|(_s, p, _o)| *p == unionOf)
        .map(|(s, _p, o)| (s, o));
    let uof_assertions_by_o = uof_assertions.map(|(s, o)| (o, s));

    let expanded_lists_arr = lists.arrange_by_key();

    // scm-int
    let scm_int = iof_assertions_by_o
        .join_core(&expanded_lists_arr, |&_x, &c, list| Some((c, list.clone())))
        .flat_map(|(c, list)| {
            list.iter()
                .map(|c_x| (c, subClassOf, *c_x))
                .collect::<Vec<Triple>>()
        });
    // scm-uni
    let scm_uni = uof_assertions_by_o
        .join_core(&expanded_lists_arr, |&_x, &c, list| Some((c, list.clone())))
        .flat_map(|(c, list)| {
            list.iter()
                .map(|c_x| (*c_x, subClassOf, c))
                .collect::<Vec<Triple>>()
        });

    let tbox = tbox.concatenate(vec![scm_int, scm_uni]);

    let materialization = outer
        .iterative::<usize, _, _>(|inner| {
            let tbox_var = iterate::Variable::new(inner, Product::new(Default::default(), 1));

            let tbox_new = tbox_var.distinct();

            let cls_assertions = tbox_new
                .filter(|(_s, _p, o)| *o == Class)
                .map(|(s, _p, _o)| (s, s));

            let sco_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == subClassOf)
                .map(|(s, _p, o)| (s, o));
            let sco_assertions_arr = sco_assertions.arrange_by_key();
            let sco_assertions_by_o = sco_assertions.map(|(s, o)| (o, s));
            let sco_assertions_by_so = sco_assertions.map(|(s, o)| ((s, o), s)).arrange_by_key();
            let sco_assertions_by_os = sco_assertions_by_o.map(|(o, s)| ((o, s), s));

            let spo_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == subPropertyOf)
                .map(|(s, _p, o)| (s, o));
            let spo_assertions_arr = spo_assertions.arrange_by_key();
            let spo_assertions_by_o = spo_assertions.map(|(s, o)| (o, s));
            let spo_assertions_by_o_arr = spo_assertions_by_o.arrange_by_key();
            let spo_assertions_by_so = spo_assertions.map(|(s, o)| ((s, o), s)).arrange_by_key();
            let spo_assertions_by_os = spo_assertions.map(|(s, o)| ((o, s), s));

            let eqc_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == equivalentClass)
                .map(|(s, _p, o)| (s, o));

            let obj_assertions = tbox_new
                .filter(|(s, _p, _o)| *s == ObjectProperty)
                .map(|(s, _p, _o)| (s, s));

            let onp_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == onProperty)
                .map(|(s, _p, o)| (s, o));
            let onp_assertions_arr = onp_assertions.arrange_by_key();
            let onp_assertions_by_so = onp_assertions.map(|(s, o)| ((s, o), s)).arrange_by_key();

            let eqp_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == equivalentProperty)
                .map(|(s, _p, o)| (s, o));

            let dom_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == domain)
                .map(|(s, _p, o)| (s, o));
            let dom_assertions_by_o = dom_assertions.map(|(s, o)| (o, s));

            let rng_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == range)
                .map(|(s, _p, o)| (s, o));
            let rng_assertions_by_o = rng_assertions.map(|(s, o)| (o, s));

            let hv_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == hasValue)
                .map(|(s, _p, o)| (s, o));
            let hv_assertions_by_o = hv_assertions.map(|(s, o)| (o, s)).arrange_by_key();

            let svf_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == someValuesFrom)
                .map(|(s, _p, o)| (s, o));
            let svf_assertions_by_o = svf_assertions.map(|(o, s)| (o, s)).arrange_by_key();

            let avf_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == allValuesFrom)
                .map(|(s, _p, o)| (s, o));
            let avf_assertions_by_o = avf_assertions.map(|(o, s)| (o, s)).arrange_by_key();

            // scm-cls
            let scm_cls = cls_assertions.flat_map(|(c, _)| {
                vec![
                    (c, subClassOf, c),
                    (c, equivalentClass, c),
                    (c, subClassOf, Thing),
                    (Nothing, subClassOf, c),
                ]
            });

            // scm-sco

            let scm_sco = sco_assertions_by_o.join_core(&sco_assertions_arr, |&_c2, &c1, &c3| {
                Some((c1, subClassOf, c3))
            });

            // scm-eqc1

            let scm_eqc1 = eqc_assertions
                .flat_map(|(c1, c2)| vec![(c1, subClassOf, c2), (c2, subClassOf, c1)]);

            // scm-eqc2

            let scm_eqc2 = sco_assertions_by_os
                .join_core(&sco_assertions_by_so, |&(c1, c2), _, _| {
                    Some((c1, equivalentClass, c2))
                });

            // scm-op

            let scm_op = obj_assertions
                .flat_map(|(p, _)| vec![(p, subPropertyOf, p), (p, equivalentProperty, p)]);

            // scm-spo

            let scm_spo = spo_assertions_by_o.join_core(&spo_assertions_arr, |&_p2, &p1, &p3| {
                Some((p1, subPropertyOf, p3))
            });

            // scm-eqp1

            let scm_eqp1 = eqp_assertions
                .flat_map(|(p1, p2)| vec![(p1, subPropertyOf, p2), (p2, subPropertyOf, p1)]);

            // scm-eqp2

            let scm_eqp2 = spo_assertions_by_os
                .join_core(&spo_assertions_by_so, |&(p1, p2), _, _| {
                    Some((p1, equivalentProperty, p2))
                });

            // scm-dom1

            let scm_dom1 = dom_assertions_by_o
                .join_core(&sco_assertions_arr, |&_c1, &p, &c2| Some((p, domain, c2)));

            // scm-dom2

            let scm_dom2 = dom_assertions.join_core(&spo_assertions_by_o_arr, |&_p2, &c, &p1| {
                Some((p1, domain, c))
            });

            // scm-rng1

            let scm_rng1 = rng_assertions_by_o
                .join_core(&sco_assertions_arr, |&_c1, &p, &c2| Some((p, range, c2)));

            // scm-rng2

            let scm_rng2 = rng_assertions.join_core(&spo_assertions_by_o_arr, |&_p2, &c, &p1| {
                Some((p1, range, c))
            });

            // scm-hv

            let scm_hv_step_one =
                hv_assertions.join_core(&onp_assertions_arr, |&c1, &i, &p1| Some((i, (c1, p1))));

            let scm_hv_step_two = scm_hv_step_one
                .join_core(&hv_assertions_by_o, |&_i, &(c1, p1), &c2| {
                    Some((c2, (p1, c1)))
                });

            let scm_hv_step_three = scm_hv_step_two
                .join_core(&onp_assertions_arr, |&c2, &(p1, c1), &p2| {
                    Some(((p1, p2), (c1, c2)))
                });

            let scm_hv = scm_hv_step_three
                .join_core(&spo_assertions_by_so, |&(_p1, _p2), &(c1, c2), _| {
                    Some((c1, subClassOf, c2))
                });

            // scm-svf1

            let scm_svf1_step_one =
                svf_assertions.join_core(&onp_assertions_arr, |&c1, &y1, &p| Some((y1, (c1, p))));

            let scm_svf1_step_two = scm_svf1_step_one
                .join_core(&sco_assertions_arr, |&_y1, &(c1, p), &y2| {
                    Some((y2, (c1, p)))
                });

            let scm_svf1_step_three = scm_svf1_step_two
                .join_core(&svf_assertions_by_o, |&_y2, &(c1, p), &c2| {
                    Some(((c2, p), c1))
                });

            let scm_svf1 = scm_svf1_step_three
                .join_core(&onp_assertions_by_so, |&(c2, _p), &c1, _| {
                    Some((c1, subClassOf, c2))
                });

            // scm-svf2

            let scm_svf2_step_one =
                svf_assertions.join_core(&onp_assertions_arr, |&c1, &y, &p1| Some((y, (c1, p1))));

            let scm_svf2_step_two = scm_svf2_step_one
                .join_core(&svf_assertions_by_o, |&_y, &(c1, p1), &c2| {
                    Some((c2, (p1, c1)))
                });

            let scm_svf2_step_three = scm_svf2_step_two
                .join_core(&onp_assertions_arr, |&c2, &(p1, c1), &p2| {
                    Some(((p1, p2), (c1, c2)))
                });

            let scm_svf2 = scm_svf2_step_three
                .join_core(&spo_assertions_by_so, |&(_p1, _p2), &(c1, c2), _| {
                    Some((c1, subClassOf, c2))
                });

            // scm-avf1

            let scm_avf1_step_one =
                avf_assertions.join_core(&onp_assertions_arr, |&c1, &y1, &p| Some((y1, (c1, p))));

            let scm_avf1_step_two = scm_avf1_step_one
                .join_core(&sco_assertions_arr, |&_y1, &(c1, p), &y2| {
                    Some((y2, (c1, p)))
                });

            let scm_avf1_step_three = scm_avf1_step_two
                .join_core(&avf_assertions_by_o, |&_y2, &(c1, p), &c2| {
                    Some(((c2, p), c1))
                });

            let scm_avf1 = scm_avf1_step_three
                .join_core(&onp_assertions_by_so, |&(c2, _p), &c1, _| {
                    Some((c1, subClassOf, c2))
                });

            // scm-avf2

            let scm_avf2_step_one =
                avf_assertions.join_core(&onp_assertions_arr, |&c1, &y, &p1| Some((y, (c1, p1))));

            let scm_avf2_step_two = scm_avf2_step_one
                .join_core(&avf_assertions_by_o, |&_y, &(c1, p1), &c2| {
                    Some((c2, (p1, c1)))
                });

            let scm_avf2_step_three = scm_avf2_step_two
                .join_core(&onp_assertions_arr, |&c2, &(p1, c1), &p2| {
                    Some(((p1, p2), (c1, c2)))
                });

            let scm_avf2 = scm_avf2_step_three
                .join_core(&spo_assertions_by_so, |&(_p1, _p2), &(c1, c2), _| {
                    Some((c2, subClassOf, c1))
                });

            let scm = scm_cls.concatenate(vec![
                scm_sco, scm_eqc1, scm_eqc2, scm_op, scm_spo, scm_eqp1, scm_eqp2, scm_dom1,
                scm_dom2, scm_rng1, scm_rng2, scm_hv, scm_svf1, scm_svf2, scm_avf1, scm_avf2,
            ]);

            tbox_var.set(&tbox.enter(&inner).concat(&scm));

            tbox_new.leave()
        })
        .concat(&tbox)
        .consolidate();
    (materialization, lists)
}

pub fn owl2rl_abox<'a>(
    tbox: &TripleCollection<'a>,
    lists: &ListCollection<'a>,
    abox: &TripleCollection<'a>,
) -> TripleCollection<'a> {
    let mut outer = abox.scope();

    let lists_arr = lists.arrange_by_key();

    let lists_unrolled = lists.flat_map(|(x, xs)| {
        xs.into_iter()
            .enumerate()
            .map(move |(i, c_i)| (x, i as u32, c_i))
    });

    let lists_unrolled_size = lists
        .map(|(x, list)| (((list.len() - 1) as u32, x as u32), x))
        .arrange_by_key();

    let lists_unrolled_by_i_c_i = lists_unrolled.map(|(x, i, c_i)| ((i as u32, c_i), x));

    let lists_by_last_i_x = lists
        .map(|(head, list)| (((list.len() - 1) as u32, head), head))
        .arrange_by_key();

    let sco_assertions = tbox
        .filter(|(_s, p, _o)| *p == subClassOf)
        .map(|(s, _p, o)| (s, o));

    let eqc_assertions = tbox
        .filter(|(_s, p, _o)| *p == equivalentClass)
        .map(|(s, _p, o)| (s, o));

    let fp_assertions = tbox
        .filter(|(_s, _p, o)| *o == FunctionalProperty)
        .map(|(s, _p, _o)| (s, s));

    let ifp_assertions = tbox
        .filter(|(_s, _p, o)| *o == InverseFunctionalProperty)
        .map(|(s, _p, _o)| (s, s));

    let dom_assertions = tbox
        .filter(|(_s, p, _o)| *p == domain)
        .map(|(s, _p, o)| (s, o));

    let rng_assertions = tbox
        .filter(|(_s, p, _o)| *p == range)
        .map(|(s, _p, o)| (s, o));

    let symp_assertions = tbox
        .filter(|(_s, _p, o)| *o == SymmetricProperty)
        .map(|(_s, p, _o)| (p, p));

    let trans_assertions = tbox
        .filter(|(_s, _p, o)| *o == TransitiveProperty)
        .map(|(_s, p, _o)| (p, p));

    let spo_assertions = tbox
        .filter(|(_s, p, _o)| *p == subPropertyOf)
        .map(|(s, _p, o)| (s, o));

    let eqp_assertions = tbox
        .filter(|(_s, p, _o)| *p == equivalentProperty)
        .map(|(s, _p, o)| (s, o));

    let inv_assertions = tbox
        .filter(|(_s, p, _o)| *p == inverseOf)
        .map(|(s, _p, o)| (s, o));

    let svf_assertions = tbox
        .filter(|(_s, p, _o)| *p == someValuesFrom)
        .map(|(s, _p, o)| (s, o));

    let avf_assertions = tbox
        .filter(|(_s, p, _o)| *p == allValuesFrom)
        .map(|(s, _p, o)| (s, o));

    let op_assertions = tbox
        .filter(|(_s, p, _o)| *p == onProperty)
        .map(|(s, _p, o)| (s, o))
        .arrange_by_key();

    let hv_assertions = tbox
        .filter(|(_s, p, _o)| *p == hasValue)
        .map(|(s, _p, o)| (s, o));

    let oof_assertions = tbox
        .filter(|(_s, p, _o)| *p == oneOf)
        .map(|(s, _p, o)| (s, o));
    let oof_assertions_by_o = oof_assertions.map(|(s, o)| (o, s)).arrange_by_key();

    let iof_assertions = tbox
        .filter(|(_s, p, _o)| *p == intersectionOf)
        .map(|(s, _p, o)| (s, o));
    let iof_assertions_by_o = iof_assertions.map(|(s, o)| (o, s)).arrange_by_key();

    let pca_assertions = tbox
        .filter(|(_s, p, _o)| *p == propertyChainAxiom)
        .map(|(s, _p, o)| (s, o));
    let pca_assertions_by_o = pca_assertions.map(|(s, o)| (o, s)).arrange_by_key();

    // cls-thing
    let cls_thing = outer.new_collection_from(vec![(Thing, r#type, Class)]).1;

    // cls-nothing1
    let cls_nothing1 = outer.new_collection_from(vec![(Nothing, r#type, Class)]).1;

    let abox = abox.concatenate(vec![cls_thing, cls_nothing1]);

    outer
        .iterative::<usize, _, _>(|inner| {
            let abox_var = iterate::Variable::new(inner, Product::new(Default::default(), 1));
            let tbox_var = iterate::Variable::new(inner, Product::new(Default::default(), 1));

            let abox_new = abox_var.distinct();
            let tbox_new = tbox_var.distinct();

            let lists_unrolled_by_i_and_o = lists_unrolled_by_i_c_i.enter(&inner);

            let abox_by_s = abox_new.map(|(s, p, o)| (s, (p, o))).arrange_by_key();
            let abox_by_p = abox_new.map(|(s, p, o)| (p, (s, o))).arrange_by_key();
            let abox_by_o = abox_new.map(|(s, p, o)| (o, (s, p))).arrange_by_key();
            let abox_by_sp = abox_new.map(|(s, p, o)| ((s, p), o)).arrange_by_key();
            let abox_by_so = abox_new.map(|(s, p, o)| ((s, o), p)).arrange_by_key();
            let abox_by_po = abox_new.map(|(s, p, o)| ((p, o), s)).arrange_by_key();

            let type_assertions = abox_new.filter(|(_s, p, _o)| *p == r#type);
            let type_assertions_by_o = type_assertions.map(|(s, _p, o)| (o, s));
            let type_assertions_by_o_arr = type_assertions_by_o.arrange_by_key();
            let type_assertions_by_so = type_assertions.map(|(s, _p, o)| ((s, o), s));

            let property_assertions = abox_new.filter(|(_s, p, _o)| *p != r#type);
            let property_assertions_by_sp =
                property_assertions.map(|(u_i, p_i, u_j)| ((u_i, p_i), u_j));

            let sco_assertions = sco_assertions.enter(&inner);

            let eqc_assertions = eqc_assertions.enter(&inner);

            let fp_assertions = fp_assertions.enter(&inner);

            let ifp_assertions = ifp_assertions.enter(&inner);

            let sas_assertions = tbox_new
                .filter(|(_s, p, _o)| *p == sameAs)
                .map(|(s, _p, o)| (s, o));

            let sas_assertions_arr = sas_assertions.arrange_by_key();
            let sas_assertions_by_o = sas_assertions.map(|(s, o)| (o, s));

            let dom_assertions = dom_assertions.enter(&inner);

            let rng_assertions = rng_assertions.enter(&inner);

            let symp_assertions = symp_assertions.enter(&inner);

            let trans_assertions = trans_assertions.enter(&inner);

            let spo_assertions = spo_assertions.enter(&inner);

            let eqp_assertions = eqp_assertions.enter(&inner);

            let inv_assertions = inv_assertions.enter(&inner);

            let svf_assertions = svf_assertions.enter(&inner);

            let avf_assertions = avf_assertions.enter(&inner);

            let op_assertions = op_assertions.enter(&inner);

            let hv_assertions = hv_assertions.enter(&inner);

            let oof_assertions_by_o = oof_assertions_by_o.enter(&inner);

            // cax-sco
            let cax_sco = sco_assertions.join_core(&type_assertions_by_o_arr, |&_c1, &c2, &x| {
                Some((x, r#type, c2))
            });

            // cax-eqc1
            let cax_eqc1 = eqc_assertions.join_core(&type_assertions_by_o_arr, |&_c1, &c2, &x| {
                Some((x, r#type, c2))
            });

            // cax-eqc2
            let cax_eqc2 = eqc_assertions
                .map(|(s, o)| (o, s))
                .join_core(&type_assertions_by_o_arr, |&_c2, &c1, &x| {
                    Some((x, r#type, c1))
                });

            let cax = cax_sco.concatenate(vec![cax_eqc1, cax_eqc2]);

            // eq-rep-s
            let eq_rep_s = sas_assertions
                .join_core(&abox_by_s, |&_s, &s_prime, &(p, o)| Some((s_prime, p, o)));

            // eq-rep-p
            let eq_rep_p = sas_assertions
                .join_core(&abox_by_p, |&_p, &p_prime, &(s, o)| Some((s, p_prime, o)));

            // eq-rep-o
            let eq_rep_o = sas_assertions
                .join_core(&abox_by_o, |&_o, &o_prime, &(s, p)| Some((s, p, o_prime)));

            // eq-sym

            let eq_sym = sas_assertions.map(|(s, o)| (o, sameAs, s));

            // eq-trans

            let eq_trans = sas_assertions_by_o
                .join_core(&sas_assertions_arr, |&_y, &x, &z| Some((x, sameAs, z)));

            let eq_abox = eq_rep_s.concatenate(vec![eq_rep_p, eq_rep_o]);
            let eq_tbox = eq_sym.concat(&eq_trans);

            // prp-dom

            let prp_dom =
                dom_assertions.join_core(&abox_by_p, |&_p, &c, &(x, _y)| Some((x, r#type, c)));

            // prp-rng

            let prp_rng =
                rng_assertions.join_core(&abox_by_p, |&_p, &c, &(_x, y)| Some((y, r#type, c)));

            // prp-fp

            let prp_fp_step_one =
                fp_assertions.join_core(&abox_by_p, |&p, _, &(x, y1)| Some(((x, p), y1)));

            let prp_fp = prp_fp_step_one
                .join_core(&abox_by_sp, |&(_x, _p), &y1, &y2| Some((y1, sameAs, y2)))
                .filter(|(y1, _, y2)| *y1 != *y2);

            // prp-ifp

            let prp_ifp_step_one =
                ifp_assertions.join_core(&abox_by_p, |&p, _, &(x1, y)| Some(((p, y), x1)));

            let prp_ifp = prp_ifp_step_one
                .join_core(&abox_by_po, |&(_p, _y), &x1, &x2| Some((x1, sameAs, x2)))
                .filter(|(x1, _, x2)| *x1 != *x2);

            // prp-symp

            let prp_symp = symp_assertions.join_core(&abox_by_p, |&p, _, &(x, y)| Some((y, p, x)));

            // prp-trp

            let prp_trp_step_one =
                trans_assertions.join_core(&abox_by_p, |&p, _, &(x, y)| Some(((p, y), x)));

            let prp_trp =
                prp_trp_step_one.join_core(&abox_by_sp, |&(p, _y), &x, &z| Some((x, p, z)));

            // prp-spo1

            let prp_spo1 =
                spo_assertions.join_core(&abox_by_p, |&_p1, &p2, &(x, y)| Some((x, p2, y)));

            // prp-spo2

            let prp_spo2_step_one = property_assertions
                .map(|(u_i, p_i, u_j)| ((0u32, p_i), (u_i, u_j)))
                .join_core(
                    &lists_unrolled_by_i_and_o.arrange_by_key(),
                    |&(i, _p_i), &(u_i, u_j), &x| Some(((i, x), (u_i, u_j))),
                );

            let prp_spo2 = inner
                .iterative::<usize, _, _>(|inner_squared| {
                    let property_assertions_var = Variable::new_from(
                        prp_spo2_step_one.enter(inner_squared),
                        Product::new(Default::default(), 1),
                    );

                    let lists_by_i_x = lists_unrolled_by_i_and_o
                        .enter(inner_squared)
                        .map(|((i, p_i), x)| ((i, x), p_i))
                        .arrange_by_key();

                    let property_assertions_new = property_assertions_var.distinct();

                    let property_assertions_by_sp = property_assertions_by_sp
                        .enter(inner_squared)
                        .arrange_by_key();

                    let property_assertions_new = property_assertions_new
                        .map(|((i, x), (u_i, u_j))| ((i + 1, x), (u_i, u_j)))
                        .join_core(&lists_by_i_x, |&(i, x), &(u_i, u_j), &p_i| {
                            Some(((u_j, p_i), (i, x, u_i)))
                        })
                        .join_core(
                            &property_assertions_by_sp,
                            |&(_u_j, _p_i), &(i, x, u_i), &u_k| Some(((i, x), (u_i, u_k))),
                        )
                        .concat(&property_assertions_new)
                        .distinct();

                    property_assertions_var.set(&property_assertions_new);

                    property_assertions_new.leave()
                })
                .join_core(
                    &lists_by_last_i_x.enter(&inner),
                    |&(_i, x), &(u_i, u_k), &_| Some((x, (u_i, u_k))),
                )
                .join_core(
                    &pca_assertions_by_o.enter(&inner),
                    |&_x, &(u_i, u_k), &p| Some((u_i, p, u_k)),
                );

            // prp-eqp1

            let prp_eqp1 =
                eqp_assertions.join_core(&abox_by_p, |&_p1, &p2, &(x, y)| Some((x, p2, y)));

            // prp-eqp2

            let prp_eqp2 = eqp_assertions
                .map(|(p1, p2)| (p2, p1))
                .join_core(&abox_by_p, |&_p2, &p1, &(x, y)| Some((x, p1, y)));

            // prp-inv1

            let prp_inv1 =
                inv_assertions.join_core(&abox_by_p, |&_p1, &p2, &(x, y)| Some((y, p2, x)));

            // prp-inv2

            let prp_inv2 = inv_assertions
                .map(|(p1, p2)| (p2, p1))
                .join_core(&abox_by_p, |&_p2, &p1, &(x, y)| Some((y, p1, x)));

            let prp_abox = prp_dom.concatenate(vec![
                prp_rng, prp_symp, prp_trp, prp_spo1, prp_spo2, prp_eqp1, prp_eqp2, prp_inv1,
                prp_inv2,
            ]);

            let prp_tbox = prp_fp.concat(&prp_ifp);

            // cls-int1

            let cls_int1_step_one = type_assertions_by_o.map(|(o, s)| ((0, o), s)).join_core(
                &lists_unrolled_by_i_and_o.arrange_by_key(),
                |&(i, _c_i), &y, &x| Some(((i as u32, x), y)),
            );

            let cls_int1 = inner
                .iterative::<usize, _, _>(|inner_squared| {
                    let type_assertions_var = Variable::new_from(
                        cls_int1_step_one.enter(inner_squared),
                        Product::new(Default::default(), 1),
                    );

                    let lists_by_i_x = lists_unrolled_by_i_and_o
                        .enter(&inner_squared)
                        .map(|((i, c_i), x)| ((i, x), c_i))
                        .arrange_by_key();

                    let type_assertions_new = type_assertions_var.distinct();

                    let type_assertions_by_so =
                        type_assertions_by_so.enter(inner_squared).arrange_by_key();

                    let type_assertions_new = type_assertions_new
                        .map(|((i, x), y)| ((i + 1, x), y))
                        .join_core(&lists_by_i_x, |&(i, x), &y, &c_i| Some(((c_i, y), (i, x))))
                        .join_core(&type_assertions_by_so, |&(_c_i, y), &(i, x), &_y| {
                            Some(((i, x), y))
                        })
                        .concat(&type_assertions_new)
                        .distinct();

                    type_assertions_var.set(&type_assertions_new);

                    type_assertions_new.leave()
                })
                .join_core(&lists_unrolled_size.enter(inner), |&(_i, x), &y, &_| {
                    Some((x, y))
                })
                .join_core(&iof_assertions_by_o.enter(&inner), |&_x, &y, &c| {
                    Some((y, r#type, c))
                });

            // cls-svf1
            let cls_svf1_step_one =
                svf_assertions.join_core(&op_assertions, |&x, &y, &p| Some((p, (y, x))));

            let cls_svf1_step_two = cls_svf1_step_one
                .join_core(&abox_by_p, |&_p, &(y, x), &(u, v)| Some(((v, y), (u, x))));

            let cls_svf1 = cls_svf1_step_two
                .join_core(&abox_by_so, |&(_v, _y), &(u, x), &_| Some((u, r#type, x)));

            // cls-svf2
            let cls_svf2_step_one = svf_assertions
                .filter(|(_s, o)| *o == Thing)
                .join_core(&op_assertions, |&x, _, &p| Some((p, x)));

            let cls_svf2 =
                cls_svf2_step_one.join_core(&abox_by_p, |&_p, &x, &(u, _v)| Some((u, r#type, x)));

            // cls-avf
            let cls_avf_step_one =
                avf_assertions.join_core(&op_assertions, |&x, &y, &p| Some((x, (p, y))));

            let cls_avf_step_two = cls_avf_step_one
                .join_core(&type_assertions_by_o_arr, |&_x, &(p, y), &u| {
                    Some(((u, p), y))
                });

            let cls_avf =
                cls_avf_step_two.join_core(&abox_by_sp, |&(_u, _p), &y, &v| Some((v, r#type, y)));

            // cls-hv1
            let cls_hv1_step_one =
                hv_assertions.join_core(&op_assertions, |&x, &y, &p| Some((x, (p, y))));

            let cls_hv1 = cls_hv1_step_one
                .join_core(&type_assertions_by_o_arr, |&_x, &(p, y), &u| {
                    Some((u, p, y))
                });

            // cls-hv2
            let cls_hv2_step_one =
                hv_assertions.join_core(&op_assertions, |&x, &y, &p| Some(((p, y), x)));

            let cls_hv2 =
                cls_hv2_step_one.join_core(&abox_by_po, |&(_p, _y), &x, &u| Some((u, r#type, x)));

            // cls-oo

            let cls_oo = oof_assertions_by_o
                .join_core(&lists_arr.enter(&inner), |&_x, &c, list| {
                    Some((c, list.clone()))
                })
                .flat_map(|(c, list)| {
                    list.iter()
                        .map(|y_x| (*y_x, r#type, c))
                        .collect::<Vec<Triple>>()
                });

            let cls =
                cls_svf1.concatenate(vec![cls_int1, cls_svf2, cls_avf, cls_hv1, cls_hv2, cls_oo]);

            abox_var.set(
                &abox
                    .enter(&inner)
                    .concatenate(vec![cax, eq_abox, prp_abox, cls]),
            );
            tbox_var.set(&tbox.enter(&inner).concatenate(vec![eq_tbox, prp_tbox]));

            abox_new.leave()
        })
        .concat(&abox)
        .consolidate()
}
