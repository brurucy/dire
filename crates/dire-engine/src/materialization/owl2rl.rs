use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::{iterate, JoinCore, Threshold};
use timely::dataflow::operators::ToStream;
use timely::dataflow::Scope;
use timely::order::Product;

use crate::model::consts::constants::owl::{
    allValuesFrom, equivalentClass, equivalentProperty, hasValue, inverseOf, onProperty, sameAs,
    someValuesFrom, Class, Nothing, ObjectProperty, SymmetricProperty, Thing, TransitiveProperty,
};
use crate::model::consts::constants::rdfs::{domain, r#type, range, subClassOf, subPropertyOf};
use crate::model::types::TripleCollection;

fn tbox_eq<'a>(tbox: &TripleCollection<'a>) -> TripleCollection<'a> {
    let sas_assertions = tbox
        .filter(|(s, p, o)| *p == sameAs)
        .map(|(s, p, o)| (s, o));

    let sas_assertions_by_o = sas_assertions.map(|(s, o)| (o, s));

    // eq-sym

    let eq_sym = sas_assertions.map(|(s, o)| (o, sameAs, s));

    // eq_trans

    let eq_trans =
        sas_assertions_by_o.join_core(&sas_assertions, |&y, &x, &z| Some((x, sameAs, z)));

    eq_trans.concat(&eq_sym)
}

fn abox_eq<'a>(tbox: &TripleCollection<'a>, abox: &TripleCollection<'a>) -> TripleCollection<'a> {
    let sas_assertions = tbox
        .filter(|(s, p, o)| *p == sameAs)
        .map(|(s, p, o)| (s, o));

    let abox_by_s = abox.map(|(s, p, o)| (s, (p, o)));

    let abox_by_p = abox.map(|(s, p, o)| (p, (s, o)));

    let abox_by_o = abox.map(|(s, p, o)| (o, (s, p)));

    // eq-rep-s
    let eq_rep_s =
        sas_assertions.join_core(&abox_by_s, |&s, &s_prime, &(p, o)| Some((s_prime, p, o)));

    // eq-rep-p
    let eq_rep_p =
        sas_assertions.join_core(&abox_by_p, |&p, &p_prime, &(s, o)| Some((s, p_prime, o)));

    // eq-rep-o
    let eq_rep_o =
        sas_assertions.join_core(&abox_by_o, |&o, &o_prime, &(s, p)| Some((s, p, o_prime)));

    eq_rep_s.concatenate(vec![eq_rep_p, eq_rep_o])
}

fn tbox_scm<'a>(tbox: &TripleCollection<'a>) -> TripleCollection<'a> {
    let cls_assertions = tbox.filter(|(s, p, o)| *o == Class).map(|(s, p, o)| (s, s));

    let sco_assertions = tbox
        .filter(|(s, p, o)| *p == subClassOf)
        .map(|(s, p, o)| (s, o));
    let sco_assertions_by_o = sco_assertions.map(|(s, o)| (o, s));
    let sco_assertions_by_so = sco_assertions.map(|(s, o)| ((s, o), s));
    let sco_assertions_by_os = sco_assertions_by_o.map(|(o, s)| ((o, s), s));

    let spo_assertions = tbox.filter(|(s, p, o)| *p == subPropertyOf);
    let spo_assertions_by_o = spo_assertions.map(|(s, o)| (o, s));
    let spo_assertions_by_so = spo_assertions.map(|(s, o)| ((s, o), s));
    let spo_assertions_by_os = spo_assertions_by_o.map(|(o, s)| ((o, s), s));

    let eqc_assertions = tbox
        .filter(|(s, p, o)| *p == equivalentClass)
        .map(|(s, p, o)| (s, o));

    let obj_assertions = tbox
        .filter(|(s, p, o)| *s == ObjectProperty)
        .map(|(s, p, o)| (s, s));

    let onp_assertions = tbox
        .filter(|(s, p, o)| *p == onProperty)
        .map(|(s, p, o)| (s, o));

    let onp_assertions_by_so = onp_assertions.map(|(s, o)| ((s, o), s));

    let eqp_assertions = tbox
        .filter(|(s, p, o)| *p == equivalentProperty)
        .map(|(s, p, o)| (s, o));

    let dom_assertions = tbox
        .filter(|(s, p, o)| *p == domain)
        .map(|(s, p, o)| (s, o));
    let dom_assertions_by_o = dom_assertions.map(|(s, o)| (o, s));

    let rng_assertions = tbox.filter(|(s, p, o)| *p == range).map(|(s, p, o)| (s, o));
    let rng_assertions_by_o = rng_assertions.map(|(s, o)| (o, s));

    let hv_assertions = tbox
        .filter(|(s, p, o)| *p == hasValue)
        .map(|(s, p, o)| (s, o));
    let hv_assertions_by_o = hv_assertions.map(|(s, o)| (o, s));

    let svf_assertions = tbox
        .filter(|(s, p, o)| *p == someValuesFrom)
        .map(|(s, p, o)| (s, o));

    let svf_assertions_by_o = svf_assertions.map(|(o, s)| (o, s));

    let avf_assertions = tbox
        .filter(|(s, p, o)| *p == allValuesFrom)
        .map(|(s, p, o)| (s, o));

    let avf_assertions_by_o = avf_assertions.map(|(o, s)| (o, s));

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

    let scm_sco =
        sco_assertions_by_o.join_core(&sco_assertions, |&c2, &c1, &c3| Some((c1, subClassOf, c3)));

    // scm-eqc1

    let scm_eqc1 =
        eqc_assertions.flat_map(|(c1, c2)| vec![(c1, subClassOf, c2), (c2, subClassOf, c1)]);

    // scm-eqc2

    let scm_eqc2 = sco_assertions_by_os.join_core(&sco_assertions_by_so, |&(c1, c2), _, _| {
        Some((c1, equivalentClass, c2))
    });

    // scm-op

    let scm_op =
        obj_assertions.flat_map(|(&p, _)| vec![(p, subPropertyOf, p), (p, equivalentProperty, p)]);

    // scm-spo

    let scm_spo = spo_assertions_by_o.join_core(&spo_assertions, |&p2, &p1, &p3| {
        Some((p1, subPropertyOf, p3))
    });

    // scm-eqp1

    let scm_eqp1 =
        eqp_assertions.flat_map(|(p1, p2)| vec![(p1, subPropertyOf, p2), (p2, subPropertyOf, p1)]);

    // scm-eqp2

    let scm_eqp2 = spo_assertions_by_os.join_core(&spo_assertions_by_so, |&(p1, p2), _, _| {
        Some((p1, equivalentProperty, p2))
    });

    // scm-dom1

    let scm_dom1 =
        dom_assertions_by_o.join_core(&sco_assertions, |&c1, &p, &c2| Some((p, domain, c2)));

    // scm-dom2

    let scm_dom2 =
        dom_assertions.join_core(&spo_assertions_by_o, |&p2, &c, &p1| Some((p1, domain, c)));

    // scm-rng1

    let scm_rng1 =
        rng_assertions_by_o.join_core(&sco_assertions, |&c1, &p, &c2| Some((p, range, c2)));

    // scm-rng2

    let scm_rng2 =
        rng_assertions.join_core(&spo_assertions_by_o, |&p2, &c, &p1| Some((p1, range, c)));

    // scm-hv

    let scm_hv_step_one =
        hv_assertions.join_core(&onp_assertions, |&c1, &i, &p1| Some((i, (c1, p1))));

    let scm_hv_step_two = scm_hv_step_one.join_core(&hv_assertions_by_o, |&i, &(c1, p1), &c2| {
        Some((c2, (p1, c1)))
    });

    let scm_hv_step_three = scm_hv_step_two.join_core(&onp_assertions, |&c2, &(p1, c1), &p2| {
        Some(((p1, p2), (c1, c2)))
    });

    let scm_hv = scm_hv_step_three.join_core(&spo_assertions_by_so, |&(p1, p2), &(c1, c2), _| {
        Some((c1, subClassOf, c2))
    });

    // scm-svf1

    let scm_svf1_step_one =
        svf_assertions.join_core(&onp_assertions, |&c1, &y1, &p| Some((y1, (c1, p))));

    let scm_svf1_step_two =
        scm_svf1_step_one.join_core(&sco_assertions, |&y1, &(c1, p), &y2| Some((y2, (c1, p))));

    let scm_svf1_step_three = scm_svf1_step_two
        .join_core(&svf_assertions_by_o, |&y2, &(c1, p), &c2| {
            Some(((c2, p), c1))
        });

    let scm_svf1 = scm_svf1_step_three.join_core(&onp_assertions_by_so, |&(c2, p), &c1, _| {
        Some((c1, subClassOf, c2))
    });

    // scm-svf2

    let scm_svf2_step_one =
        svf_assertions.join_core(&onp_assertions, |&c1, &y, &p1| Some((y, (c1, p1))));

    let scm_svf2_step_two = scm_svf2_step_one
        .join_core(&svf_assertions_by_o, |&y, &(c1, p1), &(c2)| {
            Some((c2, (p1, c1)))
        });

    let scm_svf2_step_three = scm_svf2_step_two
        .join_core(&onp_assertions, |&c2, &(p1, c1), &p2| {
            Some(((p1, p2), (c1, c2)))
        });

    let scm_svf2 = scm_svf2_step_three
        .join_core(&spo_assertions_by_so, |&(p1, p2), &(c1, c2), _| {
            Some((c1, subClassOf, c2))
        });

    // scm-avf1

    let scm_avf1_step_one =
        avf_assertions.join_core(&onp_assertions, |&c1, &y1, &p| Some((y1, (c1, p))));

    let scm_avf1_step_two =
        scm_avf1_step_one.join_core(&sco_assertions, |&y1, &(c1, p), &y2| Some((y2, (c1, p))));

    let scm_avf1_step_three = scm_avf1_step_two
        .join_core(&avf_assertions_by_o, |&y2, &(c1, p), &c2| {
            Some(((c2, p), c1))
        });

    let scm_avf1 = scm_avf1_step_three.join_core(&onp_assertions_by_so, |&(c2, p), &c1, _| {
        Some((c1, subClassOf, c2))
    });

    // scm-avf2

    let scm_avf2_step_one =
        avf_assertions.join_core(&onp_assertions, |&c1, &y, &p1| Some((y, (c1, p1))));

    let scm_avf2_step_two = scm_avf2_step_one
        .join_core(&avf_assertions_by_o, |&y, &(c1, p1), &(c2)| {
            Some((c2, (p1, c1)))
        });

    let scm_avf2_step_three = scm_avf2_step_two
        .join_core(&onp_assertions, |&c2, &(p1, c1), &p2| {
            Some(((p1, p2), (c1, c2)))
        });

    let scm_avf2 = scm_avf2_step_three
        .join_core(&spo_assertions_by_so, |&(p1, p2), &(c1, c2), _| {
            Some((c2, subClassOf, c1))
        });

    scm_cls.concatenate(vec![
        scm_sco, scm_eqc1, scm_eqc2, scm_op, scm_spo, scm_eqp1, scm_eqp2, scm_dom1, scm_dom2,
        scm_rng1, scm_rng2, scm_hv, scm_svf1, scm_svf2, scm_avf1, scm_avf2,
    ])
}

fn abox_prp<'a>(tbox: &TripleCollection<'a>, abox: &TripleCollection<'a>) -> TripleCollection<'a> {
    let dom_assertions = tbox
        .filter(|(s, p, o)| *p == domain)
        .map(|(s, p, o)| (s, o));

    let rng_assertions = tbox.filter(|(s, p, o)| *p == range).map(|(s, p, o)| (s, o));

    let symp_assertions = tbox
        .filter(|(s, p, o)| *o == SymmetricProperty)
        .map(|(s, p, o)| (p, p));

    let trans_assertions = tbox
        .filter(|(s, p, o)| *o == TransitiveProperty)
        .map(|(s, p, o)| (p, p));

    let spo_assertions = tbox
        .filter(|(s, p, o)| *p == subPropertyOf)
        .map(|(s, p, o)| (s, o));

    let eqp_assertions = tbox
        .filter(|(s, p, o)| *p == equivalentProperty)
        .map(|(s, p, o)| (s, o));

    let inv_assertions = tbox
        .filter(|(s, p, o)| *p == inverseOf)
        .map(|(s, p, o)| (s, o));

    let abox_by_p = abox.map(|(s, p, o)| (p, (s, o))).arrange_by_key();
    let abox_by_sp = abox.map(|(s, p, o)| ((s, p), o)).arrange_by_key();

    // prp-dom

    let prp_dom = dom_assertions.join_core(&abox_by_p, |&p, &c, &(x, y)| Some((x, r#type, c)));

    // prp-rng

    let prp_rng = rng_assertions.join_core(&abox_by_p, |&p, &c, &(x, y)| Some((y, r#type, c)));

    // prp-symp

    let prp_symp = symp_assertions.join_core(&abox_by_p, |&p, _, &(x, y)| Some((y, p, x)));

    // prp-trp

    let prp_trp_step_one =
        trans_assertions.join_core(&abox_by_p, |&p, _, &(x, y)| Some(((p, y), x)));

    let prp_trp = prp_trp_step_one.join_core(&abox_by_sp, |&(p, y), &x, &z| Some((x, p, z)));

    // prp-spo1

    let prp_spo1 = spo_assertions.join_core(&abox_by_p, |&p1, &p2, &(x, y)| Some((x, p2, y)));

    // prp-eqp1

    let prp_eqp1 = eqp_assertions.join_core(&abox_by_p, |&p1, &p2, &(x, y)| Some((x, p2, y)));

    // prp-eqp2

    let prp_eqp2 = eqp_assertions
        .map(|(p1, p2)| (p2, p1))
        .join_core(&abox_by_p, |&p2, &p1, &(x, y)| Some((x, p1, y)));

    // prp-inv1

    let prp_inv1 = inv_assertions.join_core(&abox_by_p, |&p1, &p2, &(x, y)| Some((y, p2, x)));

    // prp-inv2

    let prp_inv2 = inv_assertions
        .map(|(p1, p2)| (p2, p1))
        .join_core(&abox_by_p, |&p2, &p1, &(x, y)| Some((y, p1, x)));

    prp_dom.concatenate(vec![
        prp_rng, prp_symp, prp_trp, prp_spo1, prp_eqp1, prp_eqp2, prp_inv1, prp_inv2,
    ])
}

fn abox_cax<'a>(tbox: &TripleCollection<'a>, abox: &TripleCollection<'a>) -> TripleCollection<'a> {
    let sco_assertions = tbox
        .filter(|(s, p, o)| *p == subClassOf)
        .map(|(s, p, o)| (s, o));

    let eqc_assertions = tbox
        .filter(|(s, p, o)| *p == equivalentClass)
        .map(|(s, p, o)| (s, o));

    let type_assertions_by_o = abox
        .filter(|(s, p, o)| *p == r#type)
        .map(|(s, p, o)| (o, s))
        .arrange_by_key();

    // cax-sco
    let cax_sco =
        sco_assertions.join_core(&type_assertions_by_o, |&c1, &c2, &x| Some((x, r#type, c2)));

    // cax-eqc1
    let cax_eqc1 =
        eqc_assertions.join_core(&type_assertions_by_o, |&c1, &c2, &x| Some((x, r#type, c2)));

    // cax-eqc2
    let cax_eqc2 = eqc_assertions
        .map(|(s, o)| (o, s))
        .join_core(&type_assertions_by_o, |&c2, &c1, &x| Some((x, r#type, c1)));

    cax_sco.concatenate(vec![cax_eqc1, cax_eqc2])
}

fn abox_cls<'a>(tbox: &TripleCollection<'a>, abox: &TripleCollection<'a>) -> TripleCollection<'a> {
    let svf_assertions = tbox
        .filter(|(s, p, o)| *p == someValuesFrom)
        .map(|(s, p, o)| (s, o));

    let avf_assertions = tbox
        .filter(|(s, p, o)| *p == allValuesFrom)
        .map(|(s, p, o)| (s, o));

    let op_assertions = tbox
        .filter(|(s, p, o)| *p == onProperty)
        .map(|(s, p, o)| (s, o))
        .arrange_by_key();

    let hv_assertions = tbox
        .filter(|(s, p, o)| *p == hasValue)
        .map(|(s, p, o)| (s, o));

    let abox_by_p = abox.map(|(s, p, o)| (p, (s, o))).arrange_by_key();
    let abox_by_so = abox.map(|(s, p, o)| ((s, o), p)).arrange_by_key();
    let type_assertions_by_o = abox
        .filter(|(s, p, o)| *p == r#type)
        .map(|(s, p, o)| (o, s))
        .arrange_by_key();
    let abox_by_sp = abox.map(|(s, p, o)| ((s, p), o)).arrange_by_key();
    let abox_by_po = abox.map(|(s, p, o)| ((p, o), s)).arrange_by_key();

    // cls-svf1
    let cls_svf1_step_one =
        svf_assertions.join_core(&op_assertions, |&x, &y, &p| Some((p, (y, x))));

    let cls_svf1_step_two =
        cls_svf1_step_one.join_core(&abox_by_p, |&p, &(y, x), &(u, v)| Some(((v, y), (u, x))));

    let cls_svf1 =
        cls_svf1_step_two.join_core(&abox_by_so, |&(v, y), &(u, x), &_| Some((u, r#type, x)));

    // cls-svf2
    let cls_svf2_step_one = svf_assertions
        .filter(|(s, o)| *o == Thing)
        .join_core(&op_assertions, |&x, _, &p| Some((p, x)));

    let cls_svf2 = cls_svf2_step_one.join_core(&abox_by_p, |&p, &x, &(u, v)| Some((u, r#type, x)));

    // cls-avf
    let cls_avf_step_one = avf_assertions.join_core(&op_assertions, |&x, &y, &p| Some((x, (p, y))));

    let cls_avf_step_two =
        cls_avf_step_one.join_core(&type_assertions_by_o, |&x, &(p, y), &u| Some(((u, p), y)));

    let cls_avf = cls_avf_step_two.join_core(&abox_by_sp, |&(u, p), &y, &v| Some((v, r#type, y)));

    // cls-hv1
    let cls_hv1_step_one = hv_assertions.join_core(&op_assertions, |&x, &y, &p| Some((x, (p, y))));

    let cls_hv1 =
        cls_hv1_step_one.join_core(&type_assertions_by_o, |&x, &(p, y), &u| Some((u, p, y)));

    // cls-hv2
    let cls_hv2_step_one = hv_assertions.join_core(&op_assertions, |&x, &y, &p| Some(((p, y), x)));

    let cls_hv2 = cls_hv2_step_one.join_core(&abox_by_po, |&(p, y), &x, &u| Some((u, r#type, x)));

    // cls-thing
    let cls_thing = abox
        .scope()
        .new_collection_from(vec![(Thing, r#type, Class)]);

    cls_svf1.concatenate(vec![cls_svf2, cls_avf, cls_hv1, cls_hv2, cls_thing.1])
}

mod tests {
    use timely::communication::Config;

    use crate::materialization::common::dummy_binary_materialization;
    use crate::materialization::owl2rl::tbox_eq;
    use crate::model::consts::constants::owl::{sameAs, Class};
    use crate::model::consts::constants::rdfs::r#type;
    use crate::model::consts::constants::MAX_CONST;
    use crate::reason::reason;

    #[test]
    fn eq_works() {
        let (tbox_output_sink, tbox_output_source) = flume::unbounded();
        let (tbox_input_sink, tbox_input_source) = flume::bounded(5);
        let (abox_output_sink, abox_output_source) = flume::unbounded();
        let (abox_input_sink, abox_input_source) = flume::bounded(0);
        let (termination_sink, termination_source) = flume::bounded(1);

        let employee = MAX_CONST + 1;
        let empregado = MAX_CONST + 2;
        let tootaja = MAX_CONST + 3;

        tbox_input_sink
            .send(((employee, r#type, Class), 1))
            .unwrap();

        tbox_input_sink
            .send(((empregado, r#type, Class), 1))
            .unwrap();

        tbox_input_sink.send(((tootaja, r#type, Class), 1)).unwrap();

        tbox_input_sink
            .send(((employee, sameAs, empregado), 1))
            .unwrap();

        tbox_input_sink
            .send(((empregado, sameAs, tootaja), 1))
            .unwrap();

        termination_sink.send(()).unwrap();

        reason(
            timely::Config {
                communication: Config::Process(1),
                worker: Default::default(),
            },
            tbox_eq,
            dummy_binary_materialization,
            tbox_input_source,
            abox_input_source,
            tbox_output_sink,
            abox_output_sink,
            termination_source,
        );
        let mut actual_tbox_diffs: Vec<(u32, u32, u32)> = vec![];
        let mut actual_abox_diffs: Vec<(u32, u32, u32)> = vec![];

        while let Ok(diff) = tbox_output_source.try_recv() {
            actual_tbox_diffs.push(diff.0)
        }

        actual_tbox_diffs.sort_unstable();
        actual_tbox_diffs.dedup();

        let mut expected_tbox_diffs = vec![
            // Input
            //(employee, r#type, Class),
            //(empregado, r#type, Class),
            //(tootaja, r#type, Class),
            (employee, sameAs, empregado),
            (empregado, sameAs, tootaja),
            // Materialization
            (employee, sameAs, tootaja),
            (tootaja, sameAs, employee),
            (empregado, sameAs, employee),
            (tootaja, sameAs, empregado),
        ];

        expected_tbox_diffs.sort_unstable();
        expected_tbox_diffs.dedup();

        assert_eq!(expected_tbox_diffs, actual_tbox_diffs)
    }
}
