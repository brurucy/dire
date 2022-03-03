use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::{iterate, Consolidate, Iterate, JoinCore, Threshold};
use timely::dataflow::operators::ToStream;
use timely::dataflow::Scope;
use timely::order::Product;

use crate::model::consts::constants::owl::{
    allValuesFrom, equivalentClass, equivalentProperty, hasValue, inverseOf, onProperty, sameAs,
    someValuesFrom, Class, Nothing, ObjectProperty, SymmetricProperty, Thing, TransitiveProperty,
};
use crate::model::consts::constants::rdfs::{domain, r#type, range, subClassOf, subPropertyOf};
use crate::model::types::TripleCollection;

pub fn owl2rl_tbox<'a>(tbox: &TripleCollection<'a>) -> TripleCollection<'a> {
    let mut outer = tbox.scope();

    let sas_assertions = tbox
        .filter(|(s, p, o)| *p == sameAs)
        .map(|(s, p, o)| (s, o));
    let sas_assertions_arr = sas_assertions.arrange_by_key();
    let sas_assertions_by_o = sas_assertions.map(|(s, o)| (o, s));

    let cls_assertions = tbox.filter(|(s, p, o)| *o == Class).map(|(s, p, o)| (s, s));

    let sco_assertions = tbox
        .filter(|(s, p, o)| *p == subClassOf)
        .map(|(s, p, o)| (s, o));
    let sco_assertions_arr = sco_assertions.arrange_by_key();
    let sco_assertions_by_o = sco_assertions.map(|(s, o)| (o, s));
    let sco_assertions_by_so = sco_assertions.map(|(s, o)| ((s, o), s)).arrange_by_key();
    let sco_assertions_by_os = sco_assertions_by_o.map(|(o, s)| ((o, s), s));

    let spo_assertions = tbox
        .filter(|(s, p, o)| *p == subPropertyOf)
        .map(|(s, p, o)| (s, o));
    let spo_assertions_arr = spo_assertions.arrange_by_key();
    let spo_assertions_by_o = spo_assertions.map(|(s, o)| (o, s));
    let spo_assertions_by_o_arr = spo_assertions_by_o.arrange_by_key();
    let spo_assertions_by_so = spo_assertions.map(|(s, o)| ((s, o), s)).arrange_by_key();
    let spo_assertions_by_os = spo_assertions.map(|(s, o)| ((o, s), s));

    let eqc_assertions = tbox
        .filter(|(s, p, o)| *p == equivalentClass)
        .map(|(s, p, o)| (s, o));

    let obj_assertions = tbox
        .filter(|(s, p, o)| *s == ObjectProperty)
        .map(|(s, p, o)| (s, s));

    let onp_assertions = tbox
        .filter(|(s, p, o)| *p == onProperty)
        .map(|(s, p, o)| (s, o));
    let onp_assertions_arr = onp_assertions.arrange_by_key();
    let onp_assertions_by_so = onp_assertions.map(|(s, o)| ((s, o), s)).arrange_by_key();

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
    let hv_assertions_by_o = hv_assertions.map(|(s, o)| (o, s)).arrange_by_key();

    let svf_assertions = tbox
        .filter(|(s, p, o)| *p == someValuesFrom)
        .map(|(s, p, o)| (s, o));
    let svf_assertions_by_o = svf_assertions.map(|(o, s)| (o, s)).arrange_by_key();

    let avf_assertions = tbox
        .filter(|(s, p, o)| *p == allValuesFrom)
        .map(|(s, p, o)| (s, o));
    let avf_assertions_by_o = avf_assertions.map(|(o, s)| (o, s)).arrange_by_key();

    outer
        .iterative::<usize, _, _>(|inner| {
            let tbox_var = iterate::Variable::new(inner, Product::new(Default::default(), 1));

            let tbox_new = tbox_var.distinct();

            let sas_assertions = sas_assertions.enter(&inner);
            let sas_assertions_arr = sas_assertions_arr.enter(&inner);
            let sas_assertions_by_o = sas_assertions_by_o.enter(&inner);

            let cls_assertions = cls_assertions.enter(&inner);

            let sco_assertions_arr = sco_assertions_arr.enter(&inner);
            let sco_assertions_by_o = sco_assertions_by_o.enter(&inner);
            let sco_assertions_by_so = sco_assertions_by_so.enter(&inner);
            let sco_assertions_by_os = sco_assertions_by_os.enter(&inner);

            let spo_assertions_arr = spo_assertions_arr.enter(&inner);
            let spo_assertions_by_o = spo_assertions_by_o.enter(&inner);
            let spo_assertions_by_o_arr = spo_assertions_by_o_arr.enter(&inner);
            let spo_assertions_by_so = spo_assertions_by_so.enter(&inner);
            let spo_assertions_by_os = spo_assertions_by_os.enter(&inner);

            let eqc_assertions = eqc_assertions.enter(&inner);

            let obj_assertions = obj_assertions.enter(&inner);

            let onp_assertions_arr = onp_assertions_arr.enter(&inner);
            let onp_assertions_by_so = onp_assertions_by_so.enter(&inner);

            let eqp_assertions = eqp_assertions.enter(&inner);

            let dom_assertions = dom_assertions.enter(&inner);
            let dom_assertions_by_o = dom_assertions_by_o.enter(&inner);

            let rng_assertions = rng_assertions.enter(&inner);
            let rng_assertions_by_o = rng_assertions_by_o.enter(&inner);

            let hv_assertions = hv_assertions.enter(&inner);
            let hv_assertions_by_o = hv_assertions_by_o.enter(&inner);

            let svf_assertions = svf_assertions.enter(&inner);
            let svf_assertions_by_o = svf_assertions_by_o.enter(&inner);

            let avf_assertions = avf_assertions.enter(&inner);
            let avf_assertions_by_o = avf_assertions_by_o.enter(&inner);

            // eq-sym

            let eq_sym = sas_assertions.map(|(s, o)| (o, sameAs, s));

            // eq_trans

            let eq_trans = sas_assertions_by_o
                .join_core(&sas_assertions_arr, |&y, &x, &z| Some((x, sameAs, z)));

            let eq = eq_trans.concat(&eq_sym);

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

            let scm_sco = sco_assertions_by_o.join_core(&sco_assertions_arr, |&c2, &c1, &c3| {
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

            let scm_spo = spo_assertions_by_o.join_core(&spo_assertions_arr, |&p2, &p1, &p3| {
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
                .join_core(&sco_assertions_arr, |&c1, &p, &c2| Some((p, domain, c2)));

            // scm-dom2

            let scm_dom2 = dom_assertions.join_core(&spo_assertions_by_o_arr, |&p2, &c, &p1| {
                Some((p1, domain, c))
            });

            // scm-rng1

            let scm_rng1 = rng_assertions_by_o
                .join_core(&sco_assertions_arr, |&c1, &p, &c2| Some((p, range, c2)));

            // scm-rng2

            let scm_rng2 = rng_assertions.join_core(&spo_assertions_by_o_arr, |&p2, &c, &p1| {
                Some((p1, range, c))
            });

            // scm-hv

            let scm_hv_step_one =
                hv_assertions.join_core(&onp_assertions_arr, |&c1, &i, &p1| Some((i, (c1, p1))));

            let scm_hv_step_two = scm_hv_step_one
                .join_core(&hv_assertions_by_o, |&i, &(c1, p1), &c2| {
                    Some((c2, (p1, c1)))
                });

            let scm_hv_step_three = scm_hv_step_two
                .join_core(&onp_assertions_arr, |&c2, &(p1, c1), &p2| {
                    Some(((p1, p2), (c1, c2)))
                });

            let scm_hv = scm_hv_step_three
                .join_core(&spo_assertions_by_so, |&(p1, p2), &(c1, c2), _| {
                    Some((c1, subClassOf, c2))
                });

            // scm-svf1

            let scm_svf1_step_one =
                svf_assertions.join_core(&onp_assertions_arr, |&c1, &y1, &p| Some((y1, (c1, p))));

            let scm_svf1_step_two = scm_svf1_step_one
                .join_core(&sco_assertions_arr, |&y1, &(c1, p), &y2| {
                    Some((y2, (c1, p)))
                });

            let scm_svf1_step_three = scm_svf1_step_two
                .join_core(&svf_assertions_by_o, |&y2, &(c1, p), &c2| {
                    Some(((c2, p), c1))
                });

            let scm_svf1 = scm_svf1_step_three
                .join_core(&onp_assertions_by_so, |&(c2, p), &c1, _| {
                    Some((c1, subClassOf, c2))
                });

            // scm-svf2

            let scm_svf2_step_one =
                svf_assertions.join_core(&onp_assertions_arr, |&c1, &y, &p1| Some((y, (c1, p1))));

            let scm_svf2_step_two = scm_svf2_step_one
                .join_core(&svf_assertions_by_o, |&y, &(c1, p1), &(c2)| {
                    Some((c2, (p1, c1)))
                });

            let scm_svf2_step_three = scm_svf2_step_two
                .join_core(&onp_assertions_arr, |&c2, &(p1, c1), &p2| {
                    Some(((p1, p2), (c1, c2)))
                });

            let scm_svf2 = scm_svf2_step_three
                .join_core(&spo_assertions_by_so, |&(p1, p2), &(c1, c2), _| {
                    Some((c1, subClassOf, c2))
                });

            // scm-avf1

            let scm_avf1_step_one =
                avf_assertions.join_core(&onp_assertions_arr, |&c1, &y1, &p| Some((y1, (c1, p))));

            let scm_avf1_step_two = scm_avf1_step_one
                .join_core(&sco_assertions_arr, |&y1, &(c1, p), &y2| {
                    Some((y2, (c1, p)))
                });

            let scm_avf1_step_three = scm_avf1_step_two
                .join_core(&avf_assertions_by_o, |&y2, &(c1, p), &c2| {
                    Some(((c2, p), c1))
                });

            let scm_avf1 = scm_avf1_step_three
                .join_core(&onp_assertions_by_so, |&(c2, p), &c1, _| {
                    Some((c1, subClassOf, c2))
                });

            // scm-avf2

            let scm_avf2_step_one =
                avf_assertions.join_core(&onp_assertions_arr, |&c1, &y, &p1| Some((y, (c1, p1))));

            let scm_avf2_step_two = scm_avf2_step_one
                .join_core(&avf_assertions_by_o, |&y, &(c1, p1), &(c2)| {
                    Some((c2, (p1, c1)))
                });

            let scm_avf2_step_three = scm_avf2_step_two
                .join_core(&onp_assertions_arr, |&c2, &(p1, c1), &p2| {
                    Some(((p1, p2), (c1, c2)))
                });

            let scm_avf2 = scm_avf2_step_three
                .join_core(&spo_assertions_by_so, |&(p1, p2), &(c1, c2), _| {
                    Some((c2, subClassOf, c1))
                });

            let scm = scm_cls.concatenate(vec![
                scm_sco, scm_eqc1, scm_eqc2, scm_op, scm_spo, scm_eqp1, scm_eqp2, scm_dom1,
                scm_dom2, scm_rng1, scm_rng2, scm_hv, scm_svf1, scm_svf2, scm_avf1, scm_avf2,
            ]);

            tbox_var.set(&tbox.enter(&inner).concatenate(vec![eq, scm]));

            tbox_new.leave()
        })
        .concat(&tbox)
        .consolidate()
}

pub fn owl2rl_abox<'a>(
    tbox: &TripleCollection<'a>,
    abox: &TripleCollection<'a>,
) -> TripleCollection<'a> {
    let mut outer = abox.scope();

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

    let sas_assertions = tbox
        .filter(|(s, p, o)| *p == sameAs)
        .map(|(s, p, o)| (s, o));

    let abox_by_s = abox.map(|(s, p, o)| (s, (p, o))).arrange_by_key();
    let abox_by_p = abox.map(|(s, p, o)| (p, (s, o))).arrange_by_key();
    let abox_by_o = abox.map(|(s, p, o)| (o, (s, p))).arrange_by_key();

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

    let abox_by_sp = abox.map(|(s, p, o)| ((s, p), o)).arrange_by_key();

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

    let abox_by_so = abox.map(|(s, p, o)| ((s, o), p)).arrange_by_key();
    let abox_by_po = abox.map(|(s, p, o)| ((p, o), s)).arrange_by_key();

    outer
        .iterative::<usize, _, _>(|inner| {
            let abox_var = iterate::Variable::new(inner, Product::new(Default::default(), 1));

            let abox_new = abox_var.distinct();

            let abox = &abox.enter(inner);

            let sco_assertions = sco_assertions.enter(&inner);

            let eqc_assertions = eqc_assertions.enter(&inner);

            let type_assertions_by_o = type_assertions_by_o.enter(&inner);

            let sas_assertions = sas_assertions.enter(&inner);

            let abox_by_s = abox_by_s.enter(&inner);
            let abox_by_p = abox_by_p.enter(&inner);
            let abox_by_o = abox_by_o.enter(&inner);

            let dom_assertions = dom_assertions.enter(&inner);

            let rng_assertions = rng_assertions.enter(&inner);

            let symp_assertions = symp_assertions.enter(&inner);

            let trans_assertions = trans_assertions.enter(&inner);

            let spo_assertions = spo_assertions.enter(&inner);

            let eqp_assertions = eqp_assertions.enter(&inner);

            let inv_assertions = inv_assertions.enter(&inner);

            let abox_by_sp = abox_by_sp.enter(&inner);

            let svf_assertions = svf_assertions.enter(&inner);

            let avf_assertions = avf_assertions.enter(&inner);

            let op_assertions = op_assertions.enter(&inner);

            let hv_assertions = hv_assertions.enter(&inner);

            let abox_by_so = abox_by_so.enter(&inner);
            let abox_by_po = abox_by_po.enter(&inner);

            // cax-sco
            let cax_sco = sco_assertions
                .join_core(&type_assertions_by_o, |&c1, &c2, &x| Some((x, r#type, c2)));

            // cax-eqc1
            let cax_eqc1 = eqc_assertions
                .join_core(&type_assertions_by_o, |&c1, &c2, &x| Some((x, r#type, c2)));

            // cax-eqc2
            let cax_eqc2 = eqc_assertions
                .map(|(s, o)| (o, s))
                .join_core(&type_assertions_by_o, |&c2, &c1, &x| Some((x, r#type, c1)));

            let cax = cax_sco.concatenate(vec![cax_eqc1, cax_eqc2]);

            // eq-rep-s
            let eq_rep_s =
                sas_assertions.join_core(&abox_by_s, |&s, &s_prime, &(p, o)| Some((s_prime, p, o)));

            // eq-rep-p
            let eq_rep_p =
                sas_assertions.join_core(&abox_by_p, |&p, &p_prime, &(s, o)| Some((s, p_prime, o)));

            // eq-rep-o
            let eq_rep_o =
                sas_assertions.join_core(&abox_by_o, |&o, &o_prime, &(s, p)| Some((s, p, o_prime)));

            let eq = eq_rep_s.concatenate(vec![eq_rep_p, eq_rep_o]);

            // prp-dom

            let prp_dom =
                dom_assertions.join_core(&abox_by_p, |&p, &c, &(x, y)| Some((x, r#type, c)));

            // prp-rng

            let prp_rng =
                rng_assertions.join_core(&abox_by_p, |&p, &c, &(x, y)| Some((y, r#type, c)));

            // prp-symp

            let prp_symp = symp_assertions.join_core(&abox_by_p, |&p, _, &(x, y)| Some((y, p, x)));

            // prp-trp

            let prp_trp_step_one =
                trans_assertions.join_core(&abox_by_p, |&p, _, &(x, y)| Some(((p, y), x)));

            let prp_trp =
                prp_trp_step_one.join_core(&abox_by_sp, |&(p, y), &x, &z| Some((x, p, z)));

            // prp-spo1

            let prp_spo1 =
                spo_assertions.join_core(&abox_by_p, |&p1, &p2, &(x, y)| Some((x, p2, y)));

            // prp-eqp1

            let prp_eqp1 =
                eqp_assertions.join_core(&abox_by_p, |&p1, &p2, &(x, y)| Some((x, p2, y)));

            // prp-eqp2

            let prp_eqp2 = eqp_assertions
                .map(|(p1, p2)| (p2, p1))
                .join_core(&abox_by_p, |&p2, &p1, &(x, y)| Some((x, p1, y)));

            // prp-inv1

            let prp_inv1 =
                inv_assertions.join_core(&abox_by_p, |&p1, &p2, &(x, y)| Some((y, p2, x)));

            // prp-inv2

            let prp_inv2 = inv_assertions
                .map(|(p1, p2)| (p2, p1))
                .join_core(&abox_by_p, |&p2, &p1, &(x, y)| Some((y, p1, x)));

            let prp = prp_dom.concatenate(vec![
                prp_rng, prp_symp, prp_trp, prp_spo1, prp_eqp1, prp_eqp2, prp_inv1, prp_inv2,
            ]);

            // cls-svf1
            let cls_svf1_step_one =
                svf_assertions.join_core(&op_assertions, |&x, &y, &p| Some((p, (y, x))));

            let cls_svf1_step_two = cls_svf1_step_one
                .join_core(&abox_by_p, |&p, &(y, x), &(u, v)| Some(((v, y), (u, x))));

            let cls_svf1 = cls_svf1_step_two
                .join_core(&abox_by_so, |&(v, y), &(u, x), &_| Some((u, r#type, x)));

            // cls-svf2
            let cls_svf2_step_one = svf_assertions
                .filter(|(s, o)| *o == Thing)
                .join_core(&op_assertions, |&x, _, &p| Some((p, x)));

            let cls_svf2 =
                cls_svf2_step_one.join_core(&abox_by_p, |&p, &x, &(u, v)| Some((u, r#type, x)));

            // cls-avf
            let cls_avf_step_one =
                avf_assertions.join_core(&op_assertions, |&x, &y, &p| Some((x, (p, y))));

            let cls_avf_step_two = cls_avf_step_one
                .join_core(&type_assertions_by_o, |&x, &(p, y), &u| Some(((u, p), y)));

            let cls_avf =
                cls_avf_step_two.join_core(&abox_by_sp, |&(u, p), &y, &v| Some((v, r#type, y)));

            // cls-hv1
            let cls_hv1_step_one =
                hv_assertions.join_core(&op_assertions, |&x, &y, &p| Some((x, (p, y))));

            let cls_hv1 = cls_hv1_step_one
                .join_core(&type_assertions_by_o, |&x, &(p, y), &u| Some((u, p, y)));

            // cls-hv2
            let cls_hv2_step_one =
                hv_assertions.join_core(&op_assertions, |&x, &y, &p| Some(((p, y), x)));

            let cls_hv2 =
                cls_hv2_step_one.join_core(&abox_by_po, |&(p, y), &x, &u| Some((u, r#type, x)));

            // cls-thing
            let cls_thing = avf_assertions.map(|_| (Thing, r#type, Class)).consolidate();

            let cls = cls_svf1.concatenate(vec![cls_svf2, cls_avf, cls_hv1, cls_hv2, cls_thing]);

            abox_var.set(&abox.concatenate(vec![cax, eq, prp, cls]));

            abox_new.leave()
        })
        .concat(&abox)
        .consolidate()
}
