use crate::materialization::common::abox_domain_and_range_type_materialization;
use crate::model::consts::constants::owl::{inverseOf, TransitiveProperty};
use crate::model::consts::constants::rdfs::{domain, r#type, range, subClassOf, subPropertyOf};
use crate::model::types::{ListCollection, TripleCollection};
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::{Consolidate, JoinCore, Threshold};
use timely::dataflow::Scope;
use timely::order::Product;

pub fn rdfspp<'a>(
    tbox: &TripleCollection<'a>,
    _lists: &ListCollection<'a>,
    abox: &TripleCollection<'a>,
) -> TripleCollection<'a> {
    let sco_assertions = tbox
        .filter(|(_s, p, _o)| *p == subClassOf)
        .map(|(s, _p, o)| (s, o));
    let spo_assertions = tbox
        .filter(|(_s, p, _o)| *p == subPropertyOf)
        .map(|(s, _p, o)| (s, o));
    let domain_assertions = tbox
        .filter(|(_s, p, _o)| *p == domain)
        .map(|(s, _p, o)| (s, o));
    let range_assertions = tbox
        .filter(|(_s, p, _o)| *p == range)
        .map(|(s, _p, o)| (s, o));
    let general_trans_assertions = tbox
        .filter(|(_s, _p, o)| *o == TransitiveProperty)
        .map(|(s, _p, _o)| (s, s));
    let inverse_of_assertions = tbox
        .filter(|(_s, p, _o)| *p == inverseOf)
        .map(|(s, _p, o)| (s, o));
    let inverse_of_assertions_by_o = inverse_of_assertions.map(|(s, o)| (o, s));

    let type_assertions = abox
        .filter(|(_s, p, _o)| *p == r#type)
        .map(|(s, _p, o)| (s, o));

    let type_assertions_by_o = type_assertions.map(|(s, o)| (o, s));

    let property_assertions_by_p = abox
        .map(|(s, p, o)| (p, (s, o)))
        .filter(|(p, (_s, _o))| *p != r#type);

    let mut outer = tbox.scope();

    let property_materialization = outer.iterative::<usize, _, _>(|inner| {
        let spo_type_gen_trans_inv_var = Variable::new(inner, Product::new(Default::default(), 1));

        let spo_type_gen_trans_inv_new = spo_type_gen_trans_inv_var.distinct();

        let spo_type_gen_trans_inv_arr = spo_type_gen_trans_inv_new.arrange_by_key();

        let spo_assertions = spo_assertions.enter(inner);
        let general_trans_assertions = general_trans_assertions.enter(inner);

        let inverse_of_assertions = inverse_of_assertions.enter(&inner);
        let inverse_of_assertions_by_o = inverse_of_assertions_by_o.enter(&inner);

        let spo_iter_step = spo_assertions
            .join_core(&spo_type_gen_trans_inv_arr, |_a, &b, &(x, y)| {
                Some((b, (x, y)))
            });

        let left_inverse_only_iter_step = inverse_of_assertions
            .join_core(&spo_type_gen_trans_inv_arr, |&_, &p1, &(s, o)| {
                Some((p1, (o, s)))
            });

        let right_inverse_only_iter_step = inverse_of_assertions_by_o
            .join_core(&spo_type_gen_trans_inv_arr, |&_, &p0, &(o, s)| {
                Some((p0, (s, o)))
            });

        let trans_p_only = general_trans_assertions
            .join_core(&spo_type_gen_trans_inv_arr, |&p, _, &(s, o)| {
                Some(((s, p), o))
            });

        let trans_p_only_reverse = trans_p_only.map(|((s, p), o)| ((o, p), s)).arrange_by_key();

        let trans_p_only_arr = trans_p_only.arrange_by_key();

        let gen_trans_iter_step = trans_p_only_reverse
            .join_core(&trans_p_only_arr, |&(_o, p), &s, &o_prime| {
                Some((p, (s, o_prime)))
            });

        spo_type_gen_trans_inv_var.set(&property_assertions_by_p.enter(inner).concatenate(vec![
            spo_iter_step,
            gen_trans_iter_step,
            left_inverse_only_iter_step,
            right_inverse_only_iter_step,
        ]));

        spo_type_gen_trans_inv_new.leave()
    });

    let property_assertions_by_p = property_materialization.concat(&property_assertions_by_p);

    let property_assertions = property_assertions_by_p.map(|(p, (x, y))| (x, p, y));

    let (rdfs2, rdfs3) = abox_domain_and_range_type_materialization(
        &domain_assertions,
        &range_assertions,
        &property_assertions_by_p,
    );

    let type_assertions_by_o = type_assertions_by_o.concatenate(vec![rdfs2, rdfs3]);
    let type_assertions_by_o_arr = type_assertions_by_o.arrange_by_key();

    let type_assertions = sco_assertions
        .join_core(&type_assertions_by_o_arr, |&_x, &y, &z| Some((y, z)))
        .concat(&type_assertions_by_o)
        .map(|(o, s)| (s, r#type, o));

    abox.concat(&property_assertions)
        .concat(&type_assertions)
        .consolidate()
}
