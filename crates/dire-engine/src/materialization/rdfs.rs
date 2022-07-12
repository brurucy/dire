use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::{Consolidate, JoinCore};

use crate::materialization::common::abox_domain_and_range_type_materialization;
use crate::model::consts::constants::rdfs::{domain, r#type, range, subClassOf, subPropertyOf};
use crate::model::types::{ListCollection, TripleCollection};

pub fn rdfs<'a>(
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

    let type_assertions = abox.filter(|(_s, p, _o)| *p == r#type);
    let type_assertions_by_o = type_assertions.map(|(s, _p, o)| (o, s));

    let property_assertions = abox.filter(|(_s, p, _o)| *p != r#type);
    let property_assertions_by_p = property_assertions.map(|(s, p, o)| (p, (s, o)));
    let property_assertions_by_p_arr = property_assertions_by_p.arrange_by_key();

    let rdfs7 = spo_assertions.join_core(&property_assertions_by_p_arr, |&_a, &b, &(x, y)| {
        Some((b, (x, y)))
    });

    let property_assertions_by_p = rdfs7.concat(&property_assertions_by_p);
    let property_assertions = property_assertions_by_p.map(|(p, (s, o))| (s, p, o));

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
