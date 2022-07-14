use differential_dataflow::input::Input;

use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::{iterate, JoinCore, Threshold};
use timely::dataflow::Scope;
use timely::order::Product;

use crate::model::consts::constants::rdfs::{subClassOf, subPropertyOf};
use crate::model::types::{
    KeyedTripleCollection, ListCollection, TripleCollection, TupleCollection,
};

pub fn dummy_first_stage_materialization<'a>(
    collection: &TripleCollection<'a>,
) -> (TripleCollection<'a>, ListCollection<'a>) {
    let mut scope = collection.scope();
    let list = scope.new_collection_from(vec![(0, vec![0])]);
    (collection.clone(), list.1)
}

pub fn dummy_second_stage_materialization<'a>(
    _collection_one: &TripleCollection<'a>,
    _list_collection_one: &ListCollection<'a>,
    collection_two: &TripleCollection<'a>,
) -> TripleCollection<'a> {
    collection_two.clone()
}

pub fn tbox_spo_sco_materialization<'a>(
    tbox: &TripleCollection<'a>,
) -> (TripleCollection<'a>, ListCollection<'a>) {
    let mut outer = tbox.scope();
    let tbox = outer.region_named("Tbox transitive rules", |inn| {
        let tbox = tbox.enter(inn);

        let tbox_by_o = tbox.map(|(s, p, o)| (o, (p, s)));
        let tbox_by_s = tbox.map(|(s, p, o)| (s, (p, o)));

        let sco_ass_by_o = tbox_by_o.filter(|(_, (p, _))| *p == subClassOf);
        let sco_ass_by_s = sco_ass_by_o.map(|(o, (p, s))| (s, (p, o)));

        let spo_ass_by_o = tbox_by_o.filter(|(_, (p, _))| *p == subPropertyOf);
        let spo_ass_by_s = spo_ass_by_o.map(|(o, (p, s))| (s, (p, o)));

        let (spo, sco) = inn.iterative::<usize, _, _>(|inner| {
            let sco_var = iterate::Variable::new(inner, Product::new(Default::default(), 1));
            let spo_var = iterate::Variable::new(inner, Product::new(Default::default(), 1));

            let sco_new = sco_var.distinct();
            let spo_new = spo_var.distinct();

            let sco_new_arr = sco_new.arrange_by_key();
            let spo_new_arr = spo_new.arrange_by_key();

            let sco_ass_by_s = sco_ass_by_s.enter(inner);
            let spo_ass_by_s = spo_ass_by_s.enter(inner);

            let sco_ass_by_o = sco_ass_by_o.enter(inner);
            let spo_ass_by_o = spo_ass_by_o.enter(inner);

            let sco_ass_by_o_arr = sco_ass_by_o.arrange_by_key();
            let spo_ass_by_o_arr = spo_ass_by_o.arrange_by_key();

            let sco_iter_step = sco_ass_by_o_arr
                .join_core(&sco_new_arr, |&_, &(p, s), &(_, o_prime)| {
                    Some((s, (p, o_prime)))
                });

            let spo_iter_step = spo_ass_by_o_arr
                .join_core(&spo_new_arr, |&_, &(p, s), &(_, o_prime)| {
                    Some((s, (p, o_prime)))
                });

            sco_var.set(&sco_ass_by_s.concat(&sco_iter_step));
            spo_var.set(&spo_ass_by_s.concat(&spo_iter_step));

            (sco_new.leave(), spo_new.leave())
        });

        tbox_by_s
            .concat(&sco)
            .concat(&spo)
            .map(|(s, (p, o))| (s, p, o))
            .leave()
    });
    (tbox, outer.new_collection_from(vec![(0, vec![0])]).1)
}

pub fn abox_sco_type_materialization<'a>(
    tbox_sco_assertions: &KeyedTripleCollection<'a>,
    abox_class_assertions: &KeyedTripleCollection<'a>,
) -> KeyedTripleCollection<'a> {
    let mut outer = tbox_sco_assertions.scope();
    outer.region_named("CAX-SCO", |inn| {
        let sco_assertions = tbox_sco_assertions.enter(inn);
        let class_assertions = abox_class_assertions.enter(inn);

        let class_assertions_arranged = class_assertions.arrange_by_key();

        sco_assertions
            .join_core(
                &class_assertions_arranged,
                |_key, &(_sco, y), &(z, type_)| Some((y, (z, type_))),
            )
            .leave()
    })
}

pub fn abox_domain_and_range_type_materialization<'a>(
    domain_assertions: &TupleCollection<'a>,
    range_assertions: &TupleCollection<'a>,
    property_assertions_by_p: &KeyedTripleCollection<'a>,
) -> (TupleCollection<'a>, TupleCollection<'a>) {
    let property_assertions_by_p_arr = property_assertions_by_p.arrange_by_key();

    let domain_type = domain_assertions
        .join_core(&property_assertions_by_p_arr, |&_a, &x, &(y, _z)| {
            Some((x, y))
        });

    let range_type = range_assertions
        .join_core(&property_assertions_by_p_arr, |_a, &x, &(_y, z)| {
            Some((x, z))
        });

    (domain_type, range_type)
}
