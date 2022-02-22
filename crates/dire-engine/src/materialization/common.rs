use crate::model::consts::constants::rdfs::{r#type, subClassOf, subPropertyOf};
use crate::model::types::{KeyedTriple, KeyedTripleCollection, Triple, TripleCollection};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::{iterate, JoinCore, Threshold};
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::order::Product;

pub fn dummy_unary_materialization<'a>(collection: &TripleCollection<'a>) -> TripleCollection<'a> {
    collection.clone()
}

pub fn dummy_binary_materialization<'a>(
    collection_one: &TripleCollection<'a>,
    collection_two: &TripleCollection<'a>,
) -> TripleCollection<'a> {
    collection_two.clone()
}

pub fn tbox_spo_sco_materialization<'a>(tbox: &TripleCollection<'a>) -> TripleCollection<'a> {
    let mut outer = tbox.scope();
    outer.region_named("Tbox transitive rules", |inn| {
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
    })
}

// This is indirectly tested on `rdfs.rs` and `rdfspp.rs`
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

// This is indirectly tested on `rdfs.rs` and `rdfspp.rs`
pub fn abox_domain_and_range_type_materialization<'a>(
    tbox_domain_assertions: &KeyedTripleCollection<'a>,
    tbox_range_assertions: &KeyedTripleCollection<'a>,
    abox_property_assertions: &KeyedTripleCollection<'a>,
) -> (KeyedTripleCollection<'a>, KeyedTripleCollection<'a>) {
    let mut outer = tbox_domain_assertions.scope();
    outer.region_named("Domain and Range type rules", |inner| {
        let property_assertions = abox_property_assertions.enter(inner);

        let p_s_arr = property_assertions
            .map(|(p, (s, _o))| (p, s))
            .distinct()
            .arrange_by_key_named("Arrange (p, s) for PRP-DOM");

        let p_o_arr = property_assertions
            .map(|(p, (_s, o))| (p, o))
            .distinct()
            .arrange_by_key_named("Arrange (p, o) for PRP-RNG");

        let domain_assertions = tbox_domain_assertions.enter(inner);

        let domain_type =
            domain_assertions.join_core(&p_s_arr, |_p, &(_, x), &y| Some((x, (y, r#type))));

        let range_assertions = tbox_range_assertions.enter(inner);

        let range_type =
            range_assertions.join_core(&p_o_arr, |_p, &(_, x), &z| Some((x, (z, r#type))));

        (domain_type.leave(), range_type.leave())
    })
}

#[cfg(test)]
mod tests {
    use crate::materialization::common::{
        dummy_binary_materialization, tbox_spo_sco_materialization,
    };
    use crate::model::consts::constants::rdfs::{subClassOf, subPropertyOf};
    use crate::model::consts::constants::MAX_CONST;
    use crate::reason::reason;
    use timely::communication::Config;

    #[test]
    fn tbox_spo_sco_materialization_works() {
        let (tbox_output_sink, tbox_output_source) = flume::unbounded();
        let (tbox_input_sink, tbox_input_source) = flume::bounded(4);
        let (abox_output_sink, abox_output_source) = flume::bounded(2);
        let (abox_input_sink, abox_input_source) = flume::bounded(2);
        let (termination_sink, termination_source) = flume::bounded(1);
        let professor = MAX_CONST + 1;
        let employee = MAX_CONST + 2;
        let tax_payer = MAX_CONST + 3;
        tbox_input_sink
            .send(((professor, subClassOf, employee), 1))
            .unwrap();
        tbox_input_sink
            .send(((employee, subClassOf, tax_payer), 1))
            .unwrap();
        let head_of = MAX_CONST + 4;
        let works_for = MAX_CONST + 5;
        let member_of = MAX_CONST + 6;
        tbox_input_sink
            .send(((head_of, subPropertyOf, works_for), 1))
            .unwrap();
        tbox_input_sink
            .send(((works_for, subPropertyOf, member_of), 1))
            .unwrap();
        termination_sink.send(()).unwrap();
        reason(
            timely::Config {
                communication: Config::Process(2),
                worker: Default::default(),
            },
            tbox_spo_sco_materialization,
            dummy_binary_materialization,
            tbox_input_source,
            abox_input_source,
            tbox_output_sink,
            abox_output_sink,
            termination_source,
        );
        let mut actual_tbox_diffs: Vec<((u32, u32, u32))> = vec![];

        while let Ok(diff) = tbox_output_source.try_recv() {
            actual_tbox_diffs.push(diff.0)
        }

        actual_tbox_diffs.sort_unstable();
        actual_tbox_diffs.dedup();

        let mut expected_tbox_diffs = vec![
            (professor, subClassOf, employee),
            (employee, subClassOf, tax_payer),
            (professor, subClassOf, tax_payer),
            (head_of, subPropertyOf, works_for),
            (works_for, subPropertyOf, member_of),
            (head_of, subPropertyOf, member_of),
        ];

        expected_tbox_diffs.sort_unstable();
        expected_tbox_diffs.dedup();

        assert_eq!(expected_tbox_diffs, actual_tbox_diffs)
    }
}