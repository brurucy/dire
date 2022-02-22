use crate::materialization::common::{
    abox_domain_and_range_type_materialization, abox_sco_type_materialization,
    tbox_spo_sco_materialization,
};
use crate::model::consts::constants::owl::{inverseOf, TransitiveProperty};
use crate::model::consts::constants::rdfs::{domain, r#type, range, subClassOf, subPropertyOf};
use crate::model::types::TripleCollection;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::{iterate, Consolidate, JoinCore, Threshold};
use timely::dataflow::Scope;
use timely::order::Product;

pub fn rdfspp<'a>(
    tbox: &TripleCollection<'a>,
    abox: &TripleCollection<'a>,
) -> TripleCollection<'a> {
    let tbox = tbox.map(|(s, p, o)| (s, (p, o)));

    let sco_assertions = tbox.filter(|(_s, (p, _o))| *p == subClassOf);
    let spo_assertions = tbox.filter(|(_s, (p, _o))| *p == subPropertyOf);
    let domain_assertions = tbox.filter(|(_s, (p, _o))| *p == domain);
    let range_assertions = tbox.filter(|(_s, (p, _o))| *p == range);
    let general_trans_assertions = tbox.filter(|(_s, (_p, o))| *o == TransitiveProperty);
    let inverse_of_assertions = tbox.filter(|(_s, (p, _o))| *p == inverseOf);

    let type_assertions = abox
        .map(|(s, p, o)| (o, (s, p)))
        .filter(|(_o, (_s, p))| *p == r#type);

    let not_type_assertions = abox
        .map(|(s, p, o)| (p, (s, o)))
        .filter(|(p, (_s, _o))| *p != r#type);

    let mut outer = tbox.scope();

    let property_materialization = outer.region_named("Abox transitive property rules", |inn| {
        let property_assertions = not_type_assertions.enter(inn);
        let spo_assertions = spo_assertions.enter(inn);
        let general_trans_assertions = general_trans_assertions.enter(inn);
        let inverse_of_assertions = inverse_of_assertions.enter(inn);

        let property_materialization = inn.iterative::<usize, _, _>(|inner| {
            let spo_type_gen_trans_inv_var =
                iterate::SemigroupVariable::new(inner, Product::new(Default::default(), 1));

            let spo_type_gen_trans_inv_new = spo_type_gen_trans_inv_var.distinct();

            let spo_type_gen_trans_inv_arr = spo_type_gen_trans_inv_new.arrange_by_key();

            let spo_ass = spo_assertions.enter(inner);
            let gen_trans_ass = general_trans_assertions.enter(inner);
            let inverse_ass = inverse_of_assertions.enter(inner);

            let left_inverse_ass = inverse_ass.clone();
            let right_inverse_ass = inverse_ass.map(|(s, (p, o))| (o, (p, s))).arrange_by_key();

            let spo_iter_step = spo_ass
                .join_core(&spo_type_gen_trans_inv_arr, |_key, &(_spo, b), &(x, y)| {
                    Some((b, (x, y)))
                });

            let left_inverse_only_iter_step = left_inverse_ass
                .join_core(&spo_type_gen_trans_inv_arr, |&_, &(_, p1), &(s, o)| {
                    Some((p1, (o, s)))
                });

            let right_inverse_only_iter_step = right_inverse_ass
                .join_core(&spo_type_gen_trans_inv_arr, |&_, &(_, p0), &(o, s)| {
                    Some((p0, (s, o)))
                });

            let trans_p_only = gen_trans_ass.join_core(
                &spo_type_gen_trans_inv_arr,
                |&p, &(_type_kw, _trans_kw), &(s, o)| Some(((s, p), o)),
            );

            let trans_p_only_reverse = trans_p_only.map(|((s, p), o)| ((o, p), s)).arrange_by_key();

            let trans_p_only_arr = trans_p_only.arrange_by_key();

            let gen_trans_iter_step = trans_p_only_reverse
                .join_core(&trans_p_only_arr, |&(_o, p), &s, &o_prime| {
                    Some((p, (s, o_prime)))
                });

            spo_type_gen_trans_inv_var.set(&property_assertions.enter(inner).concatenate(vec![
                spo_iter_step,
                gen_trans_iter_step,
                left_inverse_only_iter_step,
                right_inverse_only_iter_step,
            ]));

            spo_type_gen_trans_inv_new.leave()
        });

        property_materialization.leave()
    });

    let property_assertions = property_materialization.concat(&not_type_assertions);

    let (domain_type, range_type) = abox_domain_and_range_type_materialization(
        &domain_assertions,
        &range_assertions,
        &property_assertions,
    );

    let class_assertions = type_assertions
        .concatenate(vec![domain_type, range_type])
        .consolidate();

    let class_materialization = abox_sco_type_materialization(&sco_assertions, &class_assertions);

    let class_assertions = class_assertions.concat(&class_materialization);

    outer.region_named("Concatenating all rules", |inner| {
        let abox = abox.enter(inner);

        let property_assertions = property_assertions
            .enter(inner)
            .map(|(p, (x, y))| (x, p, y));

        let class_assertions = class_assertions.enter(inner).map(|(y, (x, p))| (x, p, y));

        abox.concat(&property_assertions)
            .concat(&class_assertions)
            .consolidate()
            .leave()
    })
}

#[cfg(test)]
mod tests {
    use crate::materialization::common::tbox_spo_sco_materialization;
    use crate::materialization::rdfspp::rdfspp;
    use crate::model::consts::constants::owl::{
        inverseOf, Class, ObjectProperty, TransitiveProperty,
    };
    use crate::model::consts::constants::rdfs::{domain, r#type, range, subClassOf, subPropertyOf};
    use crate::model::consts::constants::MAX_CONST;
    use crate::reason::reason;
    use timely::communication::Config;

    #[test]
    fn rdfspp_works() {
        let (tbox_output_sink, tbox_output_source) = flume::unbounded();
        let (tbox_input_sink, tbox_input_source) = flume::bounded(18);
        let (abox_output_sink, abox_output_source) = flume::unbounded();
        let (abox_input_sink, abox_input_source) = flume::bounded(6);
        let (termination_sink, termination_source) = flume::bounded(1);
        // Filling the tbox
        let employee = MAX_CONST + 1;
        let faculty = MAX_CONST + 2;
        let full_professor = MAX_CONST + 3;
        let professor = MAX_CONST + 4;
        let head_of = MAX_CONST + 5;
        let member_of = MAX_CONST + 6;
        let works_for = MAX_CONST + 7;
        let employs = MAX_CONST + 8;
        let teacher_of = MAX_CONST + 9;
        let course = MAX_CONST + 10;
        tbox_input_sink
            .send(((employee, r#type, Class), 1))
            .unwrap();
        tbox_input_sink.send(((faculty, r#type, Class), 1)).unwrap();
        tbox_input_sink
            .send(((faculty, subClassOf, employee), 1))
            .unwrap();
        tbox_input_sink
            .send(((full_professor, r#type, Class), 1))
            .unwrap();
        tbox_input_sink
            .send(((full_professor, subClassOf, professor), 1))
            .unwrap();
        tbox_input_sink
            .send(((professor, r#type, Class), 1))
            .unwrap();
        tbox_input_sink
            .send(((professor, subClassOf, faculty), 1))
            .unwrap();
        tbox_input_sink
            .send(((head_of, r#type, ObjectProperty), 1))
            .unwrap();
        tbox_input_sink
            .send(((head_of, subPropertyOf, works_for), 1))
            .unwrap();
        tbox_input_sink
            .send(((member_of, r#type, ObjectProperty), 1))
            .unwrap();
        tbox_input_sink
            .send(((works_for, r#type, ObjectProperty), 1))
            .unwrap();
        tbox_input_sink
            .send(((works_for, subPropertyOf, member_of), 1))
            .unwrap();
        tbox_input_sink
            .send(((works_for, r#type, TransitiveProperty), 1))
            .unwrap();
        tbox_input_sink
            .send(((employs, r#type, ObjectProperty), 1))
            .unwrap();
        tbox_input_sink
            .send(((employs, inverseOf, works_for), 1))
            .unwrap();
        tbox_input_sink
            .send(((teacher_of, r#type, ObjectProperty), 1))
            .unwrap();
        tbox_input_sink
            .send(((teacher_of, domain, faculty), 1))
            .unwrap();
        tbox_input_sink
            .send(((teacher_of, range, course), 1))
            .unwrap();
        // Filling the abox
        let full_professor_7 = MAX_CONST + 11;
        let full_professor_8 = MAX_CONST + 12;
        let full_professor_9 = MAX_CONST + 13;
        let full_professor_10 = MAX_CONST + 14;
        let department_0 = MAX_CONST + 15;
        let course_10 = MAX_CONST + 16;
        abox_input_sink
            .send(((full_professor_7, head_of, department_0), 1))
            .unwrap();
        abox_input_sink
            .send(((full_professor_7, r#type, full_professor), 1))
            .unwrap();
        abox_input_sink
            .send(((full_professor_7, teacher_of, course_10), 1))
            .unwrap();
        abox_input_sink
            .send(((full_professor_7, works_for, full_professor_8), 1))
            .unwrap();
        abox_input_sink
            .send(((full_professor_8, works_for, full_professor_9), 1))
            .unwrap();
        abox_input_sink
            .send(((full_professor_9, works_for, full_professor_10), 1))
            .unwrap();

        termination_sink.send(()).unwrap();
        reason(
            timely::Config {
                communication: Config::Process(2),
                worker: Default::default(),
            },
            tbox_spo_sco_materialization,
            rdfspp,
            tbox_input_source,
            abox_input_source,
            tbox_output_sink,
            abox_output_sink,
            termination_source,
        );
        let mut actual_tbox_diffs: Vec<((u32, u32, u32))> = vec![];
        let mut actual_abox_diffs: Vec<((u32, u32, u32))> = vec![];

        while let Ok(diff) = tbox_output_source.try_recv() {
            actual_tbox_diffs.push(diff.0)
        }

        while let Ok(diff) = abox_output_source.try_recv() {
            actual_abox_diffs.push(diff.0)
        }

        actual_abox_diffs.sort_unstable();
        actual_abox_diffs.dedup();

        actual_tbox_diffs.sort_unstable();
        actual_tbox_diffs.dedup();

        let mut expected_tbox_diffs = vec![
            // Input
            (employee, r#type, Class),
            (faculty, r#type, Class),
            (faculty, subClassOf, employee),
            (full_professor, r#type, Class),
            (full_professor, subClassOf, professor),
            (professor, r#type, Class),
            (professor, subClassOf, faculty),
            (head_of, r#type, ObjectProperty),
            (head_of, subPropertyOf, works_for),
            (member_of, r#type, ObjectProperty),
            (works_for, r#type, ObjectProperty),
            (works_for, subPropertyOf, member_of),
            (works_for, r#type, TransitiveProperty),
            (employs, r#type, ObjectProperty),
            (employs, inverseOf, works_for),
            (teacher_of, r#type, ObjectProperty),
            (teacher_of, domain, faculty),
            (teacher_of, range, course),
            // Materialization
            (professor, subClassOf, employee),
            (full_professor, subClassOf, faculty),
            (full_professor, subClassOf, employee),
            (head_of, subPropertyOf, member_of),
        ];

        let mut expected_abox_diffs = vec![
            // Input
            (full_professor_7, head_of, department_0),
            (full_professor_7, r#type, full_professor),
            (full_professor_7, teacher_of, course_10),
            (full_professor_7, works_for, full_professor_8),
            (full_professor_8, works_for, full_professor_9),
            (full_professor_9, works_for, full_professor_10),
            // Materialization
            (full_professor_7, works_for, department_0),
            (full_professor_7, member_of, department_0),
            (full_professor_7, member_of, full_professor_8),
            (full_professor_8, member_of, full_professor_9),
            (full_professor_9, member_of, full_professor_10),
            (full_professor_7, r#type, professor),
            (full_professor_7, r#type, faculty),
            (full_professor_7, r#type, employee),
            (full_professor_7, works_for, full_professor_9),
            (full_professor_7, works_for, full_professor_10),
            (full_professor_8, works_for, full_professor_10),
            (full_professor_7, member_of, full_professor_9),
            (full_professor_7, member_of, full_professor_10),
            (full_professor_8, member_of, full_professor_10),
            (department_0, employs, full_professor_7),
            (full_professor_10, employs, full_professor_9),
            (full_professor_10, employs, full_professor_8),
            (full_professor_10, employs, full_professor_7),
            (full_professor_9, employs, full_professor_7),
            (full_professor_9, employs, full_professor_8),
            (full_professor_8, employs, full_professor_7),
            (course_10, r#type, course),
        ];

        expected_abox_diffs.sort_unstable();
        expected_abox_diffs.dedup();

        expected_tbox_diffs.sort_unstable();
        expected_tbox_diffs.dedup();

        assert_eq!(expected_tbox_diffs, actual_tbox_diffs);
        assert_eq!(expected_abox_diffs, actual_abox_diffs)
    }
}