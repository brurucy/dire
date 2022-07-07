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

#[cfg(test)]
mod tests {
    use timely::communication::Config;

    use crate::materialization::common::tbox_spo_sco_materialization;
    use crate::materialization::rdfs::rdfs;
    use crate::model::consts::constants::owl::{
        inverseOf, Class, ObjectProperty, TransitiveProperty,
    };
    use crate::model::consts::constants::rdfs::{domain, r#type, range, subClassOf, subPropertyOf};
    use crate::model::consts::constants::MAX_CONST;
    use crate::reason::reason;

    #[test]
    fn rdfs_works() {
        let (tbox_output_sink, tbox_output_source) = flume::unbounded();
        let (tbox_input_sink, tbox_input_source) = flume::bounded(10);
        let (abox_output_sink, abox_output_source) = flume::unbounded();
        let (abox_input_sink, abox_input_source) = flume::bounded(10);
        let (done_sink, done_source) = flume::bounded(0);
        let (terminate_sink, terminate_source) = flume::bounded(0);
        let (log_sink, log_source) = flume::unbounded();
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

        terminate_sink.send("STOP".to_string()).unwrap();
        reason(
            timely::Config {
                communication: Config::Process(2),
                worker: Default::default(),
            },
            tbox_spo_sco_materialization,
            rdfs,
            tbox_input_source,
            abox_input_source,
            tbox_output_sink,
            abox_output_sink,
            done_sink,
            terminate_source,
            log_sink,
        );
        let mut actual_tbox_diffs: Vec<(u32, u32, u32)> = vec![];
        let mut actual_abox_diffs: Vec<(u32, u32, u32)> = vec![];

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
