use std::thread;

use crate::materialization::common::{
    dummy_first_stage_materialization, dummy_second_stage_materialization,
    tbox_spo_sco_materialization,
};
use crate::materialization::owl2rl::{owl2rl_abox, owl2rl_tbox};
use crate::materialization::rdfs::rdfs;
use crate::materialization::rdfspp::rdfspp;
use crate::model::types::{TerminationSink, TripleInputSink, TripleOutputSource};
use crate::reason::reason;

pub enum Engine {
    RDFS,
    RDFSpp,
    OWL2RL,
    Dummy,
}

pub fn entrypoint(
    cfg: timely::Config,
    batch_size: usize,
    logic: Engine,
) -> (
    TripleInputSink,
    TripleInputSink,
    TripleOutputSource,
    TripleOutputSource,
    TerminationSink,
    std::thread::JoinHandle<()>,
) {
    let (tbox_output_sink, tbox_output_source) = flume::unbounded();
    let (tbox_input_sink, tbox_input_source) = flume::bounded(batch_size);
    let (abox_output_sink, abox_output_source) = flume::unbounded();
    let (abox_input_sink, abox_input_source) = flume::bounded(batch_size);
    let (termination_sink, termination_source) = flume::bounded(1);

    let join_handle = thread::spawn(move || {
        let tbox_materialization = match logic {
            Engine::Dummy => dummy_first_stage_materialization,
            Engine::OWL2RL => owl2rl_tbox,
            _ => tbox_spo_sco_materialization,
        };
        let abox_materialization = match logic {
            Engine::RDFS => rdfs,
            Engine::RDFSpp => rdfspp,
            Engine::OWL2RL => owl2rl_abox,
            Engine::Dummy => dummy_second_stage_materialization,
        };
        reason(
            cfg,
            tbox_materialization,
            abox_materialization,
            tbox_input_source,
            abox_input_source,
            tbox_output_sink,
            abox_output_sink,
            termination_source,
        );
    });

    (
        tbox_input_sink,
        abox_input_sink,
        tbox_output_source,
        abox_output_source,
        termination_sink,
        join_handle,
    )
}

mod tests {
    use crate::entrypoint::{entrypoint, Engine};
    use crate::model::consts::constants::owl::{
        inverseOf, Class, ObjectProperty, TransitiveProperty,
    };
    use crate::model::consts::constants::rdfs::{domain, r#type, range, subClassOf, subPropertyOf};
    use crate::model::consts::constants::MAX_CONST;
    use std::time::Duration;

    #[test]
    fn entrypoint_rdfs_works() {
        let single_threaded = timely::Config::process(1);
        let (
            tbox_input_sink,
            abox_input_sink,
            tbox_output_source,
            abox_output_source,
            termination_source,
            _joinhandle,
        ) = entrypoint(single_threaded, 1, Engine::RDFS);
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

        termination_source.send(()).unwrap();

        let mut actual_tbox_diffs: Vec<(u32, u32, u32)> = vec![];
        let mut actual_abox_diffs: Vec<(u32, u32, u32)> = vec![];

        while let Ok(diff) = tbox_output_source.recv_timeout(Duration::from_millis(50)) {
            actual_tbox_diffs.push(diff.0)
        }

        while let Ok(diff) = abox_output_source.recv_timeout(Duration::from_millis(50)) {
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
    #[test]
    fn entrypoint_rdfspp_works() {
        let single_threaded = timely::Config::process(1);
        let (
            tbox_input_sink,
            abox_input_sink,
            tbox_output_source,
            abox_output_source,
            termination_source,
            _joinhandle,
        ) = entrypoint(single_threaded, 1, Engine::RDFSpp);
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

        termination_source.send(()).unwrap();

        let mut actual_tbox_diffs: Vec<(u32, u32, u32)> = vec![];
        let mut actual_abox_diffs: Vec<(u32, u32, u32)> = vec![];

        while let Ok(diff) = tbox_output_source.recv_timeout(Duration::from_millis(50)) {
            actual_tbox_diffs.push(diff.0)
        }

        while let Ok(diff) = abox_output_source.recv_timeout(Duration::from_millis(50)) {
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
    #[test]
    fn entrypoint_dummy_works() {
        let single_threaded = timely::Config::process(1);
        let (
            tbox_input_sink,
            abox_input_sink,
            tbox_output_source,
            abox_output_source,
            termination_source,
            _joinhandle,
        ) = entrypoint(single_threaded, 1, Engine::Dummy);
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

        termination_source.send(()).unwrap();

        let mut actual_tbox_diffs: Vec<(u32, u32, u32)> = vec![];
        let mut actual_abox_diffs: Vec<(u32, u32, u32)> = vec![];

        while let Ok(diff) = tbox_output_source.recv_timeout(Duration::from_millis(50)) {
            actual_tbox_diffs.push(diff.0)
        }

        while let Ok(diff) = abox_output_source.recv_timeout(Duration::from_millis(50)) {
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
        ];

        let mut expected_abox_diffs = vec![
            // Input
            (full_professor_7, head_of, department_0),
            (full_professor_7, r#type, full_professor),
            (full_professor_7, teacher_of, course_10),
            (full_professor_7, works_for, full_professor_8),
            (full_professor_8, works_for, full_professor_9),
            (full_professor_9, works_for, full_professor_10),
        ];

        expected_abox_diffs.sort_unstable();
        expected_abox_diffs.dedup();

        expected_tbox_diffs.sort_unstable();
        expected_tbox_diffs.dedup();

        assert_eq!(expected_tbox_diffs, actual_tbox_diffs);
        assert_eq!(expected_abox_diffs, actual_abox_diffs)
    }
}
