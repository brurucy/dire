#[allow(non_upper_case_globals)]
pub mod constants {
    pub const MAX_CONST: u32 = 45;

    pub mod rdfs {
        //                 let _rdfsco: &str = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
        pub const subClassOf: u32 = 0;
        //                 let _rdfspo: &str = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
        pub const subPropertyOf: u32 = 1;
        //                 let _rdfsd: &str = "<http://www.w3.org/2000/01/rdf-schema#domain>";
        pub const domain: u32 = 2;
        //                 let _rdfsr: &str = "<http://www.w3.org/2000/01/rdf-schema#range>";
        pub const range: u32 = 3;
        //                 let _rdft: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
        pub const r#type: u32 = 4;
        //                 let _rdfcomment: &str = "<http://www.w3.org/2000/01/rdf-schema#comment>";
        pub const comment: u32 = 5;
        //                 let _rdfrest: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
        pub const rest: u32 = 6;
        //                 let _rdffirst: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
        pub const first: u32 = 7;
        //                 let _rdfl: &str = "<http://www.w3.org/2000/01/rdf-schema#label>";
        pub const label: u32 = 8;
        //                 let _rdfn: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
        pub const nil: u32 = 9;
        //                 let _rdfl: &str = "<http://www.w3.org/2000/01/rdf-schema#Literal>";
        pub const Literal: u32 = 10;
    }

    pub mod owl {
        //                 let _owltr: &str = "<http://www.w3.org/2002/07/owl#TransitiveProperty>";
        pub const TransitiveProperty: u32 = 11;
        //                 let _owlio: &str = "<http://www.w3.org/2002/07/owl#inverseOf>";
        pub const inverseOf: u32 = 12;
        //                 let _owlthing: &str = "<http://www.w3.org/2002/07/owl#Thing>";
        pub const Thing: u32 = 13;
        //                 let _owlmqc: &str = "<http://www.w3.org/2002/07/owl#maxQualifiedCardinality>";
        pub const maxQualifiedCardinality: u32 = 14;
        //                 let _owlsvf: &str = "<http://www.w3.org/2002/07/owl#someValuesFrom>";
        pub const someValuesFrom: u32 = 15;
        //                 let _owlec: &str = "<http://www.w3.org/2002/07/owl#equivalentClass>";
        pub const equivalentClass: u32 = 16;
        //                 let _owlito: &str = "<http://www.w3.org/2002/07/owl#intersectionOf>";
        pub const intersectionOf: u32 = 17;
        //                 let _owlm: &str = "<http://www.w3.org/2002/07/owl#members>";
        pub const members: u32 = 18;
        //                 let _owlep: &str = "<http://www.w3.org/2002/07/owl#equivalentProperty>";
        pub const equivalentProperty: u32 = 19;
        //                 let _owlop: &str = "<http://www.w3.org/2002/07/owl#onProperty>";
        pub const onProperty: u32 = 20;
        //                 let _owlpca: &str = "<http://www.w3.org/2002/07/owl#propertyChainAxiom>";
        pub const propertyChainAxiom: u32 = 21;
        //                 let _owldw: &str = "<http://www.w3.org/2002/07/owl#disjointWith>";
        pub const disjointWith: u32 = 22;
        //                 let _owlpdw: &str = "<http://www.w3.org/2002/07/owl#propertyDisjointWith>";
        pub const propertyDisjointWith: u32 = 23;
        //                 let _owluo: &str = "<http://www.w3.org/2002/07/owl#unionOf>";
        pub const unionOf: u32 = 24;
        //                 let _owlhk: &str = "<http://www.w3.org/2002/07/owl#hasKey>";
        pub const hasKey: u32 = 25;
        //                 let _owlavf: &str = "<http://www.w3.org/2002/07/owl#allValuesFrom>";
        pub const allValuesFrom: u32 = 26;
        //                 let _owlco: &str = "<http://www.w3.org/2002/07/owl#complementOf>";
        pub const complementOf: u32 = 27;
        //                 let _owloc: &str = "<http://www.w3.org/2002/07/owl#onClass>";
        pub const onClass: u32 = 28;
        //                 let _owldm: &str = "<http://www.w3.org/2002/07/owl#distinctMembers>";
        pub const distinctMembers: u32 = 29;
        //                 let _owlfp: &str = "<http://www.w3.org/2002/07/owl#FunctionalProperty>";
        pub const FunctionalProperty: u32 = 30;
        //                 let _owlni: &str = "<http://www.w3.org/2002/07/owl#NamedIndividual>";
        pub const NamedIndividual: u32 = 31;
        //                 let _owlop: &str = "<http://www.w3.org/2002/07/owl#ObjectProperty>";
        pub const ObjectProperty: u32 = 32;
        //                 let _owlc: &str = "<http://www.w3.org/2002/07/owl#Class>";
        pub const Class: u32 = 33;
        //                 let _owladc: &str = "<http://www.w3.org/2002/07/owl#AllDisjointClasses>";
        pub const AllDisjointClasses: u32 = 34;
        //                 let _owlr: &str = "<http://www.w3.org/2002/07/owl#Restriction>";
        pub const Restriction: u32 = 35;
        //                 let _owldp: &str = "<http://www.w3.org/2002/07/owl#DatatypeProperty>";
        pub const DatatypeProperty: u32 = 36;
        //                 let _owlo: &str = "<http://www.w3.org/2002/07/owl#Ontology>";
        pub const Ontology: u32 = 37;
        //                 let _owlap: &str = "<http://www.w3.org/2002/07/owl#AsymmetricProperty>";
        pub const AsymmetricProperty: u32 = 38;
        //                 let _owlsp: &str = "<http://www.w3.org/2002/07/owl#SymmetricProperty>";
        pub const SymmetricProperty: u32 = 39;
        //                 let _owlip: &str = "<http://www.w3.org/2002/07/owl#IrreflexiveProperty>";
        pub const IrreflexiveProperty: u32 = 40;
        //                 let _owlad: &str = "<http://www.w3.org/2002/07/owl#AllDifferent>";
        pub const AllDIfferent: u32 = 41;
        //                 let _owlifp: &str = "<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>";
        pub const InverseFunctionalProperty: u32 = 42;
        //                 let _owlsa: &str = "<http://www.w3.org/2002/07/owl#sameAs>";
        pub const sameAs: u32 = 43;
        //                 let _owlhv: &str = "<http://www.w3.org/2002/07/owl#hasValue>"
        pub const hasValue: u32 = 44;
        //                 let _owlnt: &str = "<http://www.w3.org/2002/07/owl#Nothing>"
        pub const Nothing: u32 = 45;
        //
        pub const oneOf: u32 = 46;
        //                 let _owloo: &str = "<http://www.w3.org/2002/07/owl#oneOf>"
    }
}
