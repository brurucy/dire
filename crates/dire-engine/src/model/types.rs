use differential_dataflow::input::InputSession;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::Collection;
use flume::{Receiver, Sender};
use timely::communication::allocator::Generic;
use timely::communication::Allocator;
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, ScopeParent};
use timely::worker::Worker;

pub type Terminator = Receiver<()>;

pub type Triple = (u32, u32, u32);
pub type KeyedTriple = (u32, (u32, u32));

pub type TripleInputSink = Sender<(Triple, isize)>;
pub type TripleOutputSink = Sender<(Triple, usize, isize)>;

pub type TripleInputSource = Receiver<(Triple, isize)>;
pub type TripleOutputSource = Receiver<(Triple, usize, isize)>;

pub type TerminationSink = Sender<()>;

pub type RegularScope<'b> = Child<'b, Worker<Generic>, usize>;

pub type TripleCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, Triple>;
pub type KeyedTripleCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, KeyedTriple>;

pub type UnaryMaterialization = for<'a> fn(&TripleCollection<'a>) -> TripleCollection<'a>;
pub type BinaryMaterialization =
    for<'a> fn(&TripleCollection<'a>, &TripleCollection<'a>) -> TripleCollection<'a>;

pub type WorkerExecutionClosure = Box<dyn Fn(&mut Worker<Allocator>) -> () + Send + Sync + 'static>;
