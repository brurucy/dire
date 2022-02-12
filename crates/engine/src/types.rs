use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::Collection;
use flume::{Receiver, Sender};
use timely::communication::allocator::Generic;
use timely::communication::Allocator;
use timely::dataflow::scopes::Child;
use timely::worker::Worker;

pub type Terminator = Receiver<()>;

pub type Triple = (usize, usize, usize);
pub type TripleSink = Sender<(Triple, usize, isize)>;
pub type TripleSource = Receiver<(Triple, isize)>;
pub type TripleInputSession = InputSession<usize, Triple, isize>;
pub type TripleTrace = TraceAgent<OrdKeySpine<Triple, usize, isize>>;
pub type TripleCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, Triple>;

pub type UnaryMaterialization = for<'a> fn(&TripleCollection<'a>) -> TripleCollection<'a>;
pub type AboxMaterialization =
    for<'a> fn(&TripleCollection<'a>, &TripleCollection<'a>) -> TripleCollection<'a>;

pub type WorkerExecutionClosure = Box<dyn Fn(&mut Worker<Allocator>) -> () + Send + Sync + 'static>;
