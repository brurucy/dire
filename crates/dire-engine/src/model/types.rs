use differential_dataflow::Collection;
use flume::{Receiver, Sender};
use timely::communication::allocator::Generic;
use timely::dataflow::scopes::Child;
use timely::worker::Worker;

pub type Terminator = Receiver<()>;

pub type Tuple = (u32, u32);
pub type Triple = (u32, u32, u32);
pub type KeyedTriple = (u32, (u32, u32));
pub type List = (u32, Vec<u32>);

pub type TripleInputSink = Sender<(Triple, isize)>;
pub type TripleOutputSink = Sender<(Triple, usize, isize)>;

pub type TripleInputSource = Receiver<(Triple, isize)>;
pub type TripleOutputSource = Receiver<(Triple, usize, isize)>;

pub type TerminationSink = Sender<()>;

pub type TupleCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, Tuple>;
pub type TripleCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, Triple>;
pub type KeyedTripleCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, KeyedTriple>;
pub type ListCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, List>;

pub type FirstStageMaterialization =
    for<'a> fn(&TripleCollection<'a>) -> (TripleCollection<'a>, ListCollection<'a>);
pub type SecondStageMaterialization = for<'a> fn(
    &TripleCollection<'a>,
    &ListCollection<'a>,
    &TripleCollection<'a>,
) -> TripleCollection<'a>;
