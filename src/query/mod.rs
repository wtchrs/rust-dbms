pub mod executor;
pub mod planner;

use executor::Executor;
use crate::btree::SearchMode;
use crate::tuple;

pub type Tuple = Vec<Vec<u8>>;

pub type TupleSlice<'a> = &'a [Vec<u8>];

pub type BoxExecutor<'a> = Box<dyn Executor + 'a>;

pub type Condition<'a> = &'a dyn Fn(TupleSlice) -> bool;

pub enum TupleSearchMode<'a> {
    Start,
    Key(&'a [&'a [u8]]),
}

impl<'a> TupleSearchMode<'a> {
    fn encode(&self) -> SearchMode {
        match self {
            TupleSearchMode::Start => SearchMode::Start,
            TupleSearchMode::Key(tuple) => {
                let mut key = vec![];
                tuple::encode(tuple.iter(), &mut key);
                SearchMode::Key(key)
            }
        }
    }
}
