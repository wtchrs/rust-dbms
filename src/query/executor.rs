use std::error::Error;

use super::{BoxExecutor, Condition, Tuple};

use crate::btree::{BTree, Iter, SearchMode};
use crate::buffer::BufferPoolManager;
use crate::tuple;
use crate::tuple::decode;

pub trait Executor {
    fn next(&mut self, bufmgr: &mut BufferPoolManager) -> Result<Option<Tuple>, Box<dyn Error>>;
}

pub struct ExecSeqScan<'a> {
    table_iter: Iter,
    while_cond: Condition<'a>,
}

impl<'a> ExecSeqScan<'a> {
    pub fn new(table_iter: Iter, while_cond: Condition<'a>) -> Self {
        Self {
            table_iter,
            while_cond,
        }
    }
}

impl<'a> Executor for ExecSeqScan<'a> {
    fn next(&mut self, bufmgr: &mut BufferPoolManager) -> Result<Option<Tuple>, Box<dyn Error>> {
        let (pk_bytes, tuple_bytes) = match self.table_iter.next(bufmgr)? {
            Some(pair) => pair,
            None => return Ok(None),
        };

        let mut pk = vec![];
        tuple::decode(&pk_bytes, &mut pk);
        if !(self.while_cond)(&pk) {
            return Ok(None);
        }

        let mut tuple = pk;
        tuple::decode(&tuple_bytes, &mut tuple);
        Ok(Some(tuple))
    }
}

pub struct ExecFilter<'a> {
    inner_executor: BoxExecutor<'a>,
    while_cond: Condition<'a>,
}

impl<'a> ExecFilter<'a> {
    pub fn new(inner_executor: BoxExecutor<'a>, while_cond: Condition<'a>) -> Self {
        Self {
            inner_executor,
            while_cond,
        }
    }
}

impl<'a> Executor for ExecFilter<'a> {
    fn next(&mut self, bufmgr: &mut BufferPoolManager) -> Result<Option<Tuple>, Box<dyn Error>> {
        while let Some(tuple) = self.inner_executor.next(bufmgr)? {
            if (self.while_cond)(&tuple) {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

pub struct ExecIndexScan<'a> {
    table_btree: BTree,
    index_iter: Iter,
    while_cond: Condition<'a>,
}

impl<'a> ExecIndexScan<'a> {
    pub fn new(table_btree: BTree, index_iter: Iter, while_cond: Condition<'a>) -> Self {
        Self {
            table_btree,
            index_iter,
            while_cond,
        }
    }
}

impl<'a> Executor for ExecIndexScan<'a> {
    fn next(&mut self, bufmgr: &mut BufferPoolManager) -> Result<Option<Tuple>, Box<dyn Error>> {
        let (skey_bytes, pkey_bytes) = match self.index_iter.next(bufmgr)? {
            Some(x) => x,
            None => return Ok(None),
        };
        let mut skey = vec![];
        decode(skey_bytes.as_slice(), &mut skey);
        if !(self.while_cond)(&skey) {
            return Ok(None);
        }
        let table_iter = self.table_btree.search(bufmgr, SearchMode::Key(pkey_bytes));
        let (pkey_bytes, tuple_bytes) = table_iter?.next(bufmgr)?.unwrap();
        let mut record = vec![];
        decode(pkey_bytes.as_slice(), &mut record);
        decode(tuple_bytes.as_slice(), &mut record);
        Ok(Some(record))
    }
}
