use std::error::Error;

use super::executor::{ExecFilter, ExecIndexScan, ExecSeqScan};
use super::{BoxExecutor, Condition, TupleSearchMode};

use crate::btree::BTree;
use crate::buffer::BufferPoolManager;
use crate::disk::PageId;

pub trait PlanNode {
    fn start(&self, bufmgr: &mut BufferPoolManager) -> Result<BoxExecutor, Box<dyn Error>>;
}

pub struct SeqScan<'a> {
    pub table_meta_page_id: PageId,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: Condition<'a>,
}

impl<'a> PlanNode for SeqScan<'a> {
    fn start(&self, bufmgr: &mut BufferPoolManager) -> Result<BoxExecutor, Box<dyn Error>> {
        let btree = BTree::new(self.table_meta_page_id);
        let table_iter = btree.search(bufmgr, self.search_mode.encode())?;
        Ok(Box::new(ExecSeqScan::new(table_iter, self.while_cond)))
    }
}

pub struct Filter<'a> {
    pub inner_plan: &'a dyn PlanNode,
    pub cond: Condition<'a>,
}

impl<'a> PlanNode for Filter<'a> {
    fn start(&self, bufmgr: &mut BufferPoolManager) -> Result<BoxExecutor, Box<dyn Error>> {
        let inner_executor = self.inner_plan.start(bufmgr)?;
        Ok(Box::new(ExecFilter::new(inner_executor, &self.cond)))
    }
}

pub struct IndexScan<'a> {
    pub table_meta_page_id: PageId,
    pub index_meta_page_id: PageId,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: Condition<'a>,
}

impl<'a> PlanNode for IndexScan<'a> {
    fn start(&self, bufmgr: &mut BufferPoolManager) -> Result<BoxExecutor, Box<dyn Error>> {
        let table_btree = BTree::new(self.table_meta_page_id);
        let index_btree = BTree::new(self.index_meta_page_id);
        let index_iter = index_btree.search(bufmgr, self.search_mode.encode())?;
        Ok(Box::new(ExecIndexScan::new(
            table_btree,
            index_iter,
            self.while_cond,
        )))
    }
}
