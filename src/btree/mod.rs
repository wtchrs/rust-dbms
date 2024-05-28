use crate::btree::branch::Branch;
use crate::btree::leaf::Leaf;
use crate::btree::node::Node;
use crate::btree::pair::Pair;
use crate::buffer::{Buffer, BufferPoolManager};
use crate::disk::PageId;
use error::BTreeError;
use std::convert::identity;
use std::rc::Rc;
use zerocopy::ByteSlice;

mod branch;
mod bsearch;
mod error;
mod leaf;
mod meta;
mod node;
mod pair;

pub struct BTree {
    pub meta_page_id: PageId,
}

impl BTree {
    pub fn create(bufmgr: &mut BufferPoolManager) -> Result<Self, BTreeError> {
        let buffer = bufmgr.create_page()?;
        let mut buffer_page = buffer.page.borrow_mut();
        let mut meta = meta::Meta::new(buffer_page.as_mut_slice());
        let root_buffer = bufmgr.create_page()?;
        let mut root_buffer_page = root_buffer.page.borrow_mut();
        let mut root = Node::new(root_buffer_page.as_mut_slice());
        root.initialize_as_leaf();
        let mut leaf = Leaf::new(root.body);
        leaf.initialize();
        meta.header.root_page_id = root_buffer.page_id;
        Ok(Self::new(buffer.page_id))
    }

    pub fn new(meta_page_id: PageId) -> Self {
        Self { meta_page_id }
    }

    fn fetch_root_page(&self, bufmgr: &mut BufferPoolManager) -> Result<Rc<Buffer>, BTreeError> {
        let meta_buffer = bufmgr.fetch_page(self.meta_page_id)?;
        let meta_page = meta_buffer.page.borrow();
        let meta = meta::Meta::new(meta_page.as_slice());
        let root_buffer = bufmgr.fetch_page(meta.header.root_page_id)?;
        Ok(root_buffer)
    }

    fn search_internal(
        bufmgr: &mut BufferPoolManager,
        node_buffer: Rc<Buffer>,
        search_mode: SearchMode,
    ) -> Result<Iter, BTreeError> {
        let node_page = node_buffer.page.borrow();
        let node = Node::new(node_page.as_slice());
        let node_type = node.header.node_type;
        let body = node.body;
        match node::Body::new(node_type, body) {
            node::Body::Leaf(leaf) => {
                let slot_id = search_mode.tuple_slot_id(&leaf).unwrap_or_else(identity);
                let is_right_most = slot_id == leaf.num_pairs();
                drop(node_page);

                let mut iter = Iter::new(node_buffer, slot_id);
                if is_right_most {
                    iter.advance(bufmgr)?;
                }
                Ok(iter)
            }
            node::Body::Branch(branch) => {
                let child_page_id = search_mode.child_page_id(&branch);
                drop(node_page);
                drop(node_buffer);
                let child_buffer = bufmgr.fetch_page(child_page_id)?;
                Self::search_internal(bufmgr, child_buffer, search_mode)
            }
        }
    }

    pub fn search(
        &self,
        bufmgr: &mut BufferPoolManager,
        search_mode: SearchMode,
    ) -> Result<Iter, BTreeError> {
        let root_page = self.fetch_root_page(bufmgr)?;
        Self::search_internal(bufmgr, root_page, search_mode)
    }

    fn insert_internal(
        bufmgr: &mut BufferPoolManager,
        node_buffer: Rc<Buffer>,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, PageId)>, BTreeError> {
        let mut node_page = node_buffer.page.borrow_mut();
        let node = Node::new(node_page.as_mut_slice());
        let node_type = node.header.node_type;
        let body = node.body;
        match node::Body::new(node_type, body) {
            node::Body::Leaf(mut leaf) => {
                let slot_id = match leaf.search_slot_id(key) {
                    Ok(_) => return Err(BTreeError::DuplicateKey),
                    Err(slot_id) => slot_id,
                };

                if leaf.insert(slot_id, key, value).is_some() {
                    node_buffer.is_dirty.set(true);
                    Ok(None)
                } else {
                    let prev_leaf_id = leaf.prev_page_id();
                    let prev_leaf_buffer =
                        prev_leaf_id.map(|id| bufmgr.fetch_page(id)).transpose()?;

                    let new_leaf_buffer = bufmgr.create_page()?;

                    if let Some(prev_leaf_buffer) = prev_leaf_buffer {
                        let mut prev_leaf_page = prev_leaf_buffer.page.borrow_mut();
                        let node = Node::new(prev_leaf_page.as_mut_slice());
                        let mut prev_leaf = Leaf::new(node.body);
                        prev_leaf.set_next_page_id(Some(new_leaf_buffer.page_id));
                        prev_leaf_buffer.is_dirty.set(true);
                    }

                    leaf.set_prev_page_id(Some(new_leaf_buffer.page_id));

                    let mut new_page = new_leaf_buffer.page.borrow_mut();
                    let mut new_node = Node::new(new_page.as_mut_slice());
                    new_node.initialize_as_leaf();

                    let mut new_leaf = Leaf::new(new_node.body);
                    new_leaf.initialize();

                    let split_key = leaf.split_insert(&mut new_leaf, key, value);

                    new_leaf.set_next_page_id(Some(node_buffer.page_id));
                    new_leaf.set_prev_page_id(prev_leaf_id);
                    new_leaf_buffer.is_dirty.set(true);
                    node_buffer.is_dirty.set(true);

                    Ok(Some((split_key, new_leaf_buffer.page_id)))
                }
            }
            node::Body::Branch(mut branch) => {
                let child_idx = branch.search_child_idx(key);
                let child_page_id = branch.child_at(child_idx);
                let child_buffer = bufmgr.fetch_page(child_page_id)?;
                match Self::insert_internal(bufmgr, child_buffer, key, value)? {
                    None => Ok(None),
                    Some((overflow_key_from_child, overflow_child_page_id)) => {
                        if branch
                            .insert(child_idx, &overflow_key_from_child, overflow_child_page_id)
                            .is_some()
                        {
                            node_buffer.is_dirty.set(true);
                            Ok(None)
                        } else {
                            let new_branch_buffer = bufmgr.create_page()?;
                            let mut new_page = new_branch_buffer.page.borrow_mut();
                            let mut new_node = Node::new(new_page.as_mut_slice());
                            new_node.initialize_as_branch();
                            let mut new_branch = Branch::new(new_node.body);

                            let split_key = branch.split_insert(
                                &mut new_branch,
                                &overflow_key_from_child,
                                overflow_child_page_id,
                            );

                            node_buffer.is_dirty.set(true);
                            new_branch_buffer.is_dirty.set(true);
                            Ok(Some((split_key, new_branch_buffer.page_id)))
                        }
                    }
                }
            }
        }
    }

    pub fn insert(
        &self,
        bufmgr: &mut BufferPoolManager,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), BTreeError> {
        let meta_buffer = bufmgr.fetch_page(self.meta_page_id)?;
        let mut meta_page = meta_buffer.page.borrow_mut();
        let mut meta = meta::Meta::new(meta_page.as_mut_slice());
        let root_buffer = bufmgr.fetch_page(meta.header.root_page_id)?;
        if let Some((key, child_page_id)) = Self::insert_internal(bufmgr, root_buffer, key, value)?
        {
            let new_root_buffer = bufmgr.create_page()?;
            let mut new_root_page = new_root_buffer.page.borrow_mut();
            let mut new_root = Node::new(new_root_page.as_mut_slice());
            new_root.initialize_as_branch();
            let mut branch = Branch::new(new_root.body);
            branch.initialize(&key, child_page_id, meta.header.root_page_id);
            meta.header.root_page_id = new_root_buffer.page_id;
            meta_buffer.is_dirty.set(true);
        }
        Ok(())
    }
}

pub struct Iter {
    buffer: Rc<Buffer>,
    slot_id: usize,
}

impl Iter {
    pub fn new(buffer: Rc<Buffer>, slot_id: usize) -> Self {
        Self { buffer, slot_id }
    }

    fn get(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let page = self.buffer.page.borrow();
        let node = Node::new(page.as_slice());
        let leaf = Leaf::new(node.body);
        if self.slot_id < leaf.num_pairs() {
            let Pair { key, value } = leaf.pair_at(self.slot_id);
            Some((key.to_vec(), value.to_vec()))
        } else {
            None
        }
    }

    fn advance(&mut self, bufmgr: &mut BufferPoolManager) -> Result<(), BTreeError> {
        self.slot_id += 1;
        let next_page_id = {
            let page = self.buffer.page.borrow();
            let node = Node::new(page.as_slice());
            let leaf = Leaf::new(node.body);
            if self.slot_id < leaf.num_pairs() {
                return Ok(());
            }
            leaf.next_page_id()
        };
        if let Some(next_page_id) = next_page_id {
            self.buffer = bufmgr.fetch_page(next_page_id)?;
            self.slot_id = 0;
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub fn next(
        &mut self,
        bufmgr: &mut BufferPoolManager,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, BTreeError> {
        let value = self.get();
        self.advance(bufmgr)?;
        Ok(value)
    }
}

#[derive(Debug, Clone)]
pub enum SearchMode {
    Start,
    Key(Vec<u8>),
}

impl SearchMode {
    pub fn child_page_id(&self, branch: &Branch<impl ByteSlice>) -> PageId {
        match self {
            SearchMode::Start => branch.child_at(0),
            SearchMode::Key(key) => branch.search_child(key),
        }
    }

    pub fn tuple_slot_id(&self, leaf: &Leaf<impl ByteSlice>) -> Result<usize, usize> {
        match self {
            SearchMode::Start => Ok(0),
            SearchMode::Key(key) => leaf.search_slot_id(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPool;
    use crate::disk::DiskManager;
    use tempfile::tempfile;

    #[test]
    fn test() {
        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, pool);
        let btree = BTree::create(&mut bufmgr).unwrap();
        btree
            .insert(&mut bufmgr, &6u64.to_be_bytes(), b"world")
            .unwrap();
        btree
            .insert(&mut bufmgr, &3u64.to_be_bytes(), b"hello")
            .unwrap();
        btree
            .insert(&mut bufmgr, &8u64.to_be_bytes(), b"!")
            .unwrap();
        btree
            .insert(&mut bufmgr, &4u64.to_be_bytes(), b",")
            .unwrap();

        let (_, value) = btree
            .search(&mut bufmgr, SearchMode::Key(3u64.to_be_bytes().to_vec()))
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(b"hello", &value[..]);
        let (_, value) = btree
            .search(&mut bufmgr, SearchMode::Key(8u64.to_be_bytes().to_vec()))
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(b"!", &value[..]);
    }

    #[test]
    fn test_search_iter() {
        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, pool);
        let btree = BTree::create(&mut bufmgr).unwrap();

        for i in 0u64..16 {
            btree
                .insert(&mut bufmgr, &(i * 2).to_be_bytes(), &[0; 1024])
                .unwrap();
        }

        for i in 0u64..15 {
            let (key, _) = btree
                .search(
                    &mut bufmgr,
                    SearchMode::Key((i * 2 + 1).to_be_bytes().to_vec()),
                )
                .unwrap()
                .get()
                .unwrap();

            assert_eq!(key.as_slice(), &((i + 1) * 2).to_be_bytes());
        }
    }

    #[test]
    fn test_split() {
        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, pool);
        let btree = BTree::create(&mut bufmgr).unwrap();
        let long_data_list = vec![
            vec![0xC0u8; 1000],
            vec![0x01u8; 1000],
            vec![0xCAu8; 1000],
            vec![0xFEu8; 1000],
            vec![0xDEu8; 1000],
            vec![0xADu8; 1000],
            vec![0xBEu8; 1000],
            vec![0xAEu8; 1000],
        ];
        for data in long_data_list.iter() {
            btree.insert(&mut bufmgr, data, data).unwrap();
        }
        for data in long_data_list.iter() {
            let (k, v) = btree
                .search(&mut bufmgr, SearchMode::Key(data.clone()))
                .unwrap()
                .get()
                .unwrap();
            assert_eq!(data, &k);
            assert_eq!(data, &v);
        }
    }
}
