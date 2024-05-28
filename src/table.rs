use std::error::Error;
use crate::btree::{BTree, Iter, SearchMode};
use crate::buffer::BufferPoolManager;
use crate::disk::PageId;
use crate::tuple;

pub struct Table {
    pub meta_page_id: PageId,
    /// The count of columns from the leftmost that are key elements.
    pub num_key_elems: usize,
    pub unique_index: Vec<UniqueIndex>,
}

impl Table {
    pub fn create(&mut self, bufmgr: &mut BufferPoolManager) -> Result<(), Box<dyn Error>> {
        let btree = BTree::create(bufmgr)?;
        self.meta_page_id = btree.meta_page_id;
        Ok(())
    }

    pub fn insert(
        &mut self,
        bufmgr: &mut BufferPoolManager,
        record: &[&[u8]],
    ) -> Result<(), Box<dyn Error>> {
        let btree = BTree::new(self.meta_page_id);
        let mut key = vec![];
        tuple::encode(record[..self.num_key_elems].iter(), &mut key);
        let mut value = vec![];
        tuple::encode(record[self.num_key_elems..].iter(), &mut value);
        // Check unique constraints.
        for unique_index in &self.unique_index {
            if unique_index.search(bufmgr, key.clone())?.next(bufmgr)?.is_some() {
                return Err("Unique constraint violation".into());
            }
        }
        btree.insert(bufmgr, &key, &value)?;
        for unique_index in &mut self.unique_index {
            unique_index.insert(bufmgr, &key, record)?;
        }
        Ok(())
    }
}

pub struct UniqueIndex {
    pub meta_page_id: PageId,
    pub skey: Vec<usize>,
}

impl UniqueIndex {
    pub fn create(&mut self, bufmgr: &mut BufferPoolManager) -> Result<(), Box<dyn Error>> {
        let btree = BTree::create(bufmgr)?;
        self.meta_page_id = btree.meta_page_id;
        Ok(())
    }

    pub fn insert(
        &mut self,
        bufmgr: &mut BufferPoolManager,
        pkey: &[u8],
        record: &[impl AsRef<[u8]>],
    ) -> Result<(), Box<dyn Error>> {
        let btree = BTree::new(self.meta_page_id);
        let mut skey = vec![];
        tuple::encode(
            self.skey.iter().map(|&index| record[index].as_ref()),
            &mut skey,
        );
        btree.insert(bufmgr, &skey, pkey)?;
        Ok(())
    }

    pub fn search(
        &self,
        bufmgr: &mut BufferPoolManager,
        skey: Vec<u8>,
    ) -> Result<Iter, Box<dyn Error>> {
        let btree = BTree::new(self.meta_page_id);
        let iter = btree.search(bufmgr, SearchMode::Key(skey))?;
        Ok(iter)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;
    use crate::btree;
    use crate::buffer::BufferPool;
    use crate::disk::{DiskManager};
    use crate::tuple::encode;
    use super::*;

    #[test]
    fn test_table_create() {
        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let buffer_pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, buffer_pool);

        let mut table = Table {
            meta_page_id: PageId::INVALID_PAGE_ID,
            num_key_elems: 1,
            unique_index: vec![],
        };

        assert!(table.create(&mut bufmgr).is_ok());
        assert_ne!(table.meta_page_id, PageId::INVALID_PAGE_ID);
    }

    #[test]
    fn test_table_insert() {
        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let buffer_pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, buffer_pool);

        let mut table = Table {
            meta_page_id: PageId::INVALID_PAGE_ID,
            num_key_elems: 1,
            unique_index: vec![],
        };

        table.create(&mut bufmgr).unwrap();
        table.insert(&mut bufmgr, &[b"a", b"Charlie", b"MUNGER"]).unwrap();
        table.insert(&mut bufmgr, &[b"b", b"Brian", b"LEE"]).unwrap();
        table.insert(&mut bufmgr, &[b"c", b"Alice", b"SMITH"]).unwrap();
        table.insert(&mut bufmgr, &[b"d", b"John", b"BAKERY"]).unwrap();

        bufmgr.flush().unwrap();

        // Check whether the records are inserted correctly.
        let btree = BTree::new(table.meta_page_id);
        let mut iter = btree.search(&mut bufmgr, btree::SearchMode::Start).unwrap();
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"Charlie", b"MUNGER"]));
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"Brian", b"LEE"]));
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"Alice", b"SMITH"]));
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"John", b"BAKERY"]));
    }

    #[test]
    fn test_unique_index_create_and_insert() {
        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let buffer_pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, buffer_pool);

        let mut table = Table {
            meta_page_id: PageId::INVALID_PAGE_ID,
            num_key_elems: 1,
            unique_index: vec![],
        };

        table.create(&mut bufmgr).unwrap();

        let mut unique_index = UniqueIndex {
            meta_page_id: PageId::INVALID_PAGE_ID,
            skey: vec![1, 2],
        };

        unique_index.create(&mut bufmgr).unwrap();

        table.unique_index.push(unique_index);

        table.insert(&mut bufmgr, &[b"a", b"Charlie", b"MUNGER"]).unwrap();
        table.insert(&mut bufmgr, &[b"b", b"Brian", b"LEE"]).unwrap();
        table.insert(&mut bufmgr, &[b"c", b"Alice", b"SMITH"]).unwrap();
        table.insert(&mut bufmgr, &[b"d", b"John", b"BAKERY"]).unwrap();

        bufmgr.flush().unwrap();

        // Check whether the records are inserted correctly.
        let btree = BTree::new(table.meta_page_id);
        let mut iter = btree.search(&mut bufmgr, SearchMode::Start).unwrap();
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"Charlie", b"MUNGER"]));
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"Brian", b"LEE"]));
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"Alice", b"SMITH"]));
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"John", b"BAKERY"]));

        // Check whether the unique index is created correctly.
        let btree = BTree::new(table.unique_index[0].meta_page_id);
        let mut iter = btree.search(&mut bufmgr, SearchMode::Start).unwrap();
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"c"]));
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"b"]));
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"a"]));
        assert_eq!(iter.next(&mut bufmgr).unwrap().unwrap().1, get_encoded(&[b"d"]));
    }

    #[test]
    fn test_duplicate_unique_key() {
        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let buffer_pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, buffer_pool);

        let mut table = Table {
            meta_page_id: PageId::INVALID_PAGE_ID,
            num_key_elems: 1,
            unique_index: vec![],
        };

        table.create(&mut bufmgr).unwrap();

        let mut unique_index = UniqueIndex {
            meta_page_id: PageId::INVALID_PAGE_ID,
            skey: vec![1, 2],
        };

        unique_index.create(&mut bufmgr).unwrap();

        table.unique_index.push(unique_index);

        table.insert(&mut bufmgr, &[b"a", b"Charlie", b"MUNGER"]).unwrap();
        table.insert(&mut bufmgr, &[b"b", b"Brian", b"LEE"]).unwrap();
        table.insert(&mut bufmgr, &[b"c", b"Alice", b"SMITH"]).unwrap();
        table.insert(&mut bufmgr, &[b"d", b"John", b"BAKERY"]).unwrap();

        // Try to insert a record with a duplicate unique key.
        assert!(table.insert(&mut bufmgr, &[b"e", b"Charlie", b"MUNGER"]).is_err());
    }

    fn get_encoded(record: &[&[u8]]) -> Vec<u8> {
        let mut key = vec![];
        encode(record.iter(), &mut key);
        key
    }
}
