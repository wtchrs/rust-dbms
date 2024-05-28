use putty_db::btree::{BTree, SearchMode};
use putty_db::buffer::{BufferPool, BufferPoolManager};
use putty_db::disk::{DiskManager, PageId};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let disk = DiskManager::open("btree.btr")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let btree = BTree::new(PageId(0));
    let mut iter = btree.search(&mut bufmgr, SearchMode::Key(b"daegu".to_vec()))?;
    let (key, value) = iter.next(&mut bufmgr)?.unwrap();
    println!("{:02x?} = {:02x?}", key, value);

    Ok(())
}
