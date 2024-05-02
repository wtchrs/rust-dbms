use putty_db::btree::{BTree, SearchMode};
use putty_db::buffer::{BufferPool, BufferPoolManager};
use putty_db::disk::{DiskManager, PageId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let disk = DiskManager::open("btree.btr")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let btree = BTree::new(PageId(0));
    let mut iter = btree.search(&mut bufmgr, SearchMode::Key(b"jeju".to_vec()))?;
    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        println!("{:02x?} = {:02x?}", key, value);
    }
    
    Ok(())
}