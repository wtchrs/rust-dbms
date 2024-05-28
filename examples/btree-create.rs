use putty_db::btree::BTree;
use putty_db::buffer::{BufferPool, BufferPoolManager};
use putty_db::disk::DiskManager;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let disk = DiskManager::open("btree.btr")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let btree = BTree::create(&mut bufmgr)?;

    btree.insert(&mut bufmgr, b"seoul", b"jungu")?;
    btree.insert(&mut bufmgr, b"pusan", b"yunjegu")?;
    btree.insert(&mut bufmgr, b"daegu", b"jungu")?;
    btree.insert(&mut bufmgr, b"incheon", b"namdongu")?;
    btree.insert(&mut bufmgr, b"gwangju", b"seogu")?;

    bufmgr.flush()?;

    Ok(())
}
