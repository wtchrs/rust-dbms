use putty_db::btree::{BTree, SearchMode};
use putty_db::buffer::{BufferPool, BufferPoolManager};
use putty_db::disk::{DiskManager, PageId};
use putty_db::tuple;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let disk = DiskManager::open("table.tbl")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let btree = BTree::new(PageId(0));

    let target_key = b"b";
    let mut search_key = vec![];
    tuple::encode([target_key].iter(), &mut search_key);
    let mut iter = btree.search(&mut bufmgr, SearchMode::Key(search_key))?;
    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        let mut record = vec![];
        tuple::decode(&key, &mut record);
        if record[0] != target_key {
            break;
        }
        tuple::decode(&value, &mut record);
        println!("{:?}", tuple::Pretty(&record));
    }

    Ok(())
}
