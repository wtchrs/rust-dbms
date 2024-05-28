use putty_db::buffer::{BufferPool, BufferPoolManager};
use putty_db::disk::{DiskManager, PageId};
use putty_db::table::Table;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let disk = DiskManager::open("table.tbl")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let mut table = Table {
        meta_page_id: PageId::INVALID_PAGE_ID,
        num_key_elems: 1,
        unique_index: vec![],
    };

    table.create(&mut bufmgr)?;

    table.insert(&mut bufmgr, &[b"a", b"Charlie", b"MUNGER"])?;
    table.insert(&mut bufmgr, &[b"b", b"Brian", b"LEE"])?;
    table.insert(&mut bufmgr, &[b"c", b"Alice", b"SMITH"])?;
    table.insert(&mut bufmgr, &[b"d", b"John", b"BAKERY"])?;

    bufmgr.flush()?;
    Ok(())
}
