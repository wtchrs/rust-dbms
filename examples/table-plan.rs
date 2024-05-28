use putty_db::buffer::{BufferPool, BufferPoolManager};
use putty_db::disk::{DiskManager, PageId};
use putty_db::query::planner::{Filter, PlanNode, SeqScan};
use putty_db::query::TupleSearchMode;
use putty_db::tuple;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let disk = DiskManager::open("table.tbl")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    // SELECT * from ... WHERE id >= 'a' AND id < 'e' AND first_name < 'John';
    let query_plan = SeqScan {
        table_meta_page_id: PageId(0),
        search_mode: TupleSearchMode::Key(&[b"a"]),
        while_cond: &|pk| pk[0].as_slice() < b"e",
    };
    let query_plan = Filter {
        inner_plan: &query_plan,
        cond: &|record| record[1].as_slice() < b"John",
    };

    let mut exec = query_plan.start(&mut bufmgr)?;

    while let Some(record) = exec.next(&mut bufmgr)? {
        println!("{:?}", tuple::Pretty(&record));
    }

    Ok(())
}
