use putty_db::disk::DiskManager;

fn main() {
    let mut disk = DiskManager::open("../foo.db").unwrap();
    let page_id = disk.allocate_page();
    let page = [0; 4096];
    disk.write_page(page_id, &page).unwrap();
    let mut read_page = [0; 4096];
    disk.read_page(page_id, &mut read_page).unwrap();
    assert_eq!(read_page, page);
}
