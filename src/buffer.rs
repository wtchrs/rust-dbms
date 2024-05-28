use crate::disk::{DiskManager, Page, PageId, PAGE_SIZE};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ops::{Index, IndexMut};
use std::rc::Rc;

#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
    #[error("buffer is not mutable")]
    BufferNotMutable,
}

pub type BufferId = usize;

#[derive(Debug)]
pub struct Buffer {
    pub page_id: PageId,
    pub page: RefCell<Page>,
    pub is_dirty: Cell<bool>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            page_id: Default::default(),
            page: RefCell::new([0u8; PAGE_SIZE]),
            is_dirty: Cell::new(false),
        }
    }
}

#[derive(Debug, Default)]
pub struct Frame {
    usage_count: u64,
    buffer: Rc<Buffer>,
}

pub struct BufferPool {
    buffers: Vec<Frame>,
    next_victim_id: BufferId,
}

impl BufferPool {
    pub fn new(pool_size: usize) -> Self {
        let mut buffers = vec![];
        buffers.resize_with(pool_size, Default::default);
        let next_victim_id = BufferId::default();
        Self {
            buffers,
            next_victim_id,
        }
    }

    pub fn size(&self) -> usize {
        self.buffers.len()
    }

    fn evict(&mut self) -> Option<BufferId> {
        let pool_size = self.size();
        let mut consecutive_pinned = 0;
        let victim_id = loop {
            let next_victim_id = self.next_victim_id;
            let frame = &mut self[next_victim_id];
            if frame.usage_count == 0 {
                break next_victim_id;
            }
            if Rc::get_mut(&mut frame.buffer).is_some() {
                frame.usage_count -= 1;
                consecutive_pinned = 0;
            } else {
                consecutive_pinned += 1;
                if consecutive_pinned >= pool_size {
                    return None;
                }
            }
            self.next_victim_id = self.increment_id(next_victim_id);
        };
        Some(victim_id)
    }

    fn increment_id(&self, buffer_id: BufferId) -> BufferId {
        (buffer_id + 1) % self.size()
    }
}

impl Index<BufferId> for BufferPool {
    type Output = Frame;

    fn index(&self, index: BufferId) -> &Self::Output {
        &self.buffers[index]
    }
}

impl IndexMut<BufferId> for BufferPool {
    fn index_mut(&mut self, index: BufferId) -> &mut Self::Output {
        &mut self.buffers[index]
    }
}

pub struct BufferPoolManager {
    disk: DiskManager,
    pool: BufferPool,
    page_table: HashMap<PageId, BufferId>,
}

impl BufferPoolManager {
    pub fn new(disk: DiskManager, pool: BufferPool) -> Self {
        Self {
            disk,
            pool,
            page_table: HashMap::new(),
        }
    }

    pub fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, BufferError> {
        if let Some(&buffer_id) = self.page_table.get(&page_id) {
            let frame = &mut self.pool[buffer_id];
            frame.usage_count += 1;
            return Ok(Rc::clone(&frame.buffer));
        }

        let buffer_id = self.pool.evict().ok_or(BufferError::NoFreeBuffer)?;
        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;

        {
            let buffer = Rc::get_mut(&mut frame.buffer).ok_or(BufferError::BufferNotMutable)?;
            if buffer.is_dirty.get() {
                self.disk
                    .write_page(evict_page_id, buffer.page.get_mut().as_slice())?;
            }
            buffer.page_id = page_id;
            buffer.is_dirty.set(false);

            self.disk.read_page(page_id, buffer.page.get_mut())?;
            frame.usage_count = 1;
        }

        let page = Rc::clone(&frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }

    pub fn create_page(&mut self) -> Result<Rc<Buffer>, BufferError> {
        let buffer_id = self.pool.evict().ok_or(BufferError::NoFreeBuffer)?;
        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;
        let page_id = {
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();
            if buffer.is_dirty.get() {
                self.disk.write_page(evict_page_id, buffer.page.get_mut())?;
            }
            let page_id = self.disk.allocate_page();
            *buffer = Buffer::default();
            buffer.page_id = page_id;
            buffer.is_dirty.set(true);
            frame.usage_count = 1;
            page_id
        };
        let page = Rc::clone(&frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }

    pub fn flush(&mut self) -> Result<(), BufferError> {
        for (&page_id, &buffer_id) in self.page_table.iter() {
            let frame = &self.pool[buffer_id];
            let mut page = frame.buffer.page.borrow_mut();
            self.disk.write_page(page_id, page.as_mut_slice())?;
            frame.buffer.is_dirty.set(false);
        }
        self.disk.sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;

    #[test]
    fn test() {
        let mut hello = Vec::with_capacity(PAGE_SIZE);
        hello.extend_from_slice(b"hello");
        hello.resize(PAGE_SIZE, 0);

        let mut world = Vec::with_capacity(PAGE_SIZE);
        world.extend_from_slice(b"world");
        world.resize(PAGE_SIZE, 0);

        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let pool = BufferPool::new(1);
        let mut bufmgr = BufferPoolManager::new(disk, pool);

        let page1_id = {
            let buffer = bufmgr.create_page().unwrap();
            assert!(bufmgr.create_page().is_err());
            let mut page = buffer.page.borrow_mut();
            page.copy_from_slice(&hello);
            buffer.is_dirty.set(true);
            buffer.page_id
        };

        {
            let buffer = bufmgr.fetch_page(page1_id).unwrap();
            let page = buffer.page.borrow();
            assert_eq!(hello.as_slice(), page.as_slice());
        }

        let page2_id = {
            let buffer = bufmgr.create_page().unwrap();
            let mut page = buffer.page.borrow_mut();
            page.copy_from_slice(&world);
            buffer.is_dirty.set(true);
            buffer.page_id
        };

        {
            let buffer = bufmgr.fetch_page(page1_id).unwrap();
            let page = buffer.page.borrow();
            assert_eq!(hello.as_slice(), page.as_slice());
        }
        {
            let buffer = bufmgr.fetch_page(page2_id).unwrap();
            let page = buffer.page.borrow();
            assert_eq!(world.as_slice(), page.as_slice());
        }
    }
}
