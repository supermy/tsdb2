use std::sync::atomic::{AtomicUsize, Ordering};

/// 线程安全的内存池
///
/// 使用 CAS (Compare-And-Swap) 实现无锁的并发内存分配/释放。
/// 适用于控制写入缓冲区等场景的全局内存上限。
pub struct TsdbMemoryPool {
    /// 内存上限 (字节)
    limit: usize,
    /// 已使用内存 (字节), 原子操作保证线程安全
    used: AtomicUsize,
}

impl TsdbMemoryPool {
    /// 创建指定上限的内存池
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    /// 尝试分配指定大小的内存
    ///
    /// 使用 CAS 循环确保并发安全:
    /// - 分配后不超过 limit → 返回 true, 已用量增加
    /// - 分配后超过 limit → 返回 false, 已用量不变
    pub fn allocate(&self, size: usize) -> bool {
        loop {
            let current = self.used.load(Ordering::Relaxed);
            if current + size > self.limit {
                return false;
            }
            if self
                .used
                .compare_exchange_weak(current, current + size, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// 释放指定大小的内存
    pub fn release(&self, size: usize) {
        self.used.fetch_sub(size, Ordering::SeqCst);
    }

    /// 获取当前已使用内存
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// 获取内存上限
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// 获取剩余可用内存
    pub fn available(&self) -> usize {
        self.limit.saturating_sub(self.used())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_memory_pool_basic() {
        let pool = TsdbMemoryPool::new(1024);
        assert!(pool.allocate(512));
        assert_eq!(pool.used(), 512);
        assert_eq!(pool.available(), 512);
        pool.release(512);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_memory_pool_limit() {
        let pool = TsdbMemoryPool::new(1024);
        assert!(pool.allocate(1024));
        assert!(!pool.allocate(1));
    }

    #[test]
    fn test_memory_pool_concurrent() {
        let pool = Arc::new(TsdbMemoryPool::new(10000));
        let mut handles = vec![];

        for _ in 0..10 {
            let p = pool.clone();
            handles.push(std::thread::spawn(move || {
                assert!(p.allocate(500));
                p.release(500);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(pool.used(), 0);
    }
}
