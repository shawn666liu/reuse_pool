use crossbeam::queue::{ArrayQueue, SegQueue};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

/// 队列 trait，要求元素类型 `T: Send`，且队列本身可跨线程共享（Send + Sync）。
pub trait Queue<T: Send>: Send + Sync {
    /// 将值推入队列，成功返回 Ok(())，如果队列已满则返回 Err(value)。
    fn push(&self, value: T) -> Result<(), T>;
    fn pop(&self) -> Option<T>;
}

/// 为 ArrayQueue 实现 Queue（有界）
impl<T: Send> Queue<T> for ArrayQueue<T> {
    fn push(&self, value: T) -> Result<(), T> {
        self.push(value)
    }
    fn pop(&self) -> Option<T> {
        self.pop()
    }
}

/// 为 SegQueue 实现 Queue（无界，总是成功）
impl<T: Send> Queue<T> for SegQueue<T> {
    fn push(&self, value: T) -> Result<(), T> {
        self.push(value);
        Ok(())
    }
    fn pop(&self) -> Option<T> {
        self.pop()
    }
}

/// 对象池：支持有界/无界队列，通过工厂函数创建新对象。
pub struct Pool<T: Send, Q: Queue<Box<T>> = SegQueue<Box<T>>> {
    free: Q,
    max_idle: Option<usize>, // None 表示无界，Some 表示有界容量（仅用于监控）
    idle_count: AtomicUsize, // 当前空闲对象数量
    created: AtomicUsize,    // 累计创建对象数量
    factory: Box<dyn Fn() -> T + Send + Sync>, // 创建新对象的工厂函数
    _phantom: PhantomData<T>, // 标记 T 被使用
}

impl<T: Send> Pool<T, SegQueue<Box<T>>> {
    /// 创建无界对象池，使用默认工厂函数（要求 T: Default）
    pub fn new_unbounded() -> Arc<Self>
    where
        T: Default,
    {
        Self::with_factory(SegQueue::new(), None, || T::default())
    }
}

impl<T: Send> Pool<T, ArrayQueue<Box<T>>> {
    /// 创建有界对象池，使用默认工厂函数（要求 T: Default）
    pub fn new_bounded(max_idle: usize) -> Arc<Self>
    where
        T: Default,
    {
        let free = ArrayQueue::new(max_idle);
        Arc::new(Self {
            free,
            max_idle: Some(max_idle),
            idle_count: AtomicUsize::new(0),
            created: AtomicUsize::new(0),
            factory: Box::new(|| T::default()),
            _phantom: PhantomData,
        })
    }
}

impl<T: Send, Q: Queue<Box<T>>> Pool<T, Q> {
    /// 通用构造函数，接受队列、最大空闲数（可选）和工厂函数。
    pub fn with_factory<F>(free: Q, max_idle: Option<usize>, factory: F) -> Arc<Self>
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        Arc::new(Self {
            free,
            max_idle,
            idle_count: AtomicUsize::new(0),
            created: AtomicUsize::new(0),
            factory: Box::new(factory),
            _phantom: PhantomData,
        })
    }

    /// 归还对象（内部方法）——不再自动 reset，直接入队
    fn put(&self, obj: Box<T>) {
        match self.free.push(obj) {
            Ok(()) => {
                self.idle_count.fetch_add(1, Ordering::Relaxed);
            }
            Err(_obj) => {
                // 队列满，对象被丢弃（_obj 在此作用域结束时 drop）
                // 可在此添加日志
            }
        }
    }

    /// 获取池状态：(空闲数, 最大空闲(如果有), 累计创建)
    pub fn status(&self) -> (usize, Option<usize>, usize) {
        (
            self.idle_count.load(Ordering::Relaxed),
            self.max_idle,
            self.created.load(Ordering::Relaxed),
        )
    }

    /// 从池中获取一个可变守卫（用于填充数据）
    pub fn get(self: &Arc<Self>) -> PooledMut<T, Q> {
        let obj = if let Some(obj) = self.free.pop() {
            self.idle_count.fetch_sub(1, Ordering::Relaxed);
            obj
        } else {
            self.created.fetch_add(1, Ordering::Relaxed);
            Box::new((self.factory)())
        };
        PooledMut {
            pool: Arc::downgrade(self),
            inner: Some(obj),
        }
    }

    /// 从池中获取一个可变守卫（用于填充数据）, 使用自定义工厂
    pub fn get_with<F>(self: &Arc<Self>, factory: F) -> PooledMut<T, Q>
    where
        F: FnOnce() -> T + Send,
    {
        let obj = if let Some(obj) = self.free.pop() {
            self.idle_count.fetch_sub(1, Ordering::Relaxed);
            obj
        } else {
            self.created.fetch_add(1, Ordering::Relaxed);
            Box::new(factory())
        };
        PooledMut {
            pool: Arc::downgrade(self),
            inner: Some(obj),
        }
    }
}

/// 可变守卫：独占访问，用于填充数据。
pub struct PooledMut<T: Send, Q: Queue<Box<T>> = SegQueue<Box<T>>> {
    pool: Weak<Pool<T, Q>>,
    inner: Option<Box<T>>,
}

impl<T: Send, Q: Queue<Box<T>>> PooledMut<T, Q> {
    /// 获取可变引用（填充）
    pub fn as_mut(&mut self) -> &mut T {
        self.inner
            .as_mut()
            .expect("PooledMut inner should be Some in as_mut")
    }

    /// 获取不可变引用
    pub fn as_ref(&self) -> &T {
        self.inner
            .as_ref()
            .expect("PooledMut inner should be Some in as_ref")
    }

    /// 填充完成后转换为共享只读对象
    pub fn freeze(mut self) -> Pooled<T, Q> {
        let obj = self
            .inner
            .take()
            .expect("PooledMut inner should be Some in freeze");
        let shared = Arc::new(SharedInner {
            obj: Some(obj),
            pool: self.pool.clone(),
        });
        Pooled { inner: shared }
    }
}

impl<T: Send, Q: Queue<Box<T>>> std::ops::Deref for PooledMut<T, Q> {
    type Target = T;
    fn deref(&self) -> &T {
        self.as_ref()
    }
}

impl<T: Send, Q: Queue<Box<T>>> std::ops::DerefMut for PooledMut<T, Q> {
    fn deref_mut(&mut self) -> &mut T {
        self.as_mut()
    }
}

impl<T: Send, Q: Queue<Box<T>>> Drop for PooledMut<T, Q> {
    fn drop(&mut self) {
        if let Some(obj) = self.inner.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.put(obj);
            }
        }
    }
}

/// 共享内部结构，由 Arc 管理，在 Drop 时归还 Box 给池
struct SharedInner<T: Send, Q: Queue<Box<T>>> {
    obj: Option<Box<T>>,
    pool: Weak<Pool<T, Q>>,
}

impl<T: Send, Q: Queue<Box<T>>> Drop for SharedInner<T, Q> {
    fn drop(&mut self) {
        if let Some(obj) = self.obj.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.put(obj);
            }
        }
    }
}

/// 共享只读对象：可克隆，最后一个销毁时自动归还
pub struct Pooled<T: Send, Q: Queue<Box<T>> = SegQueue<Box<T>>> {
    inner: Arc<SharedInner<T, Q>>,
}

impl<T: Send, Q: Queue<Box<T>>> Clone for Pooled<T, Q> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Send, Q: Queue<Box<T>>> std::ops::Deref for Pooled<T, Q> {
    type Target = T;
    fn deref(&self) -> &T {
        self.inner
            .obj
            .as_ref()
            .expect("Pooled inner obj should be Some in deref")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[derive(Debug, Default)]
    struct TickData {
        tradeday: String,
        inst: String,
        datetime: String,
        last: f64,
        volume: i64,
        openint: i64,
        a1: f64,
        b1: f64,
        av1: i64,
        bv1: i64,
    }

    impl TickData {
        fn fill(
            &mut self,
            tradeday: &str,
            inst: &str,
            datetime: &str,
            last: f64,
            volume: i64,
            openint: i64,
            a1: f64,
            b1: f64,
            av1: i64,
            bv1: i64,
        ) {
            self.tradeday = tradeday.to_string();
            self.inst = inst.to_string();
            self.datetime = datetime.to_string();
            self.last = last;
            self.volume = volume;
            self.openint = openint;
            self.a1 = a1;
            self.b1 = b1;
            self.av1 = av1;
            self.bv1 = bv1;
        }
    }

    #[test]
    fn test_pool() {
        let pool = Pool::<TickData>::new_unbounded();

        // 第一次获取
        let mut guard = pool.get();
        guard.fill(
            "2024-05-20",
            "ru2309",
            "10:00:00.123",
            6800.0,
            1000,
            50000,
            6799.0,
            6801.0,
            500,
            600,
        );
        let tick = guard.freeze();

        let tick1 = tick.clone();
        let tick2 = tick.clone();
        drop(tick1);
        drop(tick2);
        drop(tick); // 最后一个引用，触发归还

        // 此时池中应有一个空闲对象
        let (idle, _, _created) = pool.status();
        assert_eq!(idle, 1);

        // 第二次获取应复用
        let mut guard2 = pool.get();
        guard2.fill(
            "2024-05-20",
            "ru2305",
            "10:00:00.456",
            6801.0,
            1200,
            50100,
            6800.0,
            6802.0,
            400,
            700,
        );
        let _tick2 = guard2.freeze();

        let (_idle2, _, created2) = pool.status();
        // 创建数应为1（只创建了一次）
        assert_eq!(created2, 1);
    }

    #[test]
    fn test_cross_thread_release() {
        let pool = Pool::<TickData>::new_unbounded();

        // 初始状态：空闲0，创建0
        let (idle0, _, created0) = pool.status();
        assert_eq!(idle0, 0);
        assert_eq!(created0, 0);

        // 主线程获取一个对象并填充
        let mut guard = pool.get();
        guard.fill(
            "2024-05-20",
            "ru2305",
            "10:00:00.123",
            6800.0,
            1000,
            50000,
            6799.0,
            6801.0,
            500,
            600,
        );
        let tick = guard.freeze();

        // 此时池中空闲0，创建1
        let (idle1, _, created1) = pool.status();
        assert_eq!(idle1, 0);
        assert_eq!(created1, 1);

        // 将 tick 移动到另一个线程并释放
        let handle = thread::spawn(move || {
            // 线程内 drop tick（作用域结束自动调用）
            drop(tick); // 显式 drop，也可省略
        });

        handle.join().unwrap();

        // 验证对象已归还
        let (idle2, _, created2) = pool.status();
        assert_eq!(idle2, 1); // 空闲数变为1
        assert_eq!(created2, 1); // 创建数不变
    }
}
