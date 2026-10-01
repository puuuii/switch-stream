use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

#[derive(Clone, Default)]
pub struct Shutdown(Arc<Inner>);

#[derive(Default)]
struct Inner {
    flag: AtomicBool,
    lock: Mutex<()>,
    wakeup: Condvar,
}

impl Shutdown {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_set(&self) -> bool {
        self.0.flag.load(Ordering::Acquire)
    }

    pub fn trigger(&self) {
        self.0.flag.store(true, Ordering::Release);
        let _guard = self.0.lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.0.wakeup.notify_all();
    }

    /// 最大`timeout`待ち、停止要求が来ていれば即座に起きてtrueを返す。
    pub fn wait(&self, timeout: Duration) -> bool {
        let guard = self.0.lock.lock().unwrap_or_else(PoisonError::into_inner);
        let _ = self
            .0
            .wakeup
            .wait_timeout_while(guard, timeout, |_| !self.is_set());
        self.is_set()
    }
}
