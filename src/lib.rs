use std::future::Future;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::pin::{Pin, pin};

pub struct Counter {
    current: u32,
    max: u32,
}

impl Future for Counter {
    type Output = u32;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.current += 1;
        println!("Count: {}", self.current);
        if self.current < self.max {
            Poll::Pending
        } else {
            Poll::Ready(self.max)
        }
    }
}

impl Counter {
    pub fn new(max: u32) -> Self {
        Counter {
            current: 0,
            max,
        }
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);

    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Pending => {},
            Poll::Ready(val) => return val,
        }
    }
}

// Claude
fn dummy_waker() -> Waker {
    fn no_op(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker { dummy_raw_waker() }
    fn dummy_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &RawWakerVTable::new(clone, no_op, no_op, no_op))
    }
    unsafe { Waker::from_raw(dummy_raw_waker()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_future() {
        let counter = Counter::new(3);
        let counter_result = block_on(counter);
        assert_eq!(counter_result, 3);
    }
}

