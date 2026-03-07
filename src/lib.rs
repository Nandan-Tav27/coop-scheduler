use std::future::Future;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::pin::Pin;

pub struct Executor {
    tasks: Vec<Pin<Box<dyn Future<Output = ()>>>>,
}

pub struct Counter {
    current: u32,
    max: u32,
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            tasks: Vec::new(),
        }
    }

    pub fn spawn<F: Future<Output = ()> + 'static>(&mut self, future: F)  {
        self.tasks.push(Box::pin(future));
    }

    pub fn run(&mut self) {
        loop {
            if self.tasks.is_empty() {
                break;
            }

            let mut new_tasks = Vec::new();
            for mut task in self.tasks.drain(..) {
                let waker = dummy_waker();
                let mut cx = Context::from_waker(&waker);

                match task.as_mut().poll(&mut cx) {
                    Poll::Pending => {
                        new_tasks.push(task)
                    },
                    Poll::Ready(_) => {},
                }
            }
            self.tasks = new_tasks;
        }
    }
}

impl Future for Counter {
    type Output = ();
 
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.current += 1;
        println!("Count: {}", self.current);
        if self.current < self.max {
            Poll::Pending
        } else {
            Poll::Ready(())
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

// fn block_on<F: Future>(future: F) -> F::Output {
//     let mut future = pin!(future);
//
//     let waker = dummy_waker();
//     let mut cx = Context::from_waker(&waker);
//
//     loop {
//         match future.as_mut().poll(&mut cx) {
//             Poll::Pending => {},
//             Poll::Ready(val) => return val,
//         }
//     }
// }

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
    fn test_executor() {
        let mut executor = Executor::new();

        executor.spawn(Counter::new(3));
        executor.spawn(Counter::new(3));
        executor.run();
    }
}

