use std::future::Future;
use std::sync::{Arc, Condvar, Mutex};
use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll, Wake, Waker};
use std::pin::Pin;

#[derive(Copy, Clone, Hash, PartialEq, Eq)]
struct TaskId(usize);

struct ReadyQueue {
    queue: Mutex<VecDeque<TaskId>>,
    condvar: Condvar,
}

struct TaskWaker {
    id: TaskId,
    ready_queue: Arc<ReadyQueue>,
}

struct Counter {
    current: u32,
    max: u32,
}

impl TaskWaker {
    fn new(id: TaskId, ready_queue: Arc<ReadyQueue>) -> Self {
        TaskWaker {
            id,
            ready_queue,
        }
    }
}

pub struct Executor {
    tasks: HashMap<TaskId, Pin<Box<dyn Future<Output = ()>>>>,
    ready_queue: Arc<ReadyQueue>,
    next_id: usize,
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            tasks: HashMap::new(),
            ready_queue: Arc::new(
                    ReadyQueue {
                    queue: Mutex::new(VecDeque::new()),
                    condvar: Condvar::new(),
                }
            ),
            next_id: 1,
        }
    }

    pub fn spawn<F: Future<Output = ()> + 'static>(&mut self, future: F)  {
        let task_id = TaskId(self.next_id);
        self.next_id += 1;
        self.tasks.insert(task_id, Box::pin(future));
        self.ready_queue.queue.lock().unwrap().push_back(task_id);
        self.ready_queue.condvar.notify_one();
    }

    pub fn run(&mut self) {
        loop {
            if self.tasks.is_empty() {
                break;
            }

            let mut queue = self.ready_queue.queue.lock().unwrap();
            while queue.is_empty() {
                queue = self.ready_queue.condvar.wait(queue).unwrap();
            }

            while let Some(task_id) = queue.pop_front() {
                drop(queue);

                let waker: Waker = Arc::new(TaskWaker::new(task_id, self.ready_queue.clone())).into();
                let mut cx = Context::from_waker(&waker);

                let task = self.tasks.get_mut(&task_id).unwrap();
                match task.as_mut().poll(&mut cx) {
                    Poll::Pending => {
                    },
                    Poll::Ready(_) => {
                        self.tasks.remove(&task_id);
                    },
                }
                queue = self.ready_queue.queue.lock().unwrap();
            }
        }
    }
}

impl Future for Counter {
    type Output = ();
 
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.current += 1;
        println!("Count: {}", self.current);
        if self.current < self.max {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

impl Counter {
    fn new(max: u32) -> Self {
        Counter {
            current: 0,
            max,
        }
    }
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.ready_queue.queue.lock().unwrap().push_back(self.id);
        self.ready_queue.condvar.notify_one();
    }
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

