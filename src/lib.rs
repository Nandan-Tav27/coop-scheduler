use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};

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

impl TaskWaker {
    fn new(id: TaskId, ready_queue: Arc<ReadyQueue>) -> Self {
        TaskWaker { id, ready_queue }
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
            ready_queue: Arc::new(ReadyQueue {
                queue: Mutex::new(VecDeque::new()),
                condvar: Condvar::new(),
            }),
            next_id: 1,
        }
    }

    pub fn spawn<F: Future<Output = ()> + 'static>(&mut self, future: F) {
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

                let waker: Waker =
                    Arc::new(TaskWaker::new(task_id, self.ready_queue.clone())).into();
                let mut cx = Context::from_waker(&waker);

                let task = self.tasks.get_mut(&task_id).unwrap();
                match task.as_mut().poll(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(_) => {
                        self.tasks.remove(&task_id);
                    }
                }
                queue = self.ready_queue.queue.lock().unwrap();
            }
        }
    }
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.ready_queue.queue.lock().unwrap().push_back(self.id);
        self.ready_queue.condvar.notify_one();
    }
}

struct Inner<T> {
    queue: VecDeque<T>,
    waker: Option<Waker>,
}

pub struct Sender<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

pub struct Receiver<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Mutex::new(Inner {
        queue: VecDeque::new(),
        waker: None,
    }));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

impl<T> Sender<T> {
    pub fn send(&self, message: T) {
        let mut inner = self.inner.lock().unwrap();
        inner.queue.push_back(message);
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
    }
}

impl<T: 'static> Future for Receiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(message) = inner.queue.pop_front() {
            Poll::Ready(message)
        } else {
            inner.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel() {
        let mut executor = Executor::new();
        let (tx, rx) = channel::<u32>();

        executor.spawn(async move {
            tx.send(42);
        });

        executor.spawn(async move {
            let val = rx.await;
            assert_eq!(val, 42);
        });

        executor.run();
    }
}
