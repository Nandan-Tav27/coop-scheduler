pub enum TaskResult {
    Yield,
    Done,
}

pub trait Task {
    fn poll(&mut self) -> TaskResult;
}

pub struct Scheduler {
    tasks: Vec<Box<dyn Task>>,
}

pub struct Counter {
    current: u32,
    max: u32,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            tasks: Vec::new(),
        }
    }

    pub fn spawn<F>(&mut self, task: F)
    where F: Task + 'static,
    {
        self.tasks.push(Box::new(task));
    }

    pub fn run(&mut self) {
        loop {
            // Check if queue is empty
            if self.tasks.is_empty() {
                break;
            }

            // Iterate through tasks, run them, do a match on the output
            let mut new_tasks = Vec::new();
            for mut task in self.tasks.drain(..) {
                match task.poll() {
                    TaskResult::Done => {},
                    TaskResult::Yield => {
                        new_tasks.push(task);
                    },
                }
            }
            self.tasks = new_tasks;
        }
    }
}

impl Task for Counter {
    fn poll(&mut self) -> TaskResult {
        self.current += 1;
        println!("Count: {}", self.current);
        if self.current < self.max {
            TaskResult::Yield
        } else {
            TaskResult::Done
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let mut scheduler = Scheduler::new();

        scheduler.spawn(Counter::new(3));
        scheduler.run();
    }

    #[test]
    fn two_counters_interleaved() {
        let mut scheduler = Scheduler::new();

        scheduler.spawn(Counter::new(3));
        scheduler.spawn(Counter::new(3));
        scheduler.run();
    }
}
