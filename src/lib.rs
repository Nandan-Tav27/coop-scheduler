pub enum TaskResult {
    Yield,
    Done,
}

pub struct Scheduler {
    tasks: Vec<Box<dyn FnMut() -> TaskResult>>,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            tasks: Vec::new(),
        }
    }

    pub fn spawn<F>(&mut self, task: F)
    where F: FnMut() -> TaskResult + 'static,
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
                match task() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_two_tasks() {
        let mut scheduler = Scheduler::new();

        scheduler.spawn(|| { println!("Task A"); TaskResult::Done });
        scheduler.spawn(|| { println!("Task B"); TaskResult::Done });

        scheduler.run();
    }

    // Claude
    #[test]
    fn round_robin_three_tasks() {
        let mut scheduler = Scheduler::new();
        let log = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));

        for name in ["A", "B", "C"] {
            let log = log.clone();
            scheduler.spawn(move || {
                log.borrow_mut().push(name);
                TaskResult::Done
            });
        }

        scheduler.run();

        assert_eq!(*log.borrow(), vec!["A", "B", "C"]);
    }
}
