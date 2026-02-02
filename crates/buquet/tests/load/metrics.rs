//! Metrics collection for load tests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Metrics collected during load testing.
#[derive(Debug)]
pub struct LoadMetrics {
    /// Number of tasks submitted.
    submitted: AtomicU64,
    /// Number of tasks completed successfully.
    completed: AtomicU64,
    /// Number of tasks that failed.
    failed: AtomicU64,
    /// Number of claim conflicts (another worker claimed first).
    claim_conflicts: AtomicU64,
    /// Number of claim successes.
    claim_successes: AtomicU64,
    /// Total submission latency in microseconds.
    submit_latency_us: AtomicU64,
    /// Total claim latency in microseconds.
    claim_latency_us: AtomicU64,
    /// Total processing latency in microseconds.
    process_latency_us: AtomicU64,
    /// Number of LIST operations.
    list_ops: AtomicU64,
    /// Number of GET operations.
    get_ops: AtomicU64,
    /// Number of PUT operations.
    put_ops: AtomicU64,
    /// Number of DELETE operations.
    delete_ops: AtomicU64,
    /// When metrics collection started.
    start_time: Instant,
}

impl Clone for LoadMetrics {
    fn clone(&self) -> Self {
        Self {
            submitted: AtomicU64::new(self.submitted.load(Ordering::Relaxed)),
            completed: AtomicU64::new(self.completed.load(Ordering::Relaxed)),
            failed: AtomicU64::new(self.failed.load(Ordering::Relaxed)),
            claim_conflicts: AtomicU64::new(self.claim_conflicts.load(Ordering::Relaxed)),
            claim_successes: AtomicU64::new(self.claim_successes.load(Ordering::Relaxed)),
            submit_latency_us: AtomicU64::new(self.submit_latency_us.load(Ordering::Relaxed)),
            claim_latency_us: AtomicU64::new(self.claim_latency_us.load(Ordering::Relaxed)),
            process_latency_us: AtomicU64::new(self.process_latency_us.load(Ordering::Relaxed)),
            list_ops: AtomicU64::new(self.list_ops.load(Ordering::Relaxed)),
            get_ops: AtomicU64::new(self.get_ops.load(Ordering::Relaxed)),
            put_ops: AtomicU64::new(self.put_ops.load(Ordering::Relaxed)),
            delete_ops: AtomicU64::new(self.delete_ops.load(Ordering::Relaxed)),
            start_time: self.start_time,
        }
    }
}

impl LoadMetrics {
    /// Creates a new metrics collector.
    pub fn new() -> Self {
        Self {
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            claim_conflicts: AtomicU64::new(0),
            claim_successes: AtomicU64::new(0),
            submit_latency_us: AtomicU64::new(0),
            claim_latency_us: AtomicU64::new(0),
            process_latency_us: AtomicU64::new(0),
            list_ops: AtomicU64::new(0),
            get_ops: AtomicU64::new(0),
            put_ops: AtomicU64::new(0),
            delete_ops: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// Records a task submission.
    pub fn record_submit(&self, latency: Duration) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
        self.submit_latency_us
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
        // submit = PUT task + PUT ready index
        self.put_ops.fetch_add(2, Ordering::Relaxed);
    }

    /// Records a successful task completion.
    pub fn record_complete(&self, process_latency: Duration) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.process_latency_us
            .fetch_add(process_latency.as_micros() as u64, Ordering::Relaxed);
        // complete = GET task + PUT task + DELETE lease index
        self.get_ops.fetch_add(1, Ordering::Relaxed);
        self.put_ops.fetch_add(1, Ordering::Relaxed);
        self.delete_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a failed task.
    pub fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a successful claim.
    pub fn record_claim_success(&self, latency: Duration) {
        self.claim_successes.fetch_add(1, Ordering::Relaxed);
        self.claim_latency_us
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
        // claim = GET task + PUT task (CAS) + PUT lease index + DELETE ready index
        self.get_ops.fetch_add(1, Ordering::Relaxed);
        self.put_ops.fetch_add(2, Ordering::Relaxed);
        self.delete_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a claim conflict.
    pub fn record_claim_conflict(&self) {
        self.claim_conflicts.fetch_add(1, Ordering::Relaxed);
        // conflict = GET task + PUT task (failed CAS)
        self.get_ops.fetch_add(1, Ordering::Relaxed);
        self.put_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a list operation (for polling ready tasks).
    pub fn record_list(&self) {
        self.list_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the number of submitted tasks.
    pub fn submitted_tasks(&self) -> usize {
        self.submitted.load(Ordering::Relaxed) as usize
    }

    /// Returns the number of completed tasks.
    pub fn completed_tasks(&self) -> usize {
        self.completed.load(Ordering::Relaxed) as usize
    }

    /// Returns the number of failed tasks.
    pub fn failed_tasks(&self) -> usize {
        self.failed.load(Ordering::Relaxed) as usize
    }

    /// Returns the number of claim conflicts.
    pub fn claim_conflicts(&self) -> usize {
        self.claim_conflicts.load(Ordering::Relaxed) as usize
    }

    /// Returns the number of successful claims.
    pub fn claim_successes(&self) -> usize {
        self.claim_successes.load(Ordering::Relaxed) as usize
    }

    /// Returns total elapsed time since metrics started.
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Returns the throughput in tasks per second.
    pub fn throughput_tasks_per_sec(&self) -> f64 {
        let completed = self.completed.load(Ordering::Relaxed) as f64;
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            completed / elapsed
        } else {
            0.0
        }
    }

    /// Returns average submit latency in milliseconds.
    pub fn avg_submit_latency_ms(&self) -> f64 {
        let total_us = self.submit_latency_us.load(Ordering::Relaxed) as f64;
        let count = self.submitted.load(Ordering::Relaxed) as f64;
        if count > 0.0 {
            (total_us / count) / 1000.0
        } else {
            0.0
        }
    }

    /// Returns average claim latency in milliseconds.
    pub fn avg_claim_latency_ms(&self) -> f64 {
        let total_us = self.claim_latency_us.load(Ordering::Relaxed) as f64;
        let count = self.claim_successes.load(Ordering::Relaxed) as f64;
        if count > 0.0 {
            (total_us / count) / 1000.0
        } else {
            0.0
        }
    }

    /// Returns average processing latency in milliseconds.
    pub fn avg_process_latency_ms(&self) -> f64 {
        let total_us = self.process_latency_us.load(Ordering::Relaxed) as f64;
        let count = self.completed.load(Ordering::Relaxed) as f64;
        if count > 0.0 {
            (total_us / count) / 1000.0
        } else {
            0.0
        }
    }

    /// Returns the total number of S3 operations.
    pub fn total_s3_ops(&self) -> usize {
        (self.list_ops.load(Ordering::Relaxed)
            + self.get_ops.load(Ordering::Relaxed)
            + self.put_ops.load(Ordering::Relaxed)
            + self.delete_ops.load(Ordering::Relaxed)) as usize
    }

    /// Prints a formatted report of all metrics.
    pub fn print_report(&self) {
        println!("Task Metrics:");
        println!("  Submitted:  {}", self.submitted_tasks());
        println!("  Completed:  {}", self.completed_tasks());
        println!("  Failed:     {}", self.failed_tasks());
        println!();
        println!("Claim Metrics:");
        println!("  Successes:  {}", self.claim_successes());
        println!("  Conflicts:  {}", self.claim_conflicts());
        let conflict_rate = if self.claim_successes() + self.claim_conflicts() > 0 {
            (self.claim_conflicts() as f64
                / (self.claim_successes() + self.claim_conflicts()) as f64)
                * 100.0
        } else {
            0.0
        };
        println!("  Conflict Rate: {:.1}%", conflict_rate);
        println!();
        println!("Latency (avg):");
        println!("  Submit:   {:.2} ms", self.avg_submit_latency_ms());
        println!("  Claim:    {:.2} ms", self.avg_claim_latency_ms());
        println!("  Process:  {:.2} ms", self.avg_process_latency_ms());
        println!();
        println!("S3 Operations:");
        println!("  LIST: {}", self.list_ops.load(Ordering::Relaxed));
        println!("  GET:  {}", self.get_ops.load(Ordering::Relaxed));
        println!("  PUT:  {}", self.put_ops.load(Ordering::Relaxed));
        println!("  DELETE: {}", self.delete_ops.load(Ordering::Relaxed));
        println!("  Total: {}", self.total_s3_ops());
        println!();
        println!("Performance:");
        println!("  Total Time: {:.2}s", self.elapsed().as_secs_f64());
        println!(
            "  Throughput: {:.2} tasks/sec",
            self.throughput_tasks_per_sec()
        );
    }
}

impl Default for LoadMetrics {
    fn default() -> Self {
        Self::new()
    }
}
