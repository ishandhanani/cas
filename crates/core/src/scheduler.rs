// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Stable ready-time scheduling shared by recorded and generated workloads.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// One item released by [`ReadyQueue`].
#[derive(Debug)]
pub struct ReadyItem<T> {
    pub ready_at_ns: u64,
    pub ordinal: usize,
    pub value: T,
}

impl<T> PartialEq for ReadyItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.ready_at_ns == other.ready_at_ns && self.ordinal == other.ordinal
    }
}

impl<T> Eq for ReadyItem<T> {}

impl<T> Ord for ReadyItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .ready_at_ns
            .cmp(&self.ready_at_ns)
            .then_with(|| other.ordinal.cmp(&self.ordinal))
    }
}

impl<T> PartialOrd for ReadyItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Min-priority queue by ready time, then source ordinal.
#[derive(Debug)]
pub struct ReadyQueue<T> {
    heap: BinaryHeap<ReadyItem<T>>,
}

impl<T> ReadyQueue<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, ready_at_ns: u64, ordinal: usize, value: T) {
        self.heap.push(ReadyItem {
            ready_at_ns,
            ordinal,
            value,
        });
    }

    pub fn next_ready_at_ns(&self) -> Option<u64> {
        self.heap.peek().map(|item| item.ready_at_ns)
    }

    pub fn pop_due(&mut self, now_ns: u64, limit: usize) -> Vec<ReadyItem<T>> {
        let mut ready = Vec::new();
        while ready.len() < limit
            && self
                .heap
                .peek()
                .is_some_and(|item| item.ready_at_ns <= now_ns)
        {
            ready.push(self.heap.pop().expect("the ready item exists"));
        }
        ready
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pops_by_deadline_then_ordinal() {
        let mut queue = ReadyQueue::with_capacity(4);
        queue.push(20, 3, "late");
        queue.push(10, 2, "second");
        queue.push(10, 1, "first");
        queue.push(30, 0, "last");

        assert!(queue.pop_due(9, usize::MAX).is_empty());
        assert_eq!(
            queue
                .pop_due(20, usize::MAX)
                .into_iter()
                .map(|item| item.value)
                .collect::<Vec<_>>(),
            vec!["first", "second", "late"]
        );
        assert_eq!(queue.next_ready_at_ns(), Some(30));
    }

    #[test]
    fn limit_retains_due_items() {
        let mut queue = ReadyQueue::with_capacity(3);
        for ordinal in 0..3 {
            queue.push(0, ordinal, ordinal);
        }
        assert_eq!(queue.pop_due(0, 2).len(), 2);
        assert!(!queue.is_empty());
        assert_eq!(queue.pop_due(0, 2)[0].value, 2);
        assert!(queue.is_empty());
    }
}
