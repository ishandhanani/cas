// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Monotonic sleeps for live request scheduling.
//!
//! Linux uses a one-shot `timerfd` through Tokio's I/O reactor. Other platforms
//! use Tokio's timer. The scheduler keeps one timer active, so timer file
//! descriptors do not scale with the number of requests in a trace.

use std::time::Instant;

/// Name of the active timer implementation for run artifacts.
pub(crate) const TIMER_BACKEND: &str = if cfg!(target_os = "linux") {
    "linux-timerfd"
} else {
    "tokio-time"
};

/// Wait until `deadline` without returning before it.
pub(crate) async fn sleep_until(deadline: Instant) {
    loop {
        let now = Instant::now();
        let Some(remaining) = deadline.checked_duration_since(now) else {
            return;
        };
        if remaining.is_zero() {
            return;
        }
        sleep_ns(remaining.as_nanos().min(i64::MAX as u128) as i64).await;
    }
}

#[cfg(target_os = "linux")]
async fn sleep_ns(duration_ns: i64) {
    if duration_ns <= 0 {
        return;
    }

    let started = Instant::now();
    if timerfd_sleep_ns(duration_ns).await.is_ok() {
        return;
    }

    let remaining_ns = (duration_ns as u128).saturating_sub(started.elapsed().as_nanos());
    if remaining_ns > 0 {
        tokio::time::sleep(std::time::Duration::from_nanos(
            remaining_ns.min(u64::MAX as u128) as u64,
        ))
        .await;
    }
}

#[cfg(target_os = "linux")]
async fn timerfd_sleep_ns(duration_ns: i64) -> std::io::Result<()> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use tokio::io::unix::AsyncFd;

    const NANOS_PER_SECOND: i64 = 1_000_000_000;

    let owned = unsafe {
        let fd = libc::timerfd_create(
            libc::CLOCK_MONOTONIC,
            libc::TFD_NONBLOCK | libc::TFD_CLOEXEC,
        );
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let owned = OwnedFd::from_raw_fd(fd);
        let specification = libc::itimerspec {
            it_interval: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            it_value: libc::timespec {
                tv_sec: (duration_ns / NANOS_PER_SECOND) as libc::time_t,
                tv_nsec: (duration_ns % NANOS_PER_SECOND) as libc::c_long,
            },
        };
        if libc::timerfd_settime(fd, 0, &specification, std::ptr::null_mut()) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        owned
    };

    let timer = AsyncFd::new(owned)?;
    loop {
        let mut ready = timer.readable().await?;
        let fd = timer.get_ref().as_raw_fd();
        match ready.try_io(|_| {
            let mut expirations = 0_u64;
            let read = unsafe {
                libc::read(
                    fd,
                    (&mut expirations as *mut u64).cast::<libc::c_void>(),
                    std::mem::size_of::<u64>(),
                )
            };
            if read < 0 {
                Err(std::io::Error::last_os_error())
            } else if read as usize != std::mem::size_of::<u64>() {
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "timerfd returned a short read",
                ))
            } else {
                Ok(())
            }
        }) {
            Ok(result) => return result,
            Err(_) => continue,
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn sleep_ns(duration_ns: i64) {
    if duration_ns > 0 {
        tokio::time::sleep(std::time::Duration::from_nanos(duration_ns as u64)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deadline_sleep_does_not_return_early() {
        let started = Instant::now();
        let deadline = started + std::time::Duration::from_millis(2);
        sleep_until(deadline).await;
        assert!(Instant::now() >= deadline);
    }
}
