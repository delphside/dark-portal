//! The snapshot parser's memory stays flat in the snapshot's size
//! ([5.2.1](../docs/5.2.1-collection.md#the-snapshot), step 5).
//!
//! The body is generated on the fly (never held, never a file), parsed at
//! 30,000 and at 300,000 listings, and the peak the Rust heap reaches above
//! where it started is compared. This file is its own test binary with one
//! test, so no other test's allocations are in the count.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::DateTime;
use dp::collect::snapshot::{SnapshotMeta, parse_listings, write_parquet};

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if new_size >= layout.size() {
                let now = CURRENT.fetch_add(new_size - layout.size(), Ordering::Relaxed)
                    + (new_size - layout.size());
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// A snapshot body of `listings` listings, produced as it is read.
struct Body {
    listings: u64,
    next: u64,
    pending: Vec<u8>,
    position: usize,
    bytes: u64,
}

impl Body {
    fn new(listings: u64) -> Body {
        Body {
            listings,
            next: 0,
            pending: br#"{"_links":{"self":{"href":"https://example.invalid"}},"auctions":["#
                .to_vec(),
            position: 0,
            bytes: 0,
        }
    }
}

impl Read for Body {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position == self.pending.len() {
            self.pending.clear();
            self.position = 0;
            if self.next < self.listings {
                let i = self.next;
                let separator = if i == 0 { "" } else { "," };
                let time_left = ["SHORT", "MEDIUM", "LONG", "VERY_LONG"][(i % 4) as usize];
                self.pending.extend_from_slice(
                    format!(
                        r#"{separator}{{"id":{},"item":{{"id":{}}},"quantity":{},"unit_price":{},"time_left":"{time_left}"}}"#,
                        1_000_000_000 + i,
                        100 + i % 400,
                        1 + i % 200,
                        10_000 + i * 7 % 900_000,
                    )
                    .as_bytes(),
                );
                self.next += 1;
            } else if self.next == self.listings {
                self.pending.extend_from_slice(b"]}");
                self.next += 1;
            } else {
                return Ok(0);
            }
        }
        let n = buf.len().min(self.pending.len() - self.position);
        buf[..n].copy_from_slice(&self.pending[self.position..self.position + n]);
        self.position += n;
        self.bytes += n as u64;
        Ok(n)
    }
}

/// Parses `listings` listings and returns the heap's peak above its start, and
/// the body's size.
fn parse_peak(listings: u64) -> (usize, u64, u64) {
    let baseline = CURRENT.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let mut body = Body::new(listings);
    let mut count = 0u64;
    let summary = parse_listings(&mut body, &mut |_| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(summary.rows, listings);
    assert_eq!(count, listings);
    (PEAK.load(Ordering::Relaxed) - baseline, body.bytes, count)
}

fn high_water_mark_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .map(|v| v.trim().trim_end_matches(" kB").trim().parse().unwrap())
        .unwrap()
}

#[test]
fn parsing_300_000_listings_takes_no_more_memory_than_30_000() {
    // Warm up, so one-off allocations (the buffer, the logger) are in both.
    parse_peak(1_000);

    let (small, small_bytes, _) = parse_peak(30_000);
    let (large, large_bytes, rows) = parse_peak(300_000);
    eprintln!(
        "parse peak heap: {small} B for {small_bytes} B of body (30,000); \
         {large} B for {large_bytes} B of body ({rows})"
    );

    assert!(large_bytes > 25_000_000, "the large body is about 30 MB");
    // Flat: ten times the listings, the same peak to within a page or two.
    assert!(large <= small + 16 * 1024, "{small} -> {large}");
    // And nowhere near the body, which a whole-body parse would hold.
    assert!(large < 1_000_000, "{large}");

    // The whole path, to a Parquet file: process high-water mark (RSS), which
    // includes DuckDB's own memory, before and after 300,000 rows.
    let before = high_water_mark_kib();
    let directory = std::env::temp_dir().join(format!("dp-memory-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let file = directory.join("snapshot.parquet.tmp");
    let moment = DateTime::parse_from_rfc3339("2026-11-26T10:00:00Z")
        .unwrap()
        .to_utc();
    let meta = SnapshotMeta {
        hour: moment,
        last_modified: moment,
        fetched_at: moment,
    };
    let summary = match write_parquet(Body::new(300_000), &meta, &file) {
        Ok(summary) => summary,
        Err(_) => panic!("the write failed"),
    };
    let after = high_water_mark_kib();
    eprintln!(
        "whole path: {} rows, process peak RSS {before} KiB -> {after} KiB, file {} B",
        summary.rows,
        std::fs::metadata(&file).unwrap().len()
    );
    assert_eq!(summary.rows, 300_000);
}
