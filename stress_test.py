"""
Bitcask Stress Test & Benchmark

Measures:
  1. Sequential write throughput
  2. Sequential read throughput (all hits)
  3. Random read throughput (mix of hits and misses)
  4. Mixed read/write workload (configurable ratio)
  5. Overwrite-heavy workload (compaction pressure)
  6. Recovery time (startup from cold)

Usage:
    python stress_test.py
    python stress_test.py --num-keys 100000 --value-size 256
"""

import argparse
import os
import random
import shutil
import string
import tempfile
import time

from bitcask import Bitcask


def random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def format_ops(ops: float) -> str:
    if ops >= 1_000_000:
        return f"{ops / 1_000_000:.2f}M ops/s"
    elif ops >= 1_000:
        return f"{ops / 1_000:.2f}K ops/s"
    else:
        return f"{ops:.2f} ops/s"


def format_bytes(b: int) -> str:
    if b >= 1_073_741_824:
        return f"{b / 1_073_741_824:.2f} GB"
    elif b >= 1_048_576:
        return f"{b / 1_048_576:.2f} MB"
    elif b >= 1024:
        return f"{b / 1024:.2f} KB"
    return f"{b} B"


def bench_sequential_writes(db: Bitcask, num_keys: int, value_size: int) -> tuple[float, list[str]]:
    """Write num_keys unique keys. Returns (elapsed, list_of_keys)."""
    keys = [f"key:{i:08d}" for i in range(num_keys)]
    value = random_string(value_size)

    start = time.perf_counter()
    for key in keys:
        db.put(key, value)
    elapsed = time.perf_counter() - start

    return elapsed, keys


def bench_sequential_reads(db: Bitcask, keys: list[str]) -> float:
    """Read every key in order. Returns elapsed."""
    start = time.perf_counter()
    for key in keys:
        val = db.get(key)
        assert val is not None, f"Missing key: {key}"
    elapsed = time.perf_counter() - start

    return elapsed


def bench_random_reads(db: Bitcask, keys: list[str], num_ops: int, miss_ratio: float = 0.1) -> float:
    """Random reads with some misses. Returns elapsed."""
    # Pre-generate the access pattern to keep it out of the timed loop
    ops = []
    for _ in range(num_ops):
        if random.random() < miss_ratio:
            ops.append(f"missing:{random.randint(0, 999999):08d}")
        else:
            ops.append(random.choice(keys))

    start = time.perf_counter()
    for key in ops:
        db.get(key)
    elapsed = time.perf_counter() - start

    return elapsed


def bench_mixed_workload(db: Bitcask, keys: list[str], num_ops: int, read_ratio: float, value_size: int) -> tuple[float, int, int]:
    """Mixed reads and writes. Returns (elapsed, reads, writes)."""
    value = random_string(value_size)
    reads = 0
    writes = 0
    next_key_id = len(keys)

    # Pre-generate ops
    ops = []
    for _ in range(num_ops):
        if random.random() < read_ratio:
            ops.append(("r", random.choice(keys)))
        else:
            new_key = f"key:{next_key_id:08d}"
            next_key_id += 1
            ops.append(("w", new_key))
            keys.append(new_key)

    start = time.perf_counter()
    for op, key in ops:
        if op == "r":
            db.get(key)
            reads += 1
        else:
            db.put(key, value)
            writes += 1
    elapsed = time.perf_counter() - start

    return elapsed, reads, writes


def bench_overwrites(db: Bitcask, keys: list[str], rounds: int, value_size: int) -> float:
    """Overwrite the same keys repeatedly. Creates compaction pressure."""
    start = time.perf_counter()
    for r in range(rounds):
        val = random_string(value_size)
        for key in keys:
            db.put(key, val)
    elapsed = time.perf_counter() - start

    return elapsed


def bench_recovery(directory: str) -> tuple[float, int]:
    """Measure cold startup time. Returns (elapsed, num_keys)."""
    start = time.perf_counter()
    db = Bitcask(directory)
    elapsed = time.perf_counter() - start
    num_keys = len(db)
    db.close()

    return elapsed, num_keys


def bench_compaction(db: Bitcask) -> tuple[float, dict]:
    """Measure compaction time. Returns (elapsed, stats_after)."""
    stats_before = db.stats()
    start = time.perf_counter()
    db.force_compact()
    elapsed = time.perf_counter() - start
    stats_after = db.stats()

    return elapsed, stats_before, stats_after


def run_benchmarks(num_keys: int, value_size: int, mixed_ops: int, overwrite_keys: int, overwrite_rounds: int):
    test_dir = tempfile.mkdtemp(prefix="bitcask_bench_")
    separator = "─" * 60

    print(f"\n{'═' * 60}")
    print(f"  BITCASK STRESS TEST")
    print(f"{'═' * 60}")
    print(f"  Keys:        {num_keys:,}")
    print(f"  Value size:  {value_size} bytes")
    print(f"  Mixed ops:   {mixed_ops:,}")
    print(f"  Data dir:    {test_dir}")
    print(f"{'═' * 60}\n")

    try:
        db = Bitcask(test_dir)

        # 1. Sequential writes
        print(f"[1/7] Sequential Writes ({num_keys:,} keys)")
        elapsed, keys = bench_sequential_writes(db, num_keys, value_size)
        ops = num_keys / elapsed
        throughput = (num_keys * (value_size + 20)) / elapsed  # approx bytes/s
        print(f"      {format_ops(ops)}  |  {elapsed:.3f}s  |  ~{format_bytes(int(throughput))}/s")
        print(f"      {db.stats()}")
        print(separator)

        # 2. Sequential reads
        print(f"[2/7] Sequential Reads ({num_keys:,} keys)")
        elapsed = bench_sequential_reads(db, keys)
        ops = num_keys / elapsed
        print(f"      {format_ops(ops)}  |  {elapsed:.3f}s")
        print(separator)

        # 3. Random reads (90% hit, 10% miss)
        random_read_ops = min(num_keys * 2, mixed_ops)
        print(f"[3/7] Random Reads ({random_read_ops:,} ops, 10% miss rate)")
        elapsed = bench_random_reads(db, keys, random_read_ops, miss_ratio=0.1)
        ops = random_read_ops / elapsed
        print(f"      {format_ops(ops)}  |  {elapsed:.3f}s")
        print(separator)

        # 4. Mixed workload (80% read, 20% write)
        print(f"[4/7] Mixed Workload ({mixed_ops:,} ops, 80/20 read/write)")
        elapsed, reads, writes = bench_mixed_workload(db, keys, mixed_ops, read_ratio=0.8, value_size=value_size)
        ops = mixed_ops / elapsed
        print(f"      {format_ops(ops)}  |  {elapsed:.3f}s  |  reads={reads:,}  writes={writes:,}")
        print(separator)

        db.close()

        # 5. Recovery (cold start)
        print(f"[5/7] Recovery (cold start)")
        elapsed, recovered_keys = bench_recovery(test_dir)
        print(f"      {recovered_keys:,} keys recovered in {elapsed:.3f}s")
        print(separator)

        # 6. Overwrite-heavy workload (compaction pressure)
        # Use a fresh dir so we can isolate compaction stats
        compact_dir = tempfile.mkdtemp(prefix="bitcask_compact_")
        db2 = Bitcask(compact_dir)

        total_overwrites = overwrite_keys * overwrite_rounds
        print(f"[6/7] Overwrite Workload ({overwrite_keys:,} keys × {overwrite_rounds} rounds = {total_overwrites:,} writes)")
        elapsed = bench_overwrites(db2, [f"ow:{i:06d}" for i in range(overwrite_keys)], overwrite_rounds, value_size)
        ops = total_overwrites / elapsed
        print(f"      {format_ops(ops)}  |  {elapsed:.3f}s")

        stats = db2.stats()
        print(f"      Dead ratio: {stats['dead_ratio']:.1%}  |  Size: {format_bytes(stats['total_size_bytes'])}")
        print(separator)

        # 7. Compaction
        print(f"[7/7] Compaction")
        elapsed, stats_before, stats_after = bench_compaction(db2)
        print(f"      Completed in {elapsed:.3f}s")
        print(f"      Before: {format_bytes(stats_before['total_size_bytes'])}  ({stats_before['num_files']} files, {stats_before['dead_ratio']:.1%} dead)")
        print(f"      After:  {format_bytes(stats_after['total_size_bytes'])}  ({stats_after['num_files']} files, {stats_after['dead_ratio']:.1%} dead)")

        if stats_before['total_size_bytes'] > 0:
            ratio = stats_after['total_size_bytes'] / stats_before['total_size_bytes']
            reclaimed = stats_before['total_size_bytes'] - stats_after['total_size_bytes']
            print(f"      Reclaimed: {format_bytes(reclaimed)}  ({1 - ratio:.1%} reduction)")

        # Verify data integrity after compaction
        for i in range(overwrite_keys):
            val = db2.get(f"ow:{i:06d}")
            assert val is not None, f"Missing key after compaction: ow:{i:06d}"
        print(f"      Integrity check: {overwrite_keys:,} keys verified")

        db2.close()
        shutil.rmtree(compact_dir)

        print(f"\n{'═' * 60}")
        print(f"  ALL BENCHMARKS COMPLETE")
        print(f"{'═' * 60}\n")

    finally:
        shutil.rmtree(test_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bitcask stress test & benchmark")
    parser.add_argument("--num-keys", type=int, default=50_000, help="Number of keys for read/write tests")
    parser.add_argument("--value-size", type=int, default=100, help="Value size in bytes")
    parser.add_argument("--mixed-ops", type=int, default=100_000, help="Number of operations for mixed workload")
    parser.add_argument("--overwrite-keys", type=int, default=5_000, help="Number of keys for overwrite test")
    parser.add_argument("--overwrite-rounds", type=int, default=10, help="Rounds of overwrites")
    args = parser.parse_args()

    run_benchmarks(
        num_keys=args.num_keys,
        value_size=args.value_size,
        mixed_ops=args.mixed_ops,
        overwrite_keys=args.overwrite_keys,
        overwrite_rounds=args.overwrite_rounds,
    )