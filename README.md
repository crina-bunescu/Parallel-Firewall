# Parallel Firewall

A multi-threaded network packet firewall built in C using the POSIX threading API. Packets produced by a dedicated thread are filtered in parallel by a configurable pool of consumer threads via a shared ring buffer, and results are written to a log file sorted by arrival timestamp — without any post-processing sort.

> **Assignment context:** Operating Systems course, UNSTPB. Full requirements are in [`requirements.md`](requirements.md).

---

## How It Works

A **producer thread** generates packets and enqueues them into a **lock-protected ring buffer**. A configurable number of **consumer threads** dequeue packets, apply a filter function (PASS / DROP), and write results to a log file in timestamp order.

Key design constraints enforced in the implementation:
- No busy-waiting — consumers block on a condition variable until new packets are available.
- Log entries are written in ascending timestamp order **during** processing, not after.
- The number of live threads is always `num_consumers + 1` (producer + consumers).

---

## Implementation

All implementation files are in `src/`:

| File | Description |
|---|---|
| `ring_buffer.c` / `.h` | Thread-safe circular buffer with mutex + condition variable synchronization |
| `consumer.c` / `.h` | Consumer thread lifecycle: dequeue, filter, log |
| `firewall.c` | Entry point — argument parsing, thread creation and teardown |
| `packet.c` / `.h` | Packet hashing and filter logic (provided) |
| `serial.c` | Reference single-threaded implementation (provided) |

---

## Build & Run

```bash
cd src/
make
```

This produces two binaries: `serial` (reference) and `firewall` (parallel implementation).

```bash
# Run the parallel firewall
./firewall <input_file> <output_log> <num_consumers>

# Example
./firewall ../tests/in/test_1_000.in out.log 4
```

---

## Testing

```bash
cd tests/
make check
```

Expected output when all tests pass:

```
Test [    10 packets, sort False, 1 thread ] .... passed ...  3
Test [ 1,000 packets, sort False, 1 thread ] .... passed ...  3
Test [20,000 packets, sort False, 1 thread ] .... passed ...  4
Test [    10 packets, sort True , 2 threads] .... passed ...  5
...
Checker:                                                    90/100
```

For linting:

```bash
cd tests/
make lint
```

> The easiest way to reproduce the grading environment is via the provided Docker setup — see [`README.checker.md`](README.checker.md).
