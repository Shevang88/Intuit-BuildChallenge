# Assignment 1: Producer-Consumer (Python)

This project implements a classic producer-consumer pattern with explicit thread coordination using a bounded buffer.

## How to run
- Demo: `python3 producer_consumer.py` (prints final destination contents)

## How to test
- Run all tests: `python3 -m unittest -v`

## Notes
- `BoundedBuffer` blocks producers when full and consumers when empty using a `Condition`.
- `Producer` and `Consumer` threads support clean shutdown; consumers can transform items via `process_fn`.
