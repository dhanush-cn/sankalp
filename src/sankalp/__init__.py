"""Sankalp -- durable saga orchestrator for money movement.

Kill any process at any instant: workflows resume from the last completed step,
and no step's side effect executes twice. That is exactly-once *effects*, via
at-least-once execution plus idempotency -- never "exactly-once delivery".
"""

__version__ = "0.1.0"
