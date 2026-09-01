"""Workflow definitions: ordered steps plus their compensations.

Importing this package is what registers them: ``@workflow`` runs at import time and
``get_definition`` can only resolve a ``workflow_type`` this process has imported. That is
why ``engine/worker.py``'s ``main()`` imports it and nothing else -- so every definition
below is available to a worker started as ``python -m sankalp.engine.worker``.

A definition that is not imported here is invisible to a worker process, which shows up as
the worker claiming a workflow, failing to resolve its type, and leaving the row to its lease.
"""

from sankalp.workflows import demo, transfer, unwind  # noqa: F401

__all__ = ["demo", "transfer", "unwind"]
