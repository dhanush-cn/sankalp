"""The Phase 4 chaos suite.

A package rather than a bare directory so ``from chaos.invariants import ...`` resolves
the same way ``from fleet import WorkerFleet`` does in tests/conftest.py -- pytest puts
``tests/`` on sys.path, and this makes the import explicit rather than namespace-implicit.
"""
