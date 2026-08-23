"""The error taxonomy the engine branches on.

One branch in the worker loop decides whether money moves twice or gets stranded
(docs/spec.md, "Retry vs. compensate"):

    RetryableError and attempt < max_attempts  ->  back on the queue, completed steps
                                                   stay checkpointed, backoff applies
    anything else                              ->  COMPENSATING, unwind what ran

So the default for an *unrecognised* exception is deliberately terminal. An unknown
failure mode is not evidence that retrying is safe -- a step that blew up somewhere
unexpected may well have already moved money -- and compensation is idempotent by
contract while a wrong retry is not. Steps opt into retrying, never out of it.

``TerminalError`` exists even though "not retryable" is the default, because raising it
says *this failed for good* rather than *this crashed*: an insufficient-balance or
account-frozen response is a legitimate business outcome, and reading `raise
TerminalError(...)` at the call site is how the next person knows it was decided rather
than accidental.
"""

from __future__ import annotations


class SankalpError(Exception):
    """Base for every error this codebase raises on purpose."""


class RetryableError(SankalpError):
    """The step failed in a way that may succeed on a later attempt.

    Timeouts, connection resets, HTTP 429/503, a downstream circuit that is open. The
    workflow returns to the queue with exponential backoff and jitter, and the steps that
    already committed a ``step_outputs`` row are skipped on replay rather than re-run.

    Raise this only when re-running the whole step is safe. If the step may have already
    applied its side effect, either make the step itself idempotent (the usual answer --
    a natural key plus ``ON CONFLICT DO NOTHING``) or treat the failure as terminal.
    """


class TerminalError(SankalpError):
    """The step failed for good; retrying it would fail the same way.

    Insufficient funds, a frozen account, a validation rejection, a 4xx that is about the
    request rather than the transport. The workflow moves to ``COMPENSATING`` and the
    completed forward steps unwind in reverse ``seq`` order.
    """


class WorkflowDefinitionError(SankalpError):
    """A workflow or step is declared wrongly.

    Raised at decoration or registration time -- i.e. at import, before any workflow
    exists -- so the worker's retry/compensate branch never sees one. A missed ``seq``, a
    duplicated one, a step that is not a coroutine function: all of these are bugs in the
    definition, and the process should refuse to start rather than discover them halfway
    through moving money.
    """
