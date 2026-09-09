"""Unrestricted local-execution tools made available to Deep Investigator crews.

These tools intentionally run without path restrictions or sandboxes: the user
who launches the pipeline is the operator of the box, and the build crews need
real shell, file, git, and validation access to author and publish the target
project. Every execution is recorded to the dual-lobe evidence ledger (when the
pipeline is running) so validation claims are auditable rather than fabricated.

Security note: enabling these tools for a CrewAI crew grants that crew full
control over the host account. Only launch crews you trust.
"""

from ._core import BashTool, FileWriteTool, GitTool, ValidateTool

__all__ = ["BashTool", "FileWriteTool", "GitTool", "ValidateTool"]