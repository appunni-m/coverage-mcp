from __future__ import annotations

from typing import Annotated, Literal, Required, TypedDict

from pydantic import Field

MIN_SUMMARY_LINES = 1
MAX_SUMMARY_LINES = 500
MIN_TIMEOUT_SECONDS = 1
MAX_TIMEOUT_SECONDS = 86400

CoverageFormat = Annotated[
    Literal[
        "auto",
        "lcov",
        "coverage.py",
        "coveragepy",
        "coverage-json",
        "coveragepy-json",
        "cobertura",
        "jacoco",
        "istanbul",
        "nyc",
        "go",
        "go-cover",
        "go-coverprofile",
        "coverprofile",
        "llvm",
        "llvm-json",
    ],
    Field(description="Coverage report format. Use auto to detect it from the artifact."),
]

TopologyKind = Annotated[
    Literal[
        "project",
        "repo",
        "repository",
        "command",
        "registered_command",
        "test_command",
        "run",
        "snapshot",
        "coverage_snapshot",
        "worktree",
    ],
    Field(description="Object category whose computed project/run/worktree topology should be returned."),
]


class ArtifactSpec(TypedDict, total=False):
    path: Required[
        Annotated[str, Field(min_length=1, description="Artifact path, absolute or relative to command cwd.")]
    ]
    required: Annotated[bool, Field(description="Whether a missing artifact should be reported as expected output.")]
    coverage_format: Annotated[
        CoverageFormat | None,
        Field(description="Supported coverage parser format for this artifact, or null for non-coverage artifacts."),
    ]


ArtifactPaths = Annotated[
    dict[str, str | ArtifactSpec] | None,
    Field(
        description=(
            "Artifacts keyed by kind. Each value is either a path string or an object with path, required, and "
            "coverage_format. Relative paths resolve from command cwd."
        )
    ),
]

ResultLimit = Annotated[int, Field(ge=1, le=1000, description="Maximum records to return (1-1000).")]
FileLimit = Annotated[int, Field(ge=1, le=5000, description="Maximum file records to return (1-5000).")]
InsightLimit = Annotated[int, Field(ge=1, le=50, description="Maximum prioritized insight items to return (1-50).")]
TrendLimit = Annotated[int, Field(ge=1, le=2000, description="Maximum time-series points to return (1-2000).")]
ChangedLineLimit = Annotated[int, Field(ge=1, le=5000, description="Maximum changed line records to return (1-5000).")]
HistoryLimit = Annotated[int, Field(ge=1, le=1000, description="Maximum line-history points to return (1-1000).")]
ComparisonFileLimit = Annotated[
    int,
    Field(ge=1, le=1000, description="Maximum changed file records in a snapshot comparison (1-1000)."),
]
ComparisonLineLimit = Annotated[
    int,
    Field(ge=1, le=5000, description="Maximum changed line records in a snapshot comparison (1-5000)."),
]
SummaryLineLimit = Annotated[
    int,
    Field(
        ge=MIN_SUMMARY_LINES,
        le=MAX_SUMMARY_LINES,
        description="Maximum bounded stdout/stderr excerpt lines to return (1-500).",
    ),
]
TimeoutSeconds = Annotated[
    int | None,
    Field(
        ge=MIN_TIMEOUT_SECONDS,
        le=MAX_TIMEOUT_SECONDS,
        description="Process timeout in seconds (1-86400), or null for no timeout.",
    ),
]
IdempotencyKey = Annotated[
    str | None,
    Field(
        min_length=1,
        max_length=200,
        description="Stable key for one intended run, scoped to the registered command; null disables deduplication.",
    ),
]
PositiveLineNumber = Annotated[int, Field(ge=1, description="One-based source line number.")]
SourceBoundary = Annotated[
    int,
    Field(ge=1, description="One-based source boundary; responses contain at most 200 lines."),
]

NonEmptyName = Annotated[str, Field(min_length=1, description="Human-readable registered command name.")]
CommandText = Annotated[str, Field(min_length=1, description="Complete shell command exactly as approved by a human.")]
CommandReference = Annotated[
    str,
    Field(min_length=1, description="Registered command UUID or its latest matching name."),
]
OptionalCommandReference = Annotated[
    str | None,
    Field(description="Registered command UUID or name; null searches across all commands."),
]
RunId = Annotated[str, Field(min_length=1, description="Durable run UUID returned by run_command_profiled.")]
SnapshotId = Annotated[str, Field(min_length=1, description="Immutable coverage snapshot UUID.")]
OptionalSnapshotId = Annotated[
    str | None,
    Field(description="Immutable coverage snapshot UUID, or null to select according to the tool's documented mode."),
]
WorktreeId = Annotated[str, Field(min_length=1, description="Registered worktree UUID returned by register_worktree.")]
OptionalWorktreeId = Annotated[
    str | None,
    Field(description="Registered worktree UUID; null selects direct snapshot-comparison mode."),
]
FilePath = Annotated[
    str,
    Field(min_length=1, description="Repository-relative source path as stored in the coverage report."),
]
OptionalFilePath = Annotated[
    str | None,
    Field(description="Repository-relative source path filter; null includes every file."),
]
RepoPath = Annotated[
    str | None,
    Field(description="Repository or checkout path used to resolve the shared Git project identity."),
]
Branch = Annotated[str | None, Field(description="Exact Git branch filter or metadata; null means any or auto-detect.")]
CommitSha = Annotated[str | None, Field(description="Exact Git commit SHA; null asks Coverage MCP to auto-detect it.")]
Suite = Annotated[
    str,
    Field(min_length=1, description="Stable coverage suite name used for trends and baseline matching."),
]
OptionalSuite = Annotated[str | None, Field(description="Suite filter; null selects the latest applicable suite.")]

ReportPath = Annotated[
    str,
    Field(min_length=1, description="Local path to the coverage artifact that the server process can read."),
]
WorktreePath = Annotated[
    str,
    Field(min_length=1, description="Absolute or resolvable path to the linked worktree being registered."),
]
BaseRef = Annotated[
    str,
    Field(
        min_length=1,
        description="Reference branch or revision whose current coverage snapshot becomes frozen baseline.",
    ),
]
OptionalBaseRef = Annotated[
    str | None,
    Field(description="Reference branch/revision metadata stored on the snapshot; null leaves it unspecified."),
]
OptionalLabel = Annotated[
    str | None,
    Field(description="Optional human-readable label; null uses generated identity."),
]
ShellPath = Annotated[
    str,
    Field(min_length=1, description="Shell executable used to run the complete approved command."),
]
HumanApproval = Annotated[
    Literal[True],
    Field(description="Must be true only after a human approved the exact command, cwd, shell, and artifacts."),
]
ApprovedBy = Annotated[
    str,
    Field(min_length=1, description="Human identity or auditable label that granted command approval."),
]
ApprovalNote = Annotated[
    str,
    Field(min_length=1, description="Specific reason recording what exact command and artifacts were approved."),
]
CommandCwd = Annotated[
    str | None,
    Field(description="Command working directory; null resolves the server's current directory."),
]
WaitForCompletion = Annotated[
    bool,
    Field(description="When true, block the MCP call until terminal; normally false so callers poll run_result."),
]
IncludeLines = Annotated[
    bool,
    Field(description="Include up to 5,000 exact line records in addition to file totals."),
]
OnlyRegressions = Annotated[
    bool,
    Field(description="When true, return only lines that changed from covered to uncovered."),
]
ArtifactKind = Annotated[
    str,
    Field(min_length=1, description="Artifact kind key used when the command was registered."),
]
ObjectReference = Annotated[
    str,
    Field(
        min_length=1,
        description="Identifier accepted for the selected object kind, such as UUID, name, or repo path.",
    ),
]
