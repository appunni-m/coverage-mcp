from __future__ import annotations

from typing import Annotated, Any, Literal, Required, TypedDict

from pydantic import BaseModel, ConfigDict, Field

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
        Field(
            description=(
                "Supported coverage parser format for this artifact. A non-null value enables freshness-guarded "
                "automatic ingestion after a normally completed managed run; null marks a non-coverage artifact."
            )
        ),
    ]
    suite: Annotated[
        str | None,
        Field(
            min_length=1,
            description="Stable coverage suite name for automatic ingestion; null defaults to the command name.",
        ),
    ]


ArtifactPaths = Annotated[
    dict[str, str | ArtifactSpec] | None,
    Field(
        description=(
            "Artifacts keyed by kind. Each value is either a path string or an object with path, required, and "
            "coverage_format/suite. Relative paths resolve from command cwd. Coverage artifacts are automatically "
            "ingested only when the managed run creates or modifies them."
        )
    ),
]


class CoverageLineRange(TypedDict):
    """One inclusive source-line window requested from coverage_file."""

    start: Annotated[int, Field(ge=1, description="First one-based line in the inclusive window.")]
    end: Annotated[int, Field(ge=1, description="Last one-based line in the inclusive window.")]


CoverageLineRanges = Annotated[
    list[CoverageLineRange] | None,
    Field(
        max_length=10,
        description=(
            "Up to 10 inclusive windows whose exact coverage records should be returned; null returns no exact lines. "
            "Windows are sorted and merge duplicates, nesting, overlap, and adjacency; their combined unique span may "
            "contain at most 200 lines."
        ),
    ),
]

ResultLimit = Annotated[int, Field(ge=1, le=1000, description="Maximum records to return (1-1000).")]
FileLimit = Annotated[int, Field(ge=1, le=5000, description="Maximum file records to return (1-5000).")]
CoverageGapRangeLimit = Annotated[
    int,
    Field(ge=1, le=100, description="Maximum contiguous coverage-gap ranges to return (1-100)."),
]
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
RunAction = Annotated[
    Literal["status", "cancel"],
    Field(description="Read durable status or request process-group cancellation."),
]
CoverageQueryView = Annotated[
    Literal["summary", "files", "file", "insights", "line_history"],
    Field(description="Coverage projection to return through the consolidated query."),
]
CoverageComparisonView = Annotated[
    Literal["overview", "files", "lines", "progress"],
    Field(description="Comparison projection to return through the consolidated query."),
]
OptionalLineNumber = Annotated[
    int | None,
    Field(ge=1, description="One-based source line for line_history; null for other views."),
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
    Field(description="When true, block the MCP call until terminal; normally false so callers poll test_run."),
]
DetailedResponse = Annotated[
    bool,
    Field(
        description=(
            "Keep false for normal agent work. Set true only when the tool documents a specific required audit or "
            "raw-provenance field that is absent from compact data; detailed output never contains logs."
        )
    ),
]
LogQuery = Annotated[
    str | list[str],
    Field(
        description=(
            "One literal text term, or up to 20 literal text terms, to find in retained stdout/stderr. "
            "Multiple terms match a line when any term is present."
        )
    ),
]
LogStream = Annotated[
    Literal["both", "stdout", "stderr"],
    Field(description="Retained stream(s) to search."),
]
LogContextLines = Annotated[
    int,
    Field(ge=0, le=10, description="Lines before and after each match (0-10)."),
]
LogMatchLimit = Annotated[
    int,
    Field(ge=1, le=20, description="Maximum matching lines to anchor returned context (1-20)."),
]
LogWordLimit = Annotated[
    int,
    Field(ge=20, le=2000, description="Maximum words returned across all matching context windows (20-2000)."),
]
CaseSensitiveLogSearch = Annotated[
    bool,
    Field(description="Match letter case exactly when true; default false performs Unicode case-folded matching."),
]
IncludeRawMetrics = Annotated[
    bool,
    Field(description="Include format-specific raw file metrics; false keeps the response compact."),
]

ResponseWordBudget = Annotated[
    int,
    Field(
        ge=50,
        le=5000,
        description="Primary maximum serialized word budget for the response (50-5000).",
    ),
]
PageCursor = Annotated[
    str | None,
    Field(
        max_length=500,
        description="Opaque continuation cursor returned by the previous response; null starts from the beginning.",
    ),
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


RunStatus = Literal[
    "queued",
    "running",
    "passed",
    "failed",
    "cancelled",
    "timeout",
    "interrupted",
    "internal_error",
]
CoverageIngestStatus = Literal[
    "not_configured",
    "pending",
    "ingested",
    "partial",
    "failed",
    "skipped_stale",
    "skipped_run_status",
    "not_recorded",
]
ArtifactIngestStatus = Literal[
    "ingested",
    "failed",
    "missing",
    "skipped_stale",
    "skipped_run_status",
]
LineChangeStatus = Literal["new", "removed", "regressed", "improved", "changed"]


class OutputModel(BaseModel):
    """Base for discoverable public response contracts with no undeclared fields."""

    model_config = ConfigDict(extra="forbid")


class CompactOutputModel(OutputModel):
    """Token-conscious response model that intentionally drops detailed storage fields."""

    model_config = ConfigDict(extra="forbid")


class ResponseContextResult(OutputModel):
    """Stable ownership and contract context attached to every public response."""

    repo_key: str = Field(description="Stable shared Git repository identity.")
    checkout_path: str = Field(description="Exact checkout selected by the connector or HTTP caller.")
    suite: str | None = Field(description="Coverage suite governing this response, when applicable.")
    schema_revision: int = Field(description="Public Coverage MCP contract revision.")


class PageResult(OutputModel):
    """Word-budgeted cursor pagination metadata."""

    returned: int = Field(description="Records returned in this response.")
    total: int | None = Field(description="Total matching records when known.")
    word_count: int = Field(description="Serialized whitespace-delimited words returned in data.")
    max_words: int = Field(description="Requested primary response word budget.")
    truncated: bool = Field(description="Whether matching records remain after this response.")
    next_cursor: str | None = Field(description="Opaque continuation cursor, or null when complete.")


class ApiEnvelope(OutputModel):
    """Shared compact envelope used by MCP, REST, resources, and the dashboard."""

    context: ResponseContextResult = Field(description="Repository, checkout, suite, and schema ownership.")
    data: Any = Field(description="Compact operation-specific response data.")
    page: PageResult | None = Field(description="Cursor metadata for collections; null for singular responses.")


class CoverageMetrics(OutputModel):
    """Line, branch, function, and region totals and rates."""

    total_lines: int = Field(description="Number of instrumented lines.")
    covered_lines: int = Field(description="Number of covered instrumented lines.")
    line_rate: float | None = Field(description="Covered-lines ratio from 0 to 1, or null when unavailable.")
    total_branches: int = Field(description="Number of instrumented branch outcomes.")
    covered_branches: int = Field(description="Number of covered branch outcomes.")
    branch_rate: float | None = Field(description="Covered-branches ratio from 0 to 1, or null when unavailable.")
    total_functions: int = Field(description="Number of instrumented functions.")
    covered_functions: int = Field(description="Number of covered functions.")
    function_rate: float | None = Field(description="Covered-functions ratio from 0 to 1, or null when unavailable.")
    total_regions: int = Field(description="Number of instrumented code regions.")
    covered_regions: int = Field(description="Number of covered code regions.")
    region_rate: float | None = Field(description="Covered-regions ratio from 0 to 1, or null when unavailable.")


class ProjectSummaryResult(OutputModel):
    """One project's latest measured state and ledger activity."""

    total_lines: int | None = Field(description="Latest instrumented lines, or null when coverage is not measured.")
    covered_lines: int | None = Field(description="Latest covered lines, or null when coverage is not measured.")
    line_rate: float | None = Field(description="Latest line rate, or null when unavailable/not measured.")
    total_branches: int | None = Field(description="Latest branch outcomes, or null when coverage is not measured.")
    covered_branches: int | None = Field(description="Latest covered branches, or null when coverage is not measured.")
    branch_rate: float | None = Field(description="Latest branch rate, or null when unavailable/not measured.")
    total_functions: int | None = Field(description="Latest functions, or null when coverage is not measured.")
    covered_functions: int | None = Field(
        description="Latest covered functions, or null when coverage is not measured."
    )
    function_rate: float | None = Field(description="Latest function rate, or null when unavailable/not measured.")
    total_regions: int | None = Field(description="Latest regions, or null when coverage is not measured.")
    covered_regions: int | None = Field(description="Latest covered regions, or null when coverage is not measured.")
    region_rate: float | None = Field(description="Latest region rate, or null when unavailable/not measured.")
    repo_key: str = Field(description="Stable shared Git project identity used across linked worktrees.")
    repo_path: str = Field(description="Main repository path that owns the shared database.")
    snapshot_count: int = Field(description="Number of immutable coverage snapshots for the project.")
    branch_count: int = Field(description="Number of distinct recorded branches.")
    first_snapshot_at: str | None = Field(description="UTC oldest snapshot time, or null when not measured.")
    latest_snapshot_id: str | None = Field(description="Newest snapshot UUID, or null when not measured.")
    latest_snapshot_at: str | None = Field(description="UTC newest snapshot time, or null when not measured.")
    latest_snapshot_age_seconds: int | None = Field(
        default=None,
        description="Whole seconds since the newest snapshot, or null when not measured.",
    )
    latest_snapshot_age: str | None = Field(
        default=None,
        description="Human-readable newest snapshot age, or null when not measured.",
    )
    latest_branch: str | None = Field(description="Branch recorded on the newest snapshot.")
    latest_commit_sha: str | None = Field(description="Commit recorded on the newest snapshot.")
    latest_suite: str | None = Field(description="Suite recorded on the newest snapshot, or null when not measured.")
    latest_format: str | None = Field(description="Newest coverage format, or null when not measured.")
    warnings: list[str] | None = Field(description="Newest parser warnings, or null when not measured.")
    command_count: int = Field(description="Number of approved command registrations for the project.")
    latest_command_at: str | None = Field(description="UTC timestamp of the newest command registration.")
    run_count: int = Field(description="Number of retained terminal runs for the project.")
    latest_run_at: str | None = Field(description="UTC timestamp of the newest terminal run.")
    latest_run_age_seconds: int | None = Field(
        default=None,
        description="Whole seconds since the newest terminal run, or null when no run exists.",
    )
    latest_run_age: str | None = Field(
        default=None,
        description="Human-readable age of the newest terminal run, or null when no run exists.",
    )
    topology: dict[str, Any] = Field(description="Computed project and latest-snapshot relationships.")


class RegisteredArtifactResult(OutputModel):
    """One artifact declaration attached to an approved command."""

    kind: str = Field(description="Stable artifact kind key used by lookup tools.")
    path: str = Field(description="Approved artifact path relative to command cwd or absolute.")
    required: bool = Field(description="Whether absence is expected to be reported as missing output.")
    coverage_format: str | None = Field(description="Parser format that enables automatic ingestion, if any.")
    suite: str | None = Field(description="Coverage suite used for automatic ingestion, if any.")


class RegisteredCommandResult(OutputModel):
    """Immutable approved test-command registration."""

    id: str = Field(description="Durable command registration UUID.")
    created_at: str = Field(description="UTC registration timestamp.")
    name: str = Field(description="Human-readable command name; names may have newer registrations.")
    command: str = Field(description="Complete approved shell command.")
    cwd: str = Field(description="Exact approved command working directory.")
    repo_path: str = Field(description="Repository path detected at registration.")
    repo_key: str = Field(description="Stable shared Git project identity.")
    branch: str | None = Field(description="Branch detected at registration.")
    commit_sha: str | None = Field(description="Commit detected at registration.")
    shell: str = Field(description="Approved shell executable.")
    approved_by: str = Field(description="Human identity or audit label that approved the command.")
    approval_note: str = Field(description="Recorded reason and scope of approval.")
    artifact_specs: list[RegisteredArtifactResult] = Field(description="Approved artifact declarations.")
    enabled: bool = Field(description="Whether this immutable registration may currently be submitted.")
    duration_estimate_ms: int | None = Field(description="Learned median-like duration estimate in milliseconds.")
    duration_p90_ms: int | None = Field(description="Learned 90th-percentile duration in milliseconds.")
    duration_sample_count: int = Field(description="Number of recent terminal samples used for estimates.")
    duration_stats_updated_at: str | None = Field(description="UTC time duration statistics were last refreshed.")
    topology: dict[str, Any] = Field(description="Computed project, registration, and artifact relationships.")


class LogExcerptResult(OutputModel):
    """One bounded diagnostic line selected from retained output."""

    stream: Literal["stdout", "stderr"] = Field(description="Log stream containing the excerpt.")
    line_number: int = Field(description="One-based line number in the retained full log.")
    text: str = Field(description="Excerpt text, truncated to 1,000 characters.")


class ParsedRunSummaryResult(OutputModel):
    """Counter-only test-process summary; retained output is searched separately."""

    status: RunStatus = Field(description="Current or terminal run state.")
    exit_code: int | None = Field(description="Process exit code, or null before completion/no process launch.")
    duration_ms: int = Field(description="Elapsed process or queue duration in milliseconds.")
    stdout_line_count: int | None = Field(
        description="Total stdout lines, or null while summary generation is deferred."
    )
    stderr_line_count: int | None = Field(
        description="Total stderr lines, or null while summary generation is deferred."
    )
    counters: dict[str, int] = Field(description="Recognized pass/fail/error counters extracted from logs.")
    truncated: bool = Field(description="Whether logs contain more lines than this bounded response.")
    stdout_path: str = Field(description="Local path to the retained complete stdout log.")
    stderr_path: str = Field(description="Local path to the retained complete stderr log.")
    summary_deferred: bool | None = Field(
        default=None,
        description="True while queued/running log counts are intentionally not scanned; absent after completion.",
    )


class RunArtifactResult(OutputModel):
    """Observed state of one declared artifact after a managed run."""

    kind: str = Field(description="Artifact kind key from the command registration.")
    path: str = Field(description="Resolved local artifact path.")
    required: bool | None = Field(default=None, description="Whether the registration marked this artifact required.")
    exists: bool = Field(description="Whether the artifact existed after the run.")
    size_bytes: int | None = Field(description="Artifact size in bytes, or null when absent.")
    coverage_format: str | None = Field(description="Configured coverage parser format, or null for non-coverage data.")
    suite: str | None = Field(description="Configured automatic-ingestion suite.")
    modified_by_run: bool = Field(description="Whether this run created or changed the artifact.")
    ingest_status: ArtifactIngestStatus | None = Field(
        description="Per-artifact ingestion decision; null for non-coverage or old records."
    )
    snapshot_id: str | None = Field(description="Created coverage snapshot UUID, or null when no snapshot was created.")
    ingest_error: str | None = Field(description="Bounded parser, freshness, or missing-artifact explanation.")


class CoverageIngestResult(OutputModel):
    """Aggregate automatic-ingestion decision for a managed run."""

    status: CoverageIngestStatus = Field(
        description=(
            "Aggregate ingestion state: not_configured, pending, ingested, partial, failed, skipped_stale, "
            "skipped_run_status, or not_recorded."
        )
    )
    configured_artifacts: int = Field(description="Number of declared coverage artifacts.")
    ingested_artifacts: int = Field(description="Number successfully converted into snapshots.")
    failed_artifacts: int = Field(description="Number that were missing or failed parsing.")
    skipped_artifacts: int = Field(description="Number skipped by freshness or run-state guards.")
    snapshot_ids: list[str] = Field(description="UUIDs created by successful automatic ingestion.")


class RunResult(OutputModel):
    """Durable queued, running, or terminal managed-run state."""

    id: str = Field(description="Durable run UUID used for polling, cancellation, and topology queries.")
    command_id: str = Field(description="Immutable approved command registration UUID.")
    command_name: str = Field(description="Registered command name.")
    command: str = Field(description="Complete executed command.")
    idempotency_key: str | None = Field(description="Caller key that deduplicates one intended execution.")
    cwd: str = Field(description="Exact execution working directory.")
    repo_path: str = Field(description="Repository path detected for this run.")
    repo_key: str = Field(description="Stable shared Git project identity.")
    branch: str | None = Field(description="Branch detected at submission.")
    commit_sha: str | None = Field(description="Commit detected at submission.")
    queued_at: str | None = Field(description="UTC submission timestamp.")
    started_at: str | None = Field(description="UTC process start timestamp, or null while queued.")
    ended_at: str | None = Field(description="UTC terminal timestamp, or null before completion.")
    duration_ms: int = Field(description="Elapsed run duration in milliseconds at response time.")
    exit_code: int | None = Field(description="Process exit code, or null before completion/no process launch.")
    status: RunStatus = Field(description="Lifecycle state of the managed run.")
    stdout_path: str = Field(description="Path to retained complete stdout.")
    stderr_path: str = Field(description="Path to retained complete stderr.")
    parsed_summary: ParsedRunSummaryResult = Field(description="Counter and line-count summary without log text.")
    artifact_paths: list[RunArtifactResult] = Field(description="Declared artifact observations and ingestion links.")
    coverage_ingest: CoverageIngestResult = Field(description="Aggregate automatic coverage-ingestion outcome.")
    terminal: bool = Field(description="True when no further polling state transition is possible.")
    poll_after_ms: int | None = Field(description="ETA-aware minimum recommended polling delay, or null when terminal.")
    queue_position: int | None = Field(description="Zero for running, one-based for queued, null when terminal.")
    execution_mode: Literal["background"] = Field(description="Runner execution mode.")
    cancellation_requested: bool = Field(description="Whether process-group cancellation has been requested.")
    cancellation_requested_at: str | None = Field(description="UTC cancellation-request time, if any.")
    duration_estimate_ms: int | None = Field(description="Historical command duration estimate in milliseconds.")
    duration_p90_ms: int | None = Field(description="Historical command 90th-percentile duration in milliseconds.")
    duration_sample_count: int = Field(description="Historical samples used for the estimate.")
    duration_stats_updated_at: str | None = Field(description="UTC time duration statistics were refreshed.")
    duration_estimate_window: int = Field(description="Maximum recent command samples used for ETA learning.")
    eta_seconds: int | None = Field(description="Estimated whole seconds to completion; zero when terminal.")
    eta: str | None = Field(description="Human-readable estimated time to completion.")
    estimated_start_at: str | None = Field(description="Estimated/actual UTC start time.")
    estimated_completion_at: str | None = Field(description="Estimated/actual UTC completion time.")
    queue_wait_estimate_seconds: int | None = Field(description="Estimated whole seconds before process start.")
    estimate_overrun_seconds: int = Field(description="Seconds running beyond the learned duration estimate.")
    eta_unavailable_reason: str | None = Field(description="Why ETA cannot be computed, such as no_command_history.")
    submission_reused: bool | None = Field(
        default=None,
        description="True when idempotency returned an existing run; present on submission responses.",
    )
    queue_duration_ms: int | None = Field(default=None, description="Milliseconds spent queued before process start.")
    timeout_seconds: int | None = Field(default=None, description="Configured process timeout for active job records.")
    error: str | None = Field(default=None, description="Bounded runner error for interrupted/internal failures.")
    age_seconds: int | None = Field(default=None, description="Whole seconds since terminal completion.")
    age: str | None = Field(default=None, description="Human-readable terminal result age.")
    queued_age_seconds: int | None = Field(default=None, description="Whole seconds since submission while queued.")
    queued_age: str | None = Field(default=None, description="Human-readable queued age.")
    running_age_seconds: int | None = Field(
        default=None, description="Whole seconds since process start while running."
    )
    running_age: str | None = Field(default=None, description="Human-readable running age.")
    topology: dict[str, Any] = Field(description="Computed project, Git, command, run, and artifact relationships.")


class CompactRunResult(CompactOutputModel):
    """Small default response for submission and polling decisions."""

    id: str = Field(description="Durable run UUID.")
    command_id: str | None = Field(description="Exact immutable command registration UUID.")
    command_name: str = Field(description="Registered command name.")
    status: RunStatus = Field(description="Current or terminal run state.")
    terminal: bool = Field(description="Whether the run can change state again.")
    duration_ms: int = Field(description="Elapsed run duration in milliseconds.")
    exit_code: int | None = Field(description="Process exit code when available.")
    counters: dict[str, int] = Field(description="Recognized test counters; empty while unavailable.")
    checkout_path: str = Field(description="Exact checkout used for the run.")
    branch: str | None = Field(description="Git branch detected at submission.")
    commit_sha: str | None = Field(description="Git commit detected at submission.")
    coverage_ingest: CoverageIngestResult = Field(description="Aggregate coverage-ingestion outcome.")
    poll_after_ms: int | None = Field(description="ETA-aware minimum polling delay; null when terminal.")
    queue_position: int | None = Field(description="Queue position, zero while running, null when terminal.")
    age_seconds: int = Field(description="Age of the current lifecycle state in whole seconds.")
    age: str = Field(description="Human-readable age of the current lifecycle state.")
    eta_seconds: int | None = Field(description="Estimated seconds to completion.")
    eta: str | None = Field(description="Human-readable estimated time to completion.")
    cancellation_requested: bool = Field(description="Whether cancellation was requested.")
    submission_reused: bool | None = Field(description="Whether idempotency reused an existing run.")
    error: str | None = Field(description="Bounded runner error for interrupted/internal failures.")
    diagnostics_available: bool = Field(description="Whether retained stdout/stderr can be searched.")


class RunLogLineResult(CompactOutputModel):
    """One bounded retained log line returned by contextual search."""

    line_number: int = Field(description="One-based line number.")
    text: str = Field(description="Log text truncated to 500 characters.")
    match: bool = Field(description="Whether this line contains any search query.")


class RunLogContextResult(CompactOutputModel):
    """One merged context window around one or more nearby matches."""

    stream: Literal["stdout", "stderr"] = Field(description="Retained stream containing this context.")
    start_line: int = Field(description="First returned one-based line number.")
    end_line: int = Field(description="Last returned one-based line number.")
    lines: list[RunLogLineResult] = Field(description="Bounded ordered context lines.")


class RunLogSearchResult(CompactOutputModel):
    """Bounded literal search over retained run output."""

    run_id: str = Field(description="Searched durable run UUID.")
    query: str | list[str] = Field(description="Literal query term or terms used for matching.")
    queries: list[str] = Field(description="Normalized literal query terms used for matching.")
    case_sensitive: bool = Field(description="Whether matching preserved case.")
    streams: list[Literal["stdout", "stderr"]] = Field(description="Streams searched.")
    match_count: int = Field(description="Total matching lines found across searched streams.")
    returned_match_count: int = Field(description="Matching lines represented in returned contexts.")
    returned_line_count: int = Field(description="Total context lines returned.")
    returned_word_count: int = Field(description="Total whitespace-delimited words returned across context lines.")
    truncated: bool = Field(description="Whether match or output limits omitted relevant context.")
    contexts: list[RunLogContextResult] = Field(description="Merged bounded windows around matches.")


RunResponse = Annotated[
    CompactRunResult | RunResult,
    Field(description="Compact run state by default, or the full durable run record when detailed is true."),
]


class LatestArtifactResult(RunArtifactResult):
    """Latest retained artifact of one kind, including producing-run context."""

    run_id: str = Field(description="UUID of the run that recorded this artifact.")
    command_id: str = Field(description="Producing approved command registration UUID.")
    command_name: str = Field(description="Producing command name.")
    repo_key: str = Field(description="Stable shared Git project identity.")
    repo_path: str = Field(description="Repository path of the producing run.")
    started_at: str = Field(description="UTC producing-run start time.")
    ended_at: str = Field(description="UTC producing-run completion time.")
    status: RunStatus = Field(description="Terminal status of the producing run.")
    exit_code: int | None = Field(description="Producing process exit code.")
    run_age_seconds: int = Field(description="Whole seconds since the producing run ended.")
    run_age: str = Field(description="Human-readable age of the producing run.")
    topology: dict[str, Any] = Field(description="Computed project, command, run, and artifact relationships.")


class TopologyResult(OutputModel):
    """Resolved relationships for a project-owned object."""

    object_kind: Literal["project", "registered_command", "run", "coverage_snapshot", "worktree"] = Field(
        description="Normalized resolved object kind."
    )
    object_ref: str = Field(description="Caller-supplied reference that was resolved.")
    topology: dict[str, Any] = Field(description="Computed project, Git, command, run, snapshot, or baseline graph.")


class SnapshotResult(CoverageMetrics):
    """Immutable parsed coverage snapshot."""

    id: str = Field(description="Immutable coverage snapshot UUID.")
    created_at: str = Field(description="UTC ingestion timestamp.")
    minute_bucket: str = Field(description="UTC minute bucket used for time-series grouping.")
    repo_path: str = Field(description="Checkout path supplied or detected at ingestion.")
    repo_key: str = Field(description="Stable shared Git project identity.")
    branch: str | None = Field(description="Branch attached to the coverage measurement.")
    commit_sha: str | None = Field(description="Commit attached to the coverage measurement.")
    base_ref: str | None = Field(description="Optional reference revision metadata.")
    suite: str = Field(description="Stable suite used for trends and baseline matching.")
    format: str = Field(description="Detected or selected coverage report format.")
    report_path: str = Field(description="Local source coverage artifact path.")
    warnings: list[str] = Field(description="Parser limitations or lossy-detail warnings.")
    metadata: dict[str, Any] = Field(description="Format-specific report metadata kept out of common metrics.")
    age_seconds: int = Field(description="Whole seconds elapsed since ingestion.")
    age: str = Field(description="Human-readable snapshot age.")
    topology: dict[str, Any] = Field(description="Computed project, Git, and snapshot relationships.")


class WorktreeResult(OutputModel):
    """Registered linked checkout with a frozen baseline anchor."""

    id: str = Field(description="Durable worktree registration UUID.")
    created_at: str = Field(description="UTC registration timestamp that freezes baseline selection.")
    name: str | None = Field(description="Optional worktree label.")
    path: str = Field(description="Registered linked-checkout path.")
    repo_path: str = Field(description="Main repository path.")
    repo_key: str = Field(description="Stable shared Git project identity.")
    branch: str | None = Field(description="Worktree branch detected at registration.")
    head_sha: str | None = Field(description="Worktree HEAD detected at registration.")
    base_ref: str = Field(description="Reference branch or revision used for baseline selection.")
    base_sha: str | None = Field(description="Resolved reference commit when available.")
    baseline_snapshot_id: str | None = Field(description="Primary frozen baseline snapshot UUID.")
    topology: dict[str, Any] = Field(description="Computed project, worktree, and frozen-baseline relationships.")


class TrendPointResult(CoverageMetrics):
    """One chronological overall or file-specific coverage point."""

    id: str = Field(description="Coverage snapshot UUID represented by this point.")
    created_at: str = Field(description="UTC ingestion timestamp.")
    minute_bucket: str = Field(description="UTC minute bucket.")
    branch: str | None = Field(description="Snapshot branch.")
    commit_sha: str | None = Field(description="Snapshot commit.")
    suite: str = Field(description="Snapshot suite.")
    file_path: str | None = Field(description="Exact selected file path, or null for project-wide metrics.")
    point_kind: Literal["baseline", "worktree"] | None = Field(
        default=None,
        description="Whether the point is the frozen baseline or an independent worktree measurement.",
    )


class MetricDeltasResult(OutputModel):
    """Coverage-rate changes from the frozen baseline."""

    line_rate: float | None = Field(description="Current minus baseline line rate.")
    branch_rate: float | None = Field(description="Current minus baseline branch rate.")
    function_rate: float | None = Field(description="Current minus baseline function rate.")
    region_rate: float | None = Field(description="Current minus baseline region rate.")


class WorktreeProgressResult(OutputModel):
    """One worktree's independent progress against its frozen parent baseline."""

    worktree: WorktreeResult = Field(description="Registered worktree and frozen baseline metadata.")
    suite: str = Field(description="Selected suite.")
    file_path: str | None = Field(description="Exact selected file path, or null for project-wide metrics.")
    baseline: TrendPointResult = Field(description="Frozen baseline point for the selected suite/path.")
    current: TrendPointResult | None = Field(description="Latest worktree point, or null when not measured.")
    deltas: MetricDeltasResult = Field(description="Current-minus-baseline rates; null means not measurable.")
    points: list[TrendPointResult] = Field(description="Chronological baseline plus worktree-only trend points.")


class CoverageFileResult(CoverageMetrics):
    """Coverage counters for one exact repository-relative path."""

    snapshot_id: str = Field(description="Owning immutable coverage snapshot UUID.")
    file_path: str = Field(description="Exact repository-relative source path.")
    raw_metrics: dict[str, Any] = Field(description="Format-specific counters not represented by common metrics.")


class CoverageFileCompactResult(CoverageMetrics):
    """Common coverage counters for one file without parser-specific payloads."""

    file_path: str = Field(description="Exact repository-relative source path.")


class CoverageGapRangeResult(OutputModel):
    """One contiguous range of lines sharing the same coverage-gap reasons."""

    start_line: int = Field(description="First one-based line in this gap range.")
    end_line: int = Field(description="Last one-based line in this gap range.")
    line_count: int = Field(description="Number of relevant coverage lines in this range.")
    reasons: list[Literal["uncovered", "partial_branch", "uncovered_function"]] = Field(
        description="Why this range needs investigation."
    )
    missed_branches: int = Field(description="Uncovered branch outcomes summed across the range.")
    missed_functions: int = Field(description="Uncovered function entries summed across the range.")


class CoverageGapSummaryResult(OutputModel):
    """Bounded grouped coverage gaps for one file."""

    total_relevant_lines: int = Field(description="All distinct lines with at least one coverage gap.")
    uncovered_line_count: int = Field(description="Counted lines that were not executed.")
    partial_branch_line_count: int = Field(description="Lines with one or more uncovered branch outcomes.")
    uncovered_function_line_count: int = Field(description="Lines with one or more uncovered function entries.")
    returned_range_count: int = Field(description="Number of contiguous gap ranges in this response.")
    truncated: bool = Field(description="Whether additional gap ranges remain after this response.")
    next_start_line: int | None = Field(description="Continuation line for the next call, or null when complete.")
    ranges: list[CoverageGapRangeResult] = Field(description="At most the requested number of relevant gap ranges.")


class CoverageSelectedLineResult(OutputModel):
    """Compact exact coverage state for one explicitly requested line."""

    line_number: int = Field(description="One-based source line number.")
    hits: int = Field(description="Recorded execution count.")
    covered: bool = Field(description="Whether the counted line was executed.")
    count_line: bool = Field(description="Whether this record contributes to line totals.")
    total_branches: int = Field(description="Branch outcomes attached to the line.")
    covered_branches: int = Field(description="Covered branch outcomes attached to the line.")
    total_functions: int = Field(description="Function entries attached to the line.")
    covered_functions: int = Field(description="Covered function entries attached to the line.")


class CoverageSelectedRangeResult(OutputModel):
    """One normalized inclusive window used for exact line selection."""

    start: int = Field(description="First one-based requested line.")
    end: int = Field(description="Last one-based requested line.")


class CoverageLineSelectionResult(OutputModel):
    """Normalization and completeness metadata for requested exact lines."""

    requested_ranges: list[CoverageSelectedRangeResult] = Field(
        description="Sorted disjoint windows after merging duplicates, overlaps, nesting, and adjacency."
    )
    requested_line_count: int = Field(description="Unique source-line numbers covered by normalized windows.")
    returned_line_count: int = Field(description="Requested line numbers with coverage records.")
    unrecorded_line_count: int = Field(description="Requested line numbers absent from the coverage report.")


class CoverageFileSummaryResult(OutputModel):
    """One file's compact metrics and grouped relevant coverage gaps."""

    file: CoverageFileCompactResult = Field(description="Selected file totals and rates without raw parser metrics.")
    gaps: CoverageGapSummaryResult = Field(description="Relevant gaps grouped into contiguous ranges.")
    selected_lines: list[CoverageSelectedLineResult] = Field(
        description="Exact compact records from requested line_ranges; empty when no ranges were requested."
    )
    line_selection: CoverageLineSelectionResult = Field(
        description="Normalized range and coverage-record completeness metadata."
    )


class CoverageFileDetailResult(CoverageFileSummaryResult):
    """Compact file coverage plus explicitly requested parser-specific metrics."""

    raw_metrics: dict[str, Any] = Field(description="Format-specific file counters requested with detailed=true.")


CoverageFileResponse = Annotated[
    CoverageFileDetailResult | CoverageFileSummaryResult,
    Field(description="Selected file metrics and bounded grouped gaps, plus raw metrics only when requested."),
]


class InsightSummaryResult(OutputModel):
    """Counts of deterministic investigation findings by severity."""

    item_count: int = Field(description="Total findings before the response bound is applied.")
    high_count: int = Field(description="High-severity finding count.")
    medium_count: int = Field(description="Medium-severity finding count.")
    info_count: int = Field(description="Informational finding count.")


class InsightItemResult(OutputModel):
    """One deterministic, actionable coverage investigation finding."""

    severity: Literal["high", "medium", "info"] = Field(description="Investigation priority.")
    category: Literal[
        "parser-warning",
        "zero-coverage-file",
        "low-line-coverage",
        "low-branch-coverage",
        "overall-regression",
        "file-regression",
        "line-regression",
    ] = Field(description="Machine-readable reason for the finding.")
    title: str = Field(description="Short finding title.")
    detail: str = Field(description="Bounded evidence explaining what to investigate.")
    file_path: str | None = Field(default=None, description="Relevant exact file path, when applicable.")
    line_number: int | None = Field(default=None, description="Relevant one-based line, when applicable.")


class CoverageInsightsResult(OutputModel):
    """Deterministic priorities derived from one snapshot and optional baseline."""

    snapshot: SnapshotResult = Field(description="Current snapshot being analyzed.")
    baseline: SnapshotResult | None = Field(description="Comparison baseline, or null when not requested.")
    summary: InsightSummaryResult = Field(description="Finding counts by severity.")
    items: list[InsightItemResult] = Field(description="Bounded high-to-low priority investigation findings.")


class OverallComparisonResult(OutputModel):
    """Current-minus-baseline project-wide metric changes."""

    line_rate_delta: float | None = Field(description="Line-rate change.")
    covered_lines_delta: int = Field(description="Covered-line count change.")
    total_lines_delta: int = Field(description="Instrumented-line count change.")
    branch_rate_delta: float | None = Field(description="Branch-rate change.")
    covered_branches_delta: int = Field(description="Covered-branch count change.")
    total_branches_delta: int = Field(description="Instrumented-branch count change.")
    function_rate_delta: float | None = Field(description="Function-rate change.")
    covered_functions_delta: int = Field(description="Covered-function count change.")
    total_functions_delta: int = Field(description="Instrumented-function count change.")
    region_rate_delta: float | None = Field(description="Region-rate change.")
    covered_regions_delta: int = Field(description="Covered-region count change.")
    total_regions_delta: int = Field(description="Instrumented-region count change.")


class FileComparisonResult(OutputModel):
    """Current and baseline counters for one changed path."""

    file_path: str = Field(description="Exact repository-relative source path.")
    baseline_total_lines: int | None = Field(description="Baseline instrumented lines; null for a new file.")
    current_total_lines: int | None = Field(description="Current instrumented lines; null for a removed file.")
    baseline_covered_lines: int | None = Field(description="Baseline covered lines.")
    current_covered_lines: int | None = Field(description="Current covered lines.")
    baseline_line_rate: float | None = Field(description="Baseline line rate.")
    current_line_rate: float | None = Field(description="Current line rate.")
    line_rate_delta: float = Field(description="Current-minus-baseline line rate, treating absent rates as zero.")
    baseline_total_branches: int | None = Field(description="Baseline branch outcomes.")
    current_total_branches: int | None = Field(description="Current branch outcomes.")
    baseline_covered_branches: int | None = Field(description="Baseline covered branch outcomes.")
    current_covered_branches: int | None = Field(description="Current covered branch outcomes.")
    baseline_branch_rate: float | None = Field(description="Baseline branch rate.")
    current_branch_rate: float | None = Field(description="Current branch rate.")
    branch_rate_delta: float = Field(description="Current-minus-baseline branch rate with absent rates as zero.")
    baseline_total_functions: int | None = Field(description="Baseline instrumented functions.")
    current_total_functions: int | None = Field(description="Current instrumented functions.")
    baseline_covered_functions: int | None = Field(description="Baseline covered functions.")
    current_covered_functions: int | None = Field(description="Current covered functions.")
    baseline_function_rate: float | None = Field(description="Baseline function rate.")
    current_function_rate: float | None = Field(description="Current function rate.")
    function_rate_delta: float = Field(description="Current-minus-baseline function rate with absent rates as zero.")
    baseline_total_regions: int | None = Field(description="Baseline instrumented regions.")
    current_total_regions: int | None = Field(description="Current instrumented regions.")
    baseline_covered_regions: int | None = Field(description="Baseline covered regions.")
    current_covered_regions: int | None = Field(description="Current covered regions.")
    baseline_region_rate: float | None = Field(description="Baseline region rate.")
    current_region_rate: float | None = Field(description="Current region rate.")
    region_rate_delta: float = Field(description="Current-minus-baseline region rate with absent rates as zero.")


class ChangedLineResult(OutputModel):
    """One exact source line whose coverage state or counters changed."""

    file_path: str = Field(description="Exact repository-relative source path.")
    line_number: int = Field(description="One-based source line number.")
    baseline_covered: bool | None = Field(description="Baseline covered state; null for a new line.")
    current_covered: bool | None = Field(description="Current covered state; null for a removed line.")
    baseline_hits: int | None = Field(description="Baseline execution count.")
    current_hits: int | None = Field(description="Current execution count.")
    baseline_total_branches: int | None = Field(description="Baseline branch outcomes on the line.")
    current_total_branches: int | None = Field(description="Current branch outcomes on the line.")
    baseline_covered_branches: int | None = Field(description="Baseline covered branch outcomes on the line.")
    current_covered_branches: int | None = Field(description="Current covered branch outcomes on the line.")
    status: LineChangeStatus = Field(description="new, removed, regressed, improved, or counter-only changed.")


class ComparisonResult(OutputModel):
    """Bounded exact comparison between current and baseline snapshots."""

    baseline: SnapshotResult = Field(description="Explicit or frozen baseline snapshot.")
    current: SnapshotResult = Field(description="Current snapshot.")
    overall: OverallComparisonResult = Field(description="Project-wide metric deltas.")
    files: list[FileComparisonResult] = Field(description="Bounded changed paths ordered by worst line-rate delta.")
    changed_lines: list[ChangedLineResult] = Field(description="Bounded exact line and branch changes.")
    worktree: WorktreeResult | None = Field(
        default=None,
        description="Worktree metadata in frozen-baseline mode; null for direct snapshot comparison.",
    )


class LineHistoryResult(OutputModel):
    """One chronological coverage observation for an exact path and line."""

    snapshot_id: str = Field(description="Immutable snapshot UUID.")
    created_at: str = Field(description="UTC snapshot ingestion time.")
    branch: str | None = Field(description="Snapshot branch.")
    commit_sha: str | None = Field(description="Snapshot commit.")
    suite: str = Field(description="Snapshot suite.")
    file_path: str = Field(description="Exact repository-relative source path.")
    line_number: int = Field(description="One-based source line number.")
    hits: int = Field(description="Recorded execution count at this point.")
    covered: bool = Field(description="Covered state at this point.")
    total_branches: int = Field(description="Branch outcomes attached to the line.")
    covered_branches: int = Field(description="Covered branch outcomes attached to the line.")


class SourceLineResult(OutputModel):
    """One bounded source line read from the snapshot's repository."""

    line_number: int = Field(description="One-based source line number.")
    text: str = Field(description="Source text without the trailing newline.")


ProjectSummaryResults = Annotated[
    list[ProjectSummaryResult],
    Field(description="Known projects ordered by latest coverage activity."),
]
RegisteredCommandResults = Annotated[
    list[RegisteredCommandResult],
    Field(description="Approved immutable command registrations ordered newest first."),
]
RunQueueResults = Annotated[
    list[CompactRunResult],
    Field(description="Running jobs followed by queued jobs in FIFO order."),
]
CoverageFileResults = Annotated[
    list[CoverageFileResult],
    Field(description="Bounded files ordered by lowest line coverage then largest size."),
]
ChangedLineResults = Annotated[
    list[ChangedLineResult],
    Field(description="Bounded exact coverage changes ordered with regressions first."),
]
LineHistoryResults = Annotated[
    list[LineHistoryResult],
    Field(description="Chronological coverage history for one exact path and line."),
]
SourceLineResults = Annotated[
    list[SourceLineResult],
    Field(description="At most 200 repository-contained source lines."),
]
