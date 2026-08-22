//! Transport-neutral orchestration and response projection.
//!
//! Keeping this layer independent of Hyper and the MCP wire format makes the
//! REST, dashboard, and agent-facing interfaces share the same validation,
//! pagination, response budgets, and compact projections.

// Service projections consume validated storage rows and use assertions for
// schema invariants. Boundary and storage failures remain typed AppErrors.
#![allow(clippy::expect_used, clippy::unwrap_in_result)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};
use crate::git::{ChangedLineRange, changed_line_ranges, inspect_git};
use crate::storage::{
    COLLECTION_FETCH_LIMIT, CoverageStore, LineRange, MAX_COLLECTION_RECORDS, ProjectSettingsPatch,
};
use crate::{SCHEMA_REVISION, hex_prefix};

/// Default response word budget used by compact agent-facing calls.
pub const DEFAULT_MAX_WORDS: usize = 600;

/// Repository identity attached to every response envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestContext {
    /// Stable Git repository key.
    pub repo_key: String,
    /// Selected checkout path.
    pub checkout_path: String,
    /// Optional suite selector.
    pub suite: Option<String>,
}

/// Shared orchestration service used by every public transport.
#[derive(Clone)]
pub struct CoverageService {
    store: CoverageStore,
    context: Arc<RequestContext>,
}

impl CoverageService {
    /// Creates a service for a store whose project has already been selected.
    pub fn new(store: CoverageStore, context: RequestContext) -> Self {
        Self {
            store,
            context: Arc::new(context),
        }
    }

    /// Returns the backing store.
    pub fn store(&self) -> &CoverageStore {
        &self.store
    }

    /// Returns the selected request context, optionally overriding its suite.
    pub fn context(&self, suite: Option<&str>) -> RequestContext {
        let mut context = (*self.context).clone();
        if suite.is_some() {
            context.suite = suite.map(str::to_owned);
        }
        context
    }

    /// Wraps data in the versioned public response envelope.
    pub fn envelope(&self, data: Value, suite: Option<&str>, page: Option<Value>) -> Value {
        let context = self.context(suite);
        json!({
            "context": {
                "repo_key": context.repo_key,
                "checkout_path": context.checkout_path,
                "suite": context.suite,
                "schema_revision": SCHEMA_REVISION,
            },
            "data": data,
            "page": page,
        })
    }

    /// Applies a singular response budget.
    pub fn apply_budget(&self, response: Value, max_words: usize) -> AppResult<Value> {
        validate_max_words(max_words)?;
        let data = response.get("data").cloned().unwrap_or(Value::Null);
        let count = serialized_word_count(&data);
        if count > max_words {
            return Err(AppError::Validation(format!(
                "response requires {count} words; increase max_words or request detailed=false"
            )));
        }
        Ok(response)
    }

    /// Applies an exact serialized-byte budget to the complete response.
    pub fn apply_byte_budget(&self, response: Value, max_bytes: usize) -> AppResult<Value> {
        let bytes = serde_json::to_vec(&response)
            .expect("serde_json::Value serialization must be infallible")
            .len();
        if bytes > max_bytes {
            return Err(AppError::Validation(format!(
                "response requires {bytes} bytes; reduce limits or omit source"
            )));
        }
        Ok(response)
    }

    /// Validates a path selector against the selected repository.
    pub fn validate_repository_path(&self, repo_path: Option<&str>) -> AppResult<()> {
        if let Some(repo_path) = repo_path {
            if inspect_git(Path::new(repo_path))?.repo_key != self.context.repo_key {
                return Err(AppError::Validation(
                    "repo_path does not belong to the selected repository".to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Pages an already bounded collection using opaque, query-scoped cursors.
    pub fn page(
        &self,
        values: &[Value],
        cursor: Option<&str>,
        max_words: usize,
        scope: &str,
        total: Option<usize>,
    ) -> AppResult<(Vec<Value>, Value)> {
        validate_max_words(max_words)?;
        let known_total = total.unwrap_or(values.len());
        if values.len() > MAX_COLLECTION_RECORDS || known_total > values.len() {
            return Err(AppError::Validation(format!(
                "result exceeds the defensive {MAX_COLLECTION_RECORDS}-record cap; refine the query"
            )));
        }
        let start = if let Some(cursor) = cursor {
            let (anchor, occurrence) = decode_cursor(cursor, scope)?;
            let mut seen = 0usize;
            let mut position = None;
            for (index, value) in values.iter().enumerate() {
                if cursor_anchor(value) == anchor {
                    seen += 1;
                    if seen == occurrence {
                        position = Some(index + 1);
                        break;
                    }
                }
            }
            position.ok_or_else(|| {
                AppError::Validation(
                    "pagination cursor no longer matches the available results".to_owned(),
                )
            })?
        } else {
            0
        };
        let mut selected = Vec::new();
        let mut word_count = 0usize;
        for value in values.iter().skip(start) {
            let item_words = serialized_word_count(value);
            if !selected.is_empty() && word_count + item_words > max_words {
                break;
            }
            selected.push(value.clone());
            word_count += item_words;
            if word_count >= max_words {
                break;
            }
        }
        let consumed = start + selected.len();
        let truncated = consumed < known_total;
        let next_cursor = if truncated && !selected.is_empty() {
            let anchor = cursor_anchor(
                selected
                    .last()
                    .expect("a truncated page must contain a selected value"),
            );
            let occurrence = values[..consumed]
                .iter()
                .filter(|value| cursor_anchor(value) == anchor)
                .count();
            Some(
                encode_cursor(&anchor, scope, occurrence)
                    .expect("a truncated page always has a positive cursor occurrence"),
            )
        } else {
            None
        };
        let returned = selected.len();
        Ok((
            selected,
            json!({
                "returned": returned,
                "total": known_total,
                "word_count": word_count,
                "max_words": max_words,
                "truncated": truncated,
                "next_cursor": next_cursor,
            }),
        ))
    }

    /// Returns the project summary, command inventory, and active runs.
    pub fn project_context(
        &self,
        cursor: Option<&str>,
        max_words: usize,
        detailed: bool,
    ) -> AppResult<Value> {
        let context = self.context(None);
        let project = self.store.project_summary()?;
        let commands = self
            .store
            .list_registered_commands(COLLECTION_FETCH_LIMIT)?
            .into_iter()
            .map(|value| compact_command(&value, detailed))
            .collect::<Vec<_>>();
        let scope = format!("project-context:{}:{detailed}", context.repo_key);
        let (commands, page) = self.page(&commands, cursor, max_words, &scope, None)?;
        let latest = self
            .store
            .latest_run(None)?
            .map(|value| compact_run_result(&value, detailed));
        let active = self
            .store
            .list_run_queue(COLLECTION_FETCH_LIMIT)?
            .into_iter()
            .map(|value| compact_run_result(&value, false))
            .collect::<Vec<_>>();
        let project = if detailed {
            project
        } else {
            compact_project(&project)
        };
        Ok(self.envelope(json!({"project": project, "commands": commands, "latest_run": latest, "active_runs": active}), None, Some(page)))
    }

    /// Registers one human-approved command.
    #[allow(clippy::too_many_arguments)]
    pub fn command_registration(
        &self,
        name: &str,
        command: &str,
        human_approved: bool,
        approved_by: &str,
        approval_note: &str,
        cwd: Option<&str>,
        shell: &str,
        artifact_paths: Option<Value>,
        detailed: bool,
    ) -> AppResult<Value> {
        let context = self.context(None);
        let resolved = cwd
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(&context.checkout_path));
        if inspect_git(&resolved)?.repo_key != context.repo_key {
            return Err(AppError::Validation(
                "command cwd does not belong to the selected repository".to_owned(),
            ));
        }
        let value = self.store.register_command(
            name,
            command,
            Some(&resolved),
            shell,
            artifact_paths,
            human_approved,
            approved_by,
            approval_note,
            true,
        )?;
        Ok(self.envelope(compact_command(&value, detailed), None, None))
    }

    /// Submits or waits for one registered command.
    pub fn run_submission(
        &self,
        command_ref: &str,
        timeout_seconds: Option<u64>,
        idempotency_key: Option<&str>,
        wait: bool,
        detailed: bool,
    ) -> AppResult<Value> {
        self.run_submission_with_options(
            command_ref,
            timeout_seconds,
            idempotency_key,
            wait,
            false,
            detailed,
        )
    }

    /// Submits or waits for one registered command with reuse policy.
    pub fn run_submission_with_options(
        &self,
        command_ref: &str,
        timeout_seconds: Option<u64>,
        idempotency_key: Option<&str>,
        wait: bool,
        reuse_if_unchanged: bool,
        detailed: bool,
    ) -> AppResult<Value> {
        let value = if wait {
            self.store.run_command_with_options(
                command_ref,
                timeout_seconds,
                idempotency_key,
                20,
                reuse_if_unchanged,
            )?
        } else {
            self.store.submit_command_with_options(
                command_ref,
                timeout_seconds,
                idempotency_key,
                20,
                reuse_if_unchanged,
            )?
        };
        let mut result = compact_run_result(&value, detailed);
        self.attach_terminal_review(&mut result);
        Ok(self.envelope(result, None, None))
    }

    /// Returns or cancels a run.
    pub fn run_state(&self, run_id: &str, action: &str, detailed: bool) -> AppResult<Value> {
        let value = match action {
            "status" => self.store.run_result(run_id, 20)?,
            "cancel" => self.store.cancel_run(run_id, 20)?,
            _ => {
                return Err(AppError::Validation(
                    "action must be status or cancel".to_owned(),
                ));
            }
        };
        let mut result = compact_run_result(&value, detailed);
        if action == "status" {
            self.attach_terminal_review(&mut result);
        }
        Ok(self.envelope(result, None, None))
    }

    /// Reads one durable run projection or targeted log projection.
    #[allow(clippy::too_many_arguments)]
    pub fn run_review(
        &self,
        run_id: &str,
        view: &str,
        query: Option<Vec<String>>,
        stream: &str,
        context_lines: usize,
        max_matches: usize,
        case_sensitive: bool,
        max_words: usize,
        max_bytes: usize,
    ) -> AppResult<Value> {
        validate_max_words(max_words)?;
        validate_review_byte_budget(max_bytes)?;
        if context_lines > 20 {
            return Err(AppError::Validation(
                "context_lines must be between 0 and 20".to_owned(),
            ));
        }
        if !(1..=50).contains(&max_matches) {
            return Err(AppError::Validation(
                "max_matches must be between 1 and 50".to_owned(),
            ));
        }
        if !matches!(stream, "stdout" | "stderr" | "both") {
            return Err(AppError::Validation(
                "stream must be stdout, stderr, or both".to_owned(),
            ));
        }
        let response = match view {
            "status" => self.run_state(run_id, "status", false)?,
            "logs" => self.search_logs(
                run_id,
                query.ok_or_else(|| {
                    AppError::Validation("run_review logs view requires query".to_owned())
                })?,
                stream,
                context_lines,
                max_matches,
                max_words,
                case_sensitive,
            )?,
            _ => {
                return Err(AppError::Validation(
                    "run_review view must be status or logs".to_owned(),
                ));
            }
        };
        let response = self.apply_budget(response, max_words)?;
        self.apply_byte_budget(response, max_bytes)
    }

    fn attach_terminal_review(&self, result: &mut Value) {
        let terminal = result
            .get("terminal")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let Some(snapshot_id) = result
            .get("coverage_ingest")
            .and_then(|value| value.get("snapshot_ids"))
            .and_then(Value::as_array)
            .and_then(|values| values.iter().find_map(Value::as_str))
        else {
            return;
        };
        if !terminal {
            return;
        }
        let review = match self.coverage_review(
            "change",
            Some(snapshot_id),
            None,
            None,
            None,
            None,
            None,
            2,
            10,
            5,
            8,
            false,
            3,
            40,
            800,
            8_000,
            "review",
        ) {
            Ok(value) => value
                .get("data")
                .cloned()
                .expect("coverage review responses always contain data"),
            Err(error) => {
                json!({"claim_status":"limited","status":"unavailable","reason":error.to_string()})
            }
        };
        result
            .as_object_mut()
            .expect("managed run projections are JSON objects")
            .insert("coverage_review".to_owned(), review);
    }

    /// Searches retained run output with literal OR matching.
    #[allow(clippy::too_many_arguments)]
    pub fn search_logs(
        &self,
        run_id: &str,
        query: Vec<String>,
        stream: &str,
        context_lines: usize,
        max_matches: usize,
        max_words: usize,
        case_sensitive: bool,
    ) -> AppResult<Value> {
        let mut value = self.store.search_run_logs(
            run_id,
            &query,
            stream,
            context_lines,
            max_matches,
            case_sensitive,
            max_words,
        )?;
        strip_log_metadata(&mut value);
        Ok(self.envelope(value, None, None))
    }

    /// Parses and ingests a coverage artifact.
    #[allow(clippy::too_many_arguments)]
    pub fn ingest(
        &self,
        report_path: &str,
        format: &str,
        suite: &str,
        branch: Option<&str>,
        commit_sha: Option<&str>,
        base_ref: Option<&str>,
        detailed: bool,
    ) -> AppResult<Value> {
        let suite = suite.trim();
        if suite.is_empty() {
            return Err(AppError::Validation("suite must not be blank".to_owned()));
        }
        let context = self.context(Some(suite));
        let path = PathBuf::from(report_path);
        let path = if path.is_absolute() {
            path
        } else {
            PathBuf::from(&context.checkout_path).join(path)
        };
        let snapshot = self.store.ingest_report(
            &path,
            format,
            Some(Path::new(&context.checkout_path)),
            branch,
            commit_sha,
            base_ref,
            suite,
        )?;
        Ok(self.envelope(compact_snapshot(&snapshot, detailed), Some(suite), None))
    }

    /// Imports one external or historical report through the public tool boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn coverage_import(
        &self,
        report_path: &str,
        format: &str,
        suite: &str,
        branch: Option<&str>,
        commit_sha: Option<&str>,
        base_ref: Option<&str>,
        max_words: usize,
        max_bytes: usize,
    ) -> AppResult<Value> {
        validate_max_words(max_words)?;
        validate_review_byte_budget(max_bytes)?;
        validate_relative_report_path(report_path, &self.context(None).checkout_path)?;
        let response = self.ingest(
            report_path,
            format,
            suite,
            branch,
            commit_sha,
            base_ref,
            false,
        )?;
        let response = self.apply_budget(response, max_words)?;
        self.apply_byte_budget(response, max_bytes)
    }

    /// Registers a Git worktree against the selected repository.
    pub fn ensure_lineage_baseline(
        &self,
        path: &str,
        base_ref: &str,
        name: Option<&str>,
    ) -> AppResult<Value> {
        let context = self.context(None);
        let git = inspect_git(Path::new(path))?;
        if git.commit_sha.is_none() || git.repo_key != context.repo_key {
            return Err(AppError::Validation(
                "worktree must be a Git checkout of the selected repository".to_owned(),
            ));
        }
        let result =
            self.store
                .ensure_lineage_baseline(Path::new(&git.repo_path), base_ref.trim(), name)?;
        let compact = json!({"id": result["id"], "name": result["name"], "created_at": result["created_at"], "path": result["path"], "branch": result["branch"], "head_sha": result["head_sha"], "base_ref": result["base_ref"], "base_sha": result["base_sha"], "baseline_snapshot_id": result["baseline_snapshot_id"]});
        Ok(self.envelope(compact, None, None))
    }

    /// Executes a snapshot, worktree, progress, file, or line comparison.
    #[allow(clippy::too_many_arguments)]
    pub fn coverage_comparison(
        &self,
        view: &str,
        snapshot_id: Option<&str>,
        baseline_snapshot_id: Option<&str>,
        worktree_id: Option<&str>,
        suite: Option<&str>,
        file_path: Option<&str>,
        only_regressions: bool,
        cursor: Option<&str>,
        max_words: usize,
        detailed: bool,
    ) -> AppResult<Value> {
        validate_max_words(max_words)?;
        if view == "progress" {
            let worktree_id = worktree_id.ok_or_else(|| {
                AppError::Validation(
                    "worktree_id and suite are required for progress view".to_owned(),
                )
            })?;
            let suite = suite.ok_or_else(|| {
                AppError::Validation(
                    "worktree_id and suite are required for progress view".to_owned(),
                )
            })?;
            let mut progress = self.store.worktree_progress(
                worktree_id,
                suite,
                file_path,
                COLLECTION_FETCH_LIMIT,
            )?;
            let points = progress["points"]
                .as_array()
                .expect("worktree progress always contains points")
                .to_vec();
            let (points, page) = self.page(
                &points,
                cursor,
                max_words,
                &format!("worktree-progress:{worktree_id}:{suite}:{file_path:?}"),
                None,
            )?;
            update_worktree_progress(&mut progress, points, detailed)
                .expect("stored worktree progress has the required shape");
            return Ok(self.envelope(progress, Some(suite), Some(page)));
        }
        let comparison = if view == "regions" {
            if let Some(worktree_id) = worktree_id {
                self.store.compare_worktree_regions(
                    worktree_id,
                    snapshot_id,
                    file_path,
                    only_regressions,
                    COLLECTION_FETCH_LIMIT,
                )?
            } else {
                let context = self.context(suite);
                let current_id = if let Some(snapshot_id) = snapshot_id {
                    snapshot_id.to_owned()
                } else {
                    self.store
                        .latest_snapshot(Some(&context.checkout_path), None, suite)?
                        .map(|value| {
                            value["id"]
                                .as_str()
                                .expect("stored snapshots always contain an id")
                                .to_owned()
                        })
                        .ok_or_else(|| AppError::NotFound("no snapshots found".to_owned()))?
                };
                let baseline_id = if let Some(baseline_snapshot_id) = baseline_snapshot_id {
                    baseline_snapshot_id.to_owned()
                } else {
                    self.store
                        .previous_snapshot(&current_id)?
                        .map(|value| {
                            value["id"]
                                .as_str()
                                .expect("stored snapshots always contain an id")
                                .to_owned()
                        })
                        .ok_or_else(|| {
                            AppError::NotFound(
                                "no previous snapshot found for the selected coverage".to_owned(),
                            )
                        })?
                };
                self.store.compare_regions(
                    &current_id,
                    &baseline_id,
                    file_path,
                    only_regressions,
                    COLLECTION_FETCH_LIMIT,
                )?
            }
        } else if let Some(worktree_id) = worktree_id {
            self.store
                .compare_worktree_default_limits(worktree_id, snapshot_id)?
        } else {
            let snapshot_id = snapshot_id.ok_or_else(|| {
                AppError::Validation(
                    "snapshot_id and baseline_snapshot_id are required without worktree_id"
                        .to_owned(),
                )
            })?;
            let baseline_snapshot_id = baseline_snapshot_id.ok_or_else(|| {
                AppError::Validation(
                    "snapshot_id and baseline_snapshot_id are required without worktree_id"
                        .to_owned(),
                )
            })?;
            self.store.compare(
                snapshot_id,
                baseline_snapshot_id,
                COLLECTION_FETCH_LIMIT,
                COLLECTION_FETCH_LIMIT,
            )?
        };
        let current_suite = comparison["current"]["suite"]
            .as_str()
            .expect("comparisons always contain the current suite")
            .to_owned();
        if suite.is_some_and(|value| value != current_suite) {
            return Err(AppError::Validation(
                "requested suite does not match the current snapshot".to_owned(),
            ));
        }
        let mut base = json!({"baseline": compact_snapshot(&comparison["baseline"], detailed), "current": compact_snapshot(&comparison["current"], detailed), "overall": comparison["overall"]});
        if view == "overview" {
            base["file_change_count"] = json!(
                comparison["files"]
                    .as_array()
                    .expect("comparisons always contain files")
                    .len()
            );
            base["line_change_count"] = json!(
                comparison["changed_lines"]
                    .as_array()
                    .expect("comparisons always contain changed lines")
                    .len()
            );
            return Ok(self.envelope(base, Some(&current_suite), None));
        }
        let mut values = match view {
            "files" => comparison["files"]
                .as_array()
                .expect("comparisons always contain files")
                .to_vec(),
            "lines" => comparison["changed_lines"]
                .as_array()
                .expect("comparisons always contain changed lines")
                .to_vec()
                .into_iter()
                .filter(|value| {
                    !only_regressions
                        || value.get("status").and_then(Value::as_str) == Some("regressed")
                })
                .collect(),
            "regions" => {
                let regions = comparison["regions"]
                    .as_array()
                    .expect("comparisons always contain regions");
                base["region_change_count"] = json!(regions.len());
                regions.to_vec()
            }
            _ => {
                return Err(AppError::Validation(
                    "view must be overview, files, lines, regions, or progress".to_owned(),
                ));
            }
        };
        let (selected, page) = self.page(
            &values,
            cursor,
            max_words,
            &format!(
                "coverage-compare:{}:{}:{view}:{only_regressions}",
                comparison["current"]["id"], comparison["baseline"]["id"]
            ),
            None,
        )?;
        base[view] = Value::Array(selected);
        values.clear();
        Ok(self.envelope(base, Some(&current_suite), Some(page)))
    }

    /// Returns one bounded review for change, history, insight, or all three.
    #[allow(clippy::too_many_arguments)]
    pub fn coverage_review(
        &self,
        focus: &str,
        snapshot_id: Option<&str>,
        baseline_snapshot_id: Option<&str>,
        worktree_id: Option<&str>,
        suite: Option<&str>,
        branch: Option<&str>,
        file_path: Option<&str>,
        detail_snapshots: usize,
        summary_window: usize,
        max_files: usize,
        max_regions: usize,
        include_source: bool,
        context_lines: usize,
        max_source_lines: usize,
        max_words: usize,
        max_bytes: usize,
        representation: &str,
    ) -> AppResult<Value> {
        if !matches!(focus, "change" | "history" | "insight" | "all") {
            return Err(AppError::Validation(
                "focus must be change, history, insight, or all".to_owned(),
            ));
        }
        if !matches!(representation, "review" | "compact" | "audit") {
            return Err(AppError::Validation(
                "representation must be review, compact, or audit".to_owned(),
            ));
        }
        if !(1..=5).contains(&detail_snapshots) {
            return Err(AppError::Validation(
                "detail_snapshots must be between 1 and 5".to_owned(),
            ));
        }
        if !(2..=50).contains(&summary_window) {
            return Err(AppError::Validation(
                "summary_window must be between 2 and 50".to_owned(),
            ));
        }
        if !(1..=50).contains(&max_files) {
            return Err(AppError::Validation(
                "max_files must be between 1 and 50".to_owned(),
            ));
        }
        if !(1..=100).contains(&max_regions) {
            return Err(AppError::Validation(
                "max_regions must be between 1 and 100".to_owned(),
            ));
        }
        if context_lines > 20 {
            return Err(AppError::Validation(
                "source.context_lines must be between 0 and 20".to_owned(),
            ));
        }
        if !(10..=500).contains(&max_source_lines) {
            return Err(AppError::Validation(
                "max_source_lines must be between 10 and 500".to_owned(),
            ));
        }
        if !(1_000..=2_000_000).contains(&max_bytes) {
            return Err(AppError::Validation(
                "max_bytes must be between 1000 and 2000000".to_owned(),
            ));
        }
        validate_max_words(max_words)?;
        let context = self.context(suite);
        let current = if let Some(snapshot_id) = snapshot_id {
            Some(self.store.snapshot(snapshot_id)?)
        } else {
            self.store
                .latest_snapshot(Some(&context.checkout_path), branch, suite)?
        };
        let current_id = current.as_ref().map(|value| {
            value["id"]
                .as_str()
                .expect("stored snapshots always contain an id")
                .to_owned()
        });
        let selected_suite = suite.map(str::to_owned).or_else(|| {
            current
                .as_ref()
                .and_then(|value| value["suite"].as_str().map(str::to_owned))
        });
        let mut result = json!({
            "focus": focus,
            "task": focus,
            "representation": representation,
            "claim_status": if current.is_some() { "limited" } else { "not_measured" },
            "reasons": if current.is_some() {
                json!([])
            } else {
                json!(["no compatible coverage snapshot is available"])
            },
            "measurement": current.as_ref().map(|value| compact_snapshot(value, false)).unwrap_or(Value::Null),
            "baseline": Value::Null,
        });

        if matches!(focus, "change" | "all") {
            let mut change = self.review_change(
                current_id.as_deref(),
                baseline_snapshot_id,
                worktree_id,
                file_path,
                max_files,
                max_regions,
                include_source,
                context_lines,
                max_source_lines,
                representation == "audit",
            )?;
            let changed_code_status = change["changed_code"]["status"].as_str();
            if !change["baseline"].is_null() {
                result["baseline"] = change["baseline"].clone();
                if matches!(changed_code_status, Some("measured" | "no_source_changes")) {
                    result["claim_status"] = json!("supported");
                }
            } else if change["status"] == "no_baseline" {
                result["reasons"] = json!(["no compatible comparison baseline is available"]);
            }
            if representation == "compact" {
                compact_review_change(&mut change);
            } else if representation == "audit" {
                expand_review_change(&mut change);
            }
            result["change"] = change;
        }

        if matches!(focus, "history" | "all") {
            result["history"] = self.review_history(
                branch,
                selected_suite.as_deref(),
                file_path,
                worktree_id,
                detail_snapshots,
                summary_window,
            )?;
            if focus == "history" && result["history"]["status"] == "measured" {
                result["claim_status"] = json!("supported");
            }
        }

        if matches!(focus, "insight" | "all") {
            result["insight"] =
                self.review_insight(current_id.as_deref(), baseline_snapshot_id, max_regions)?;
            if focus == "insight" && result["insight"]["status"] == "measured" {
                result["claim_status"] = json!("supported");
            }
        }

        let response = self.apply_budget(
            self.envelope(result, selected_suite.as_deref(), None),
            max_words,
        )?;
        self.apply_byte_budget(response, max_bytes)
    }

    /// Returns the compact immutable summary used by the snapshot resource.
    pub fn snapshot_summary(
        &self,
        snapshot_id: &str,
        max_words: usize,
        detailed: bool,
    ) -> AppResult<Value> {
        validate_max_words(max_words)?;
        let snapshot = self.store.snapshot(snapshot_id)?;
        let suite = snapshot["suite"]
            .as_str()
            .expect("stored snapshots always contain a suite")
            .to_owned();
        self.apply_budget(
            self.envelope(compact_snapshot(&snapshot, detailed), Some(&suite), None),
            max_words,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn review_change(
        &self,
        current_id: Option<&str>,
        baseline_snapshot_id: Option<&str>,
        worktree_id: Option<&str>,
        file_path: Option<&str>,
        max_files: usize,
        max_regions: usize,
        include_source: bool,
        context_lines: usize,
        max_source_lines: usize,
        audit_regions: bool,
    ) -> AppResult<Value> {
        let Some(current_id) = current_id else {
            return Ok(json!({
                "status": "not_measured",
                "baseline": Value::Null,
                "current": Value::Null,
                "overall": Value::Null,
                "files": [],
                "regions": [],
                "changed_code": {"status":"not_measured","files":[]},
                "next_action": {"kind":"obtain_measurement","reason":"no compatible coverage snapshot is available"},
                "source": []
            }));
        };
        let baseline_id = if baseline_snapshot_id == Some("none") {
            None
        } else if let Some(baseline_snapshot_id) = baseline_snapshot_id {
            Some(baseline_snapshot_id.to_owned())
        } else if let Some(worktree_id) = worktree_id {
            let current_snapshot = self.store.snapshot(current_id)?;
            let suite = current_snapshot["suite"]
                .as_str()
                .expect("stored snapshots always contain a suite");
            self.store
                .worktree_baseline_snapshot(worktree_id, suite)?
                .and_then(|value| value["id"].as_str().map(str::to_owned))
        } else {
            self.store.previous_snapshot(current_id)?.map(|value| {
                value["id"]
                    .as_str()
                    .expect("stored snapshots always contain an id")
                    .to_owned()
            })
        };
        let Some(baseline_id) = baseline_id else {
            let current = self.store.snapshot(current_id)?;
            return Ok(json!({
                "status": "no_baseline",
                "baseline": Value::Null,
                "current": compact_snapshot(&current, false),
                "overall": Value::Null,
                "files": [],
                "regions": [],
                "changed_code": {"status":"no_baseline","files":[]},
                "next_action": {"kind":"establish_baseline","reason":"no compatible comparison baseline is available"},
                "source": []
            }));
        };
        let comparison = self.store.compare(
            current_id,
            &baseline_id,
            max_files.max(1),
            max_regions.saturating_mul(20).max(max_regions),
        )?;
        let current = &comparison["current"];
        let baseline = &comparison["baseline"];
        let changed_code = self.review_changed_code(current_id, baseline, current, max_regions)?;
        let raw_regions =
            self.store
                .changed_regions(current_id, &baseline_id, file_path, false, max_regions)?;
        let regions = if audit_regions {
            Value::Array(raw_regions.clone())
        } else {
            compact_changed_regions(&raw_regions)
        };
        let next_action = review_next_action(&changed_code, &raw_regions);
        let mut source = Vec::new();
        let mut source_line_count = 0usize;
        if include_source {
            for region in raw_regions.iter().take(max_regions) {
                if source_line_count >= max_source_lines {
                    break;
                }
                let path = required_string_field(region, "file_path", "changed region")
                    .expect("stored changed-region projections always contain file_path");
                let start = required_i64_field(region, "start", "changed region")
                    .expect("stored changed-region projections always contain start");
                let end = required_i64_field(region, "end", "changed region")
                    .expect("stored changed-region projections always contain end");
                let start = start.saturating_sub(context_lines as i64).max(1);
                let end = end
                    .saturating_add(context_lines as i64)
                    .min(start.saturating_add((max_source_lines - source_line_count) as i64 - 1));
                let lines = self.store.source_lines(current_id, &path, start, end)?;
                let coverage = self
                    .store
                    .lines_in_ranges(current_id, &path, &[(start, end)])?;
                let coverage_lines = coverage["lines"]
                    .as_array()
                    .expect("line coverage projections always contain lines");
                let (lines, red_regions) = annotate_source_lines(lines, Some(coverage_lines));
                source.push(json!({
                    "file_path": path,
                    "start": start,
                    "end": end,
                    "source_resolution": self.store.source_resolution(current_id, &path)?,
                    "red_regions": red_regions,
                    "lines": lines
                }));
                source_line_count = source_line_count.saturating_add(
                    source
                        .last()
                        .and_then(|value| value["lines"].as_array())
                        .map_or(0, Vec::len),
                );
            }
        }
        let files = comparison["files"]
            .as_array()
            .expect("comparisons always contain files")
            .iter()
            .take(max_files)
            .filter(|value| {
                file_path
                    .is_none_or(|path| value.get("file_path").and_then(Value::as_str) == Some(path))
            })
            .map(compact_file_change)
            .collect::<Vec<_>>();
        Ok(json!({
            "status": "measured",
            "baseline": compact_snapshot(baseline, false),
            "current": compact_snapshot(current, false),
            "overall": comparison["overall"],
            "files": files,
            "regions": regions,
            "changed_code": changed_code,
            "next_action": next_action,
            "source": source
        }))
    }

    fn review_changed_code(
        &self,
        snapshot_id: &str,
        baseline: &Value,
        current: &Value,
        max_regions: usize,
    ) -> AppResult<Value> {
        let repo_path = current["repo_path"]
            .as_str()
            .expect("stored snapshots always contain a repository path")
            .to_owned();
        let baseline_commit = baseline.get("commit_sha").and_then(Value::as_str);
        let current_commit = current.get("commit_sha").and_then(Value::as_str);
        let (Some(baseline_commit), Some(current_commit)) = (baseline_commit, current_commit)
        else {
            return Ok(json!({
                "status": "unavailable",
                "reason": "both snapshots need commit_sha for changed-code coverage",
                "files": []
            }));
        };
        let ranges = match changed_line_ranges(&repo_path, baseline_commit, current_commit) {
            Ok(ranges) => ranges,
            Err(error) => {
                return Ok(json!({
                    "status": "unavailable",
                    "reason": error.to_string(),
                    "baseline_commit": baseline_commit,
                    "current_commit": current_commit,
                    "files": []
                }));
            }
        };
        let mut by_file: BTreeMap<String, BTreeMap<String, Vec<i64>>> = BTreeMap::new();
        let mut range_count = 0usize;
        for range in ranges.iter().take(max_regions) {
            range_count += 1;
            self.classify_changed_range(snapshot_id, range, &mut by_file)?;
        }
        let files = by_file
            .into_iter()
            .map(|(path, statuses)| {
                let mut file = Map::new();
                file.insert("path".to_owned(), json!(path));
                for (status, numbers) in statuses {
                    file.insert(status, Value::Array(line_regions(&numbers)));
                }
                Value::Object(file)
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "status": if ranges.is_empty() { "no_source_changes" } else { "measured" },
            "baseline_commit": baseline_commit,
            "current_commit": current_commit,
            "range_count": range_count,
            "files": files
        }))
    }

    fn classify_changed_range(
        &self,
        snapshot_id: &str,
        range: &ChangedLineRange,
        by_file: &mut BTreeMap<String, BTreeMap<String, Vec<i64>>>,
    ) -> AppResult<()> {
        let end = range
            .start
            .saturating_add(range.line_count)
            .saturating_sub(1);
        let selected =
            self.store
                .lines_in_ranges(snapshot_id, &range.file_path, &[(range.start, end)])?;
        let measurements = selected["lines"]
            .as_array()
            .expect("line range projections always contain lines")
            .iter()
            .filter_map(|line| {
                line.get("line_number")
                    .and_then(Value::as_i64)
                    .map(|number| (number, line))
            })
            .collect::<BTreeMap<_, _>>();
        let statuses = by_file.entry(range.file_path.clone()).or_default();
        for number in range.start..=end {
            let Some(line) = measurements.get(&number) else {
                statuses
                    .entry("unmeasured".to_owned())
                    .or_default()
                    .push(number);
                continue;
            };
            if !line
                .get("count_line")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                statuses
                    .entry("non_executable".to_owned())
                    .or_default()
                    .push(number);
                continue;
            }
            let covered = line
                .get("covered")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            statuses
                .entry(if covered { "covered" } else { "uncovered" }.to_owned())
                .or_default()
                .push(number);
            let branch_gap = line
                .get("total_branches")
                .and_then(Value::as_i64)
                .zip(line.get("covered_branches").and_then(Value::as_i64))
                .map_or(0, |(total, covered)| total.saturating_sub(covered));
            if branch_gap > 0 {
                statuses
                    .entry("branch_gap".to_owned())
                    .or_default()
                    .push(number);
            }
        }
        Ok(())
    }

    fn review_history(
        &self,
        branch: Option<&str>,
        suite: Option<&str>,
        file_path: Option<&str>,
        worktree_id: Option<&str>,
        detail_snapshots: usize,
        summary_window: usize,
    ) -> AppResult<Value> {
        let context = self.context(suite);
        let points = self.store.trend(
            Some(&context.checkout_path),
            branch,
            suite,
            file_path,
            worktree_id,
            summary_window,
        )?;
        let detail = points
            .iter()
            .take(detail_snapshots)
            .map(compact_history_snapshot)
            .collect::<Vec<_>>();
        let summary = summarize_history(&points);
        Ok(json!({
            "status": if points.is_empty() { "not_measured" } else { "measured" },
            "detail": detail,
            "summary": summary
        }))
    }

    fn review_insight(
        &self,
        snapshot_id: Option<&str>,
        baseline_snapshot_id: Option<&str>,
        max_regions: usize,
    ) -> AppResult<Value> {
        let Some(snapshot_id) = snapshot_id else {
            return Ok(json!({"status":"not_measured","items":[]}));
        };
        let result = self
            .store
            .insights(snapshot_id, baseline_snapshot_id, max_regions)?;
        let targets = self.store.targets(snapshot_id, "priority", max_regions)?;
        let items = targets
            .into_iter()
            .take(max_regions)
            .map(|target| {
                let path = required_string_field(&target, "file_path", "coverage target")
                    .expect("stored coverage targets always contain file_path");
                let uncovered_lines =
                    required_i64_field(&target, "uncovered_lines", "coverage target")
                        .expect("stored coverage targets always contain uncovered_lines");
                let priority = required_i64_field(&target, "priority", "coverage target")
                    .expect("stored coverage targets always contain priority");
                json!({
                    "severity": if uncovered_lines > 0 { "high" } else { "medium" },
                    "category": "uncovered-target",
                    "title": "Uncovered executable region",
                    "detail": format!("{path} has {uncovered_lines} uncovered executable lines."),
                    "file_path": path,
                    "priority": priority,
                    "uncovered_lines": uncovered_lines,
                    "uncovered_branches": target["uncovered_branches"],
                    "uncovered_functions": target["uncovered_functions"],
                    "regions": target["regions"]
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "status": "measured",
            "summary": {"source":"ranked_targets","item_count":items.len(),"heuristic":result["summary"]},
            "items": items
        }))
    }

    /// Reads a bounded source range associated with a snapshot.
    pub fn source(
        &self,
        snapshot_id: &str,
        file_path: &str,
        start: i64,
        end: i64,
        cursor: Option<&str>,
        max_words: usize,
    ) -> AppResult<Value> {
        if start < 1 {
            return Err(AppError::Validation(
                "start must be a positive line number".to_owned(),
            ));
        }
        if end < start {
            return Err(AppError::Validation(
                "end must be greater than or equal to start".to_owned(),
            ));
        }
        let line_count = end - start + 1;
        if line_count > 200 {
            return Err(AppError::Validation(
                "source ranges may contain at most 200 lines".to_owned(),
            ));
        }
        let snapshot = self.store.snapshot(snapshot_id)?;
        self.store.file_coverage(snapshot_id, file_path)?;
        let lines = self
            .store
            .source_lines(snapshot_id, file_path, start, end)?;
        let line_range = (start, end);
        let coverage = self
            .store
            .lines_in_ranges(snapshot_id, file_path, &[line_range])?;
        let coverage_lines = coverage["lines"]
            .as_array()
            .expect("line coverage projections always contain lines");
        let (lines, red_regions) = annotate_source_lines(lines, Some(coverage_lines));
        let (lines, page) = self.page(
            &lines,
            cursor,
            max_words,
            &format!("source:{snapshot_id}:{file_path}:{start}:{end}"),
            None,
        )?;
        Ok(self.envelope(
            json!({"snapshot_commit_sha": required_string_field(&snapshot, "commit_sha", "snapshot")?, "source_resolution": self.store.source_resolution(snapshot_id, file_path)?, "file_path": file_path, "red_regions": red_regions, "lines": lines}),
            Some(
                snapshot["suite"]
                    .as_str()
                    .expect("stored snapshots always contain a suite"),
            ),
            Some(page),
        ))
    }

    /// Reads several disjoint source ranges in one bounded evidence response.
    ///
    /// The storage layer normalizes overlapping and adjacent ranges and caps
    /// the combined unique span at 200 lines. This keeps explicit audits from
    /// fanning out into one MCP request per uncovered region.
    pub fn source_ranges(
        &self,
        snapshot_id: &str,
        file_path: &str,
        ranges: Vec<LineRange>,
        cursor: Option<&str>,
        max_words: usize,
    ) -> AppResult<Value> {
        if ranges.is_empty() {
            return Err(AppError::Validation(
                "line_ranges must contain at least one range".to_owned(),
            ));
        }
        let snapshot = self.store.snapshot(snapshot_id)?;
        let snapshot_commit_sha = required_string_field(&snapshot, "commit_sha", "snapshot")?;
        self.store.file_coverage(snapshot_id, file_path)?;
        let coverage = self
            .store
            .lines_in_ranges(snapshot_id, file_path, &ranges)?;
        let normalized = coverage["requested_ranges"]
            .as_array()
            .expect("source projections always contain requested ranges")
            .iter()
            .map(|range| {
                (
                    range["start"]
                        .as_i64()
                        .expect("source ranges always contain a start"),
                    range["end"]
                        .as_i64()
                        .expect("source ranges always contain an end"),
                )
            })
            .collect::<Vec<_>>();
        let coverage_lines = coverage["lines"]
            .as_array()
            .expect("line coverage projections always contain lines");
        let mut range_values = Vec::new();
        for (start, end) in normalized {
            let source = self
                .store
                .source_lines(snapshot_id, file_path, start, end)?;
            let selected_coverage = coverage_lines
                .iter()
                .filter(|line| {
                    line.get("line_number")
                        .and_then(Value::as_i64)
                        .is_some_and(|number| number >= start && number <= end)
                })
                .cloned()
                .collect::<Vec<_>>();
            let (lines, red_regions) = annotate_source_lines(source, Some(&selected_coverage));
            range_values.push(json!({
                "start": start,
                "end": end,
                "red_regions": red_regions,
                "lines": lines
            }));
        }
        let (ranges, page) = self.page(
            &range_values,
            cursor,
            max_words,
            &format!("source-batch:{snapshot_id}:{file_path}"),
            None,
        )?;
        Ok(self.envelope(
            json!({
                "snapshot_commit_sha": snapshot_commit_sha,
                "source_resolution": self.store.source_resolution(snapshot_id, file_path)?,
                "file_path": file_path,
                "ranges": ranges
            }),
            Some(
                snapshot["suite"]
                    .as_str()
                    .expect("stored snapshots always contain a suite"),
            ),
            Some(page),
        ))
    }

    /// Reads grouped source ranges across one or more files in one bounded
    /// review projection.
    pub fn source_review(
        &self,
        snapshot_id: &str,
        ranges: Vec<(String, i64, i64)>,
        max_source_lines: usize,
        max_words: usize,
        max_bytes: usize,
    ) -> AppResult<Value> {
        validate_max_words(max_words)?;
        validate_review_byte_budget(max_bytes)?;
        if !(10..=500).contains(&max_source_lines) {
            return Err(AppError::Validation(
                "max_source_lines must be between 10 and 500".to_owned(),
            ));
        }
        if ranges.is_empty() {
            return Err(AppError::Validation(
                "source ranges must contain at least one file range".to_owned(),
            ));
        }
        if ranges.len() > 10 {
            return Err(AppError::Validation(
                "source ranges accept at most 10 ranges".to_owned(),
            ));
        }
        let snapshot = self.store.snapshot(snapshot_id)?;
        let snapshot_commit_sha = required_string_field(&snapshot, "commit_sha", "snapshot")?;
        let mut grouped: BTreeMap<String, Vec<LineRange>> = BTreeMap::new();
        let mut requested_lines = 0usize;
        for (file_path, start, end) in ranges {
            validate_source_file_path(&file_path)?;
            if start < 1 || end < start {
                return Err(AppError::Validation(
                    "source range must have positive bounds with end >= start".to_owned(),
                ));
            }
            // Saturating to the largest platform size keeps oversized ranges on the
            // normal bounded-budget error path even on 32-bit targets.
            let line_count =
                usize::try_from(end.saturating_sub(start).saturating_add(1)).unwrap_or(usize::MAX);
            requested_lines = requested_lines.saturating_add(line_count);
            if requested_lines > max_source_lines {
                return Err(AppError::Validation(format!(
                    "source ranges require {requested_lines} lines; reduce ranges or increase max_source_lines"
                )));
            }
            grouped.entry(file_path).or_default().push((start, end));
        }
        let mut sources = Vec::new();
        for (file_path, ranges) in grouped {
            let response = self.source_ranges(snapshot_id, &file_path, ranges, None, 5_000)?;
            let data = response["data"].clone();
            sources.push(json!({
                "file_path": file_path,
                "source_resolution": data["source_resolution"],
                "ranges": data["ranges"]
            }));
        }
        let response = self.envelope(
            json!({
                "focus": "source",
                "task": "source",
                "representation": "review",
                "claim_status": "supported",
                "reasons": [],
                "measurement": compact_snapshot(&snapshot, false),
                "baseline": Value::Null,
                "snapshot_commit_sha": snapshot_commit_sha,
                "source": sources
            }),
            Some(
                snapshot["suite"]
                    .as_str()
                    .expect("stored snapshots always contain a suite"),
            ),
            None,
        );
        let response = self.apply_budget(response, max_words)?;
        self.apply_byte_budget(response, max_bytes)
    }

    /// Returns detailed file lines for dashboard callers.
    pub fn file_detail(
        &self,
        snapshot_id: &str,
        file_path: &str,
        cursor: Option<&str>,
        max_words: usize,
        detailed: bool,
    ) -> AppResult<Value> {
        let snapshot = self.store.snapshot(snapshot_id)?;
        let file = self.store.file_coverage(snapshot_id, file_path)?;
        let lines = self
            .store
            .lines(snapshot_id, file_path, COLLECTION_FETCH_LIMIT)?;
        let lines = if detailed {
            lines
        } else {
            lines
                .into_iter()
                .map(|line| {
                    let keys = [
                        "line_number",
                        "hits",
                        "covered",
                        "count_line",
                        "total_branches",
                        "covered_branches",
                        "total_functions",
                        "covered_functions",
                    ];
                    let mut value = Map::new();
                    for key in keys {
                        value.insert(
                            key.to_owned(),
                            line.get(key).cloned().unwrap_or(Value::Null),
                        );
                    }
                    Value::Object(value)
                })
                .collect()
        };
        let (lines, page) = self.page(
            &lines,
            cursor,
            max_words,
            &format!("dashboard-file:{snapshot_id}:{file_path}:{detailed}"),
            None,
        )?;
        Ok(self.envelope(
            json!({"file": compact_file(&file, detailed), "lines": lines}),
            snapshot["suite"].as_str(),
            Some(page),
        ))
    }

    /// Applies per-project compaction settings.
    pub fn update_project_settings(&self, patch: ProjectSettingsPatch) -> AppResult<Value> {
        let settings = self.store.update_project_settings(patch)?;
        Ok(self.envelope(
            serde_json::to_value(settings).expect("project settings serialization is infallible"),
            None,
            None,
        ))
    }

    /// Runs compaction immediately for the selected project.
    pub fn compact_now(&self) -> AppResult<Value> {
        Ok(self.envelope(self.store.compact_now()?, None, None))
    }
}

fn update_worktree_progress(
    progress: &mut Value,
    points: Vec<Value>,
    detailed: bool,
) -> AppResult<()> {
    let Some(object) = progress.as_object_mut() else {
        return Err(AppError::Runtime(
            "worktree progress projection is not an object".to_owned(),
        ));
    };
    object.insert("points".to_owned(), Value::Array(points));
    if !detailed {
        let worktree = required_value(
            &Value::Object(object.clone()),
            "worktree",
            "worktree progress",
        )?
        .clone();
        let worktree_id = required_value(&worktree, "id", "worktree")
            .expect("stored worktree projections always contain id")
            .clone();
        let worktree_path = required_value(&worktree, "path", "worktree")
            .expect("stored worktree projections always contain path")
            .clone();
        let worktree_branch = required_value(&worktree, "branch", "worktree")
            .expect("stored worktree projections always contain branch")
            .clone();
        object.insert(
            "worktree".to_owned(),
            json!({"id":worktree_id,"path":worktree_path,"branch":worktree_branch}),
        );
    }
    Ok(())
}

fn required_value<'a>(value: &'a Value, key: &str, context: &str) -> AppResult<&'a Value> {
    value
        .get(key)
        .ok_or_else(|| AppError::Runtime(format!("{context} is missing required field '{key}'")))
}

fn required_string_field(value: &Value, key: &str, context: &str) -> AppResult<String> {
    required_value(value, key, context)?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            AppError::Runtime(format!(
                "{context} field '{key}' must be a non-empty string"
            ))
        })
}

fn required_i64_field(value: &Value, key: &str, context: &str) -> AppResult<i64> {
    required_value(value, key, context)?
        .as_i64()
        .ok_or_else(|| AppError::Runtime(format!("{context} field '{key}' must be an integer")))
}

#[cfg(test)]
fn required_array_field<'a>(
    value: &'a Value,
    key: &str,
    context: &str,
) -> AppResult<&'a Vec<Value>> {
    required_value(value, key, context)?
        .as_array()
        .ok_or_else(|| AppError::Runtime(format!("{context} field '{key}' must be an array")))
}

/// Counts stable serialized response words.
pub fn serialized_word_count(value: &Value) -> usize {
    value
        .to_string()
        .split(|character: char| character.is_whitespace() || "[]{} ,:".contains(character))
        .filter(|token| !token.is_empty())
        .count()
}

/// Encodes a cursor for one query scope.
pub fn encode_cursor(anchor: &str, scope: &str, occurrence: usize) -> AppResult<String> {
    if occurrence < 1 {
        return Err(AppError::Validation(
            "cursor occurrence must be positive".to_owned(),
        ));
    }
    let payload = json!({"after": anchor, "occurrence": occurrence, "scope": cursor_scope(scope)})
        .to_string();
    Ok(URL_SAFE_NO_PAD.encode(payload.as_bytes()))
}

/// Decodes and validates a cursor for one query scope.
pub fn decode_cursor(cursor: &str, scope: &str) -> AppResult<(String, usize)> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AppError::Validation("invalid pagination cursor".to_owned()))?;
    let payload: Value = serde_json::from_slice(&bytes)
        .map_err(|_| AppError::Validation("invalid pagination cursor".to_owned()))?;
    let object = payload
        .as_object()
        .ok_or_else(|| AppError::Validation("invalid pagination cursor".to_owned()))?;
    let anchor = object
        .get("after")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Validation("invalid pagination cursor anchor".to_owned()))?;
    let occurrence = object
        .get("occurrence")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| AppError::Validation("invalid pagination cursor occurrence".to_owned()))?;
    let scope_value = object
        .get("scope")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Validation("invalid pagination cursor scope".to_owned()))?;
    if anchor.len() != 64
        || !anchor
            .chars()
            .all(|character| character.is_ascii_hexdigit())
        || occurrence < 1
        || scope_value != cursor_scope(scope)
    {
        return Err(AppError::Validation(
            "pagination cursor does not belong to this query".to_owned(),
        ));
    }
    Ok((anchor.to_owned(), occurrence))
}

fn validate_max_words(max_words: usize) -> AppResult<()> {
    if !(50..=5000).contains(&max_words) {
        return Err(AppError::Validation(
            "max_words must be between 50 and 5000".to_owned(),
        ));
    }
    Ok(())
}

fn validate_review_byte_budget(max_bytes: usize) -> AppResult<()> {
    if !(1_000..=2_000_000).contains(&max_bytes) {
        return Err(AppError::Validation(
            "max_bytes must be between 1000 and 2000000".to_owned(),
        ));
    }
    Ok(())
}

fn validate_relative_report_path(report_path: &str, checkout_path: &str) -> AppResult<()> {
    let path = Path::new(report_path);
    if report_path.trim().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(AppError::Validation(
            "report_path must be a repository-relative path without parent traversal".to_owned(),
        ));
    }
    let root = Path::new(checkout_path).canonicalize()?;
    let resolved = root.join(path).canonicalize()?;
    if !resolved.starts_with(&root) {
        return Err(AppError::Validation(
            "report_path must remain inside the selected repository".to_owned(),
        ));
    }
    Ok(())
}

fn validate_source_file_path(file_path: &str) -> AppResult<()> {
    let path = Path::new(file_path);
    if file_path.trim().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(AppError::Validation(
            "file_path must be a repository-relative path without parent traversal".to_owned(),
        ));
    }
    Ok(())
}

fn cursor_scope(scope: &str) -> String {
    let digest = Sha256::digest(scope.as_bytes());
    hex_prefix(&digest, 8)
}

fn cursor_anchor(value: &Value) -> String {
    let canonical = canonical_json(value);
    let digest = Sha256::digest(canonical.as_bytes());
    hex_prefix(&digest, digest.len())
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(object) => {
            let ordered = object
                .iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect::<BTreeMap<_, _>>();
            format!(
                "{{{}}}",
                ordered
                    .into_iter()
                    .map(|(key, value)| format!("{key:?}:{value}"))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        _ => value.to_string(),
    }
}

fn compact_project(value: &Value) -> Value {
    let keys = [
        "id",
        "snapshot_count",
        "branch_count",
        "command_count",
        "run_count",
        "latest_snapshot_id",
        "latest_snapshot_age",
        "latest_snapshot_age_seconds",
        "latest_run_age",
        "latest_run_age_seconds",
        "latest_branch",
        "latest_commit_sha",
        "latest_suite",
        "latest_format",
        "total_lines",
        "covered_lines",
        "line_rate",
        "total_branches",
        "covered_branches",
        "branch_rate",
        "total_functions",
        "covered_functions",
        "function_rate",
        "total_regions",
        "covered_regions",
        "region_rate",
        "warnings",
        "compaction",
    ];
    let mut result = Map::new();
    for key in keys {
        result.insert(
            key.to_owned(),
            value.get(key).cloned().unwrap_or(Value::Null),
        );
    }
    Value::Object(result)
}

fn compact_snapshot(value: &Value, detailed: bool) -> Value {
    let keys = [
        "id",
        "created_at",
        "age_seconds",
        "age",
        "branch",
        "commit_sha",
        "base_ref",
        "suite",
        "format",
        "total_lines",
        "covered_lines",
        "line_rate",
        "total_branches",
        "covered_branches",
        "branch_rate",
        "total_functions",
        "covered_functions",
        "function_rate",
        "total_regions",
        "covered_regions",
        "region_rate",
    ];
    let mut result = Map::new();
    for key in keys {
        result.insert(
            key.to_owned(),
            value.get(key).cloned().unwrap_or(Value::Null),
        );
    }
    result.insert(
        "measurement_checkout_path".to_owned(),
        value.get("repo_path").cloned().unwrap_or(Value::Null),
    );
    result.insert(
        "warnings".to_owned(),
        value
            .get("warnings")
            .cloned()
            .filter(|value| !value.is_null())
            .unwrap_or_else(|| json!([])),
    );
    if detailed {
        result.insert(
            "repo_path".to_owned(),
            value.get("repo_path").cloned().unwrap_or(Value::Null),
        );
        result.insert(
            "report_path".to_owned(),
            value.get("report_path").cloned().unwrap_or(Value::Null),
        );
        result.insert(
            "metadata".to_owned(),
            value
                .get("metadata")
                .cloned()
                .filter(|value| !value.is_null())
                .unwrap_or_else(|| json!({})),
        );
    }
    Value::Object(result)
}

fn compact_file(value: &Value, detailed: bool) -> Value {
    let keys = [
        "file_path",
        "total_lines",
        "covered_lines",
        "line_rate",
        "total_branches",
        "covered_branches",
        "branch_rate",
        "total_functions",
        "covered_functions",
        "function_rate",
        "total_regions",
        "covered_regions",
        "region_rate",
    ];
    let mut result = Map::new();
    for key in keys {
        result.insert(
            key.to_owned(),
            value.get(key).cloned().unwrap_or(Value::Null),
        );
    }
    if detailed {
        result.insert(
            "raw_metrics".to_owned(),
            value
                .get("raw_metrics")
                .cloned()
                .filter(|value| !value.is_null())
                .unwrap_or_else(|| json!({})),
        );
    }
    Value::Object(result)
}

fn compact_changed_regions(values: &[Value]) -> Value {
    let mut grouped: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
    for value in values {
        let Some(path) = value.get("file_path").and_then(Value::as_str) else {
            continue;
        };
        let Some(status) = value.get("status").and_then(Value::as_str) else {
            continue;
        };
        let Some(start) = value.get("start").and_then(Value::as_i64) else {
            continue;
        };
        let Some(end) = value.get("end").and_then(Value::as_i64) else {
            continue;
        };
        let line_count = value
            .get("line_count")
            .and_then(Value::as_i64)
            .unwrap_or_else(|| end.saturating_sub(start).saturating_add(1));
        let entry = grouped.entry(path.to_owned()).or_default();
        let ranges = entry
            .entry(status.to_owned())
            .or_insert_with(|| Value::Array(Vec::new()));
        ranges
            .as_array_mut()
            .expect("compact region groups always store arrays")
            .push(json!([start, end, line_count]));
    }
    Value::Array(
        grouped
            .into_iter()
            .map(|(path, values)| {
                let mut object = Map::new();
                object.insert("path".to_owned(), json!(path));
                for (status, ranges) in values {
                    object.insert(status, ranges);
                }
                Value::Object(object)
            })
            .collect(),
    )
}

fn compact_review_change(change: &mut Value) {
    if let Some(object) = change.as_object_mut() {
        object.remove("baseline");
        object.remove("current");
    }
    let Some(changed_code) = change.get_mut("changed_code") else {
        return;
    };
    let Some(files) = changed_code.get("files").and_then(Value::as_array) else {
        return;
    };
    let compact_files = files
        .iter()
        .filter_map(|file| {
            let path = file.get("path")?.as_str()?;
            let mut ranges = Vec::new();
            let object = file
                .as_object()
                .expect("a changed-code file with a path is an object");
            for (status, symbol) in [
                ("covered", "+"),
                ("uncovered", "!"),
                ("unmeasured", "?"),
                ("non_executable", "."),
                ("branch_gap", "~"),
            ] {
                let Some(values) = object.get(status).and_then(Value::as_array) else {
                    continue;
                };
                for range in values {
                    let Some(items) = range.as_array() else {
                        continue;
                    };
                    let Some(start) = items.first().and_then(Value::as_i64) else {
                        continue;
                    };
                    let Some(end) = items.get(1).and_then(Value::as_i64) else {
                        continue;
                    };
                    ranges.push(json!([start, end, symbol]));
                }
            }
            ranges.sort_by_key(|range| {
                (
                    range.get(0).and_then(Value::as_i64).unwrap_or_default(),
                    range.get(1).and_then(Value::as_i64).unwrap_or_default(),
                )
            });
            Some(json!({"p": path, "r": ranges}))
        })
        .collect::<Vec<_>>();
    changed_code["legend"] = json!({
        "+": "added executable line covered",
        "!": "added executable line uncovered",
        "~": "changed line has a branch gap",
        ".": "added line is non-executable",
        "?": "coverage is unavailable or unmeasured"
    });
    changed_code["files"] = Value::Array(compact_files);
    if let Some(regions) = change.get("regions").cloned() {
        change["regions"] = compact_region_groups(&regions);
    }
    if let Some(files) = change.get("files").and_then(Value::as_array).cloned() {
        change["file_legend"] = json!({
            "p": "file_path",
            "l": ["baseline_total_lines", "current_total_lines", "line_rate_delta"],
            "b": ["baseline_branch_rate", "current_branch_rate", "branch_rate_delta"],
            "f": ["baseline_function_rate", "current_function_rate", "function_rate_delta"],
            "r": ["baseline_region_rate", "current_region_rate", "region_rate_delta"]
        });
        change["files"] = Value::Array(
            files
                .iter()
                .filter_map(compact_file_change_token)
                .collect::<Vec<_>>(),
        );
    }
    change["representation"] = json!("compact");
}

fn compact_file_change_token(value: &Value) -> Option<Value> {
    let path = value.get("file_path")?.clone();
    let values = |keys: &[&str]| {
        Value::Array(
            keys.iter()
                .map(|key| value.get(*key).cloned().unwrap_or(Value::Null))
                .collect(),
        )
    };
    Some(json!({
        "p": path,
        "l": values(&["baseline_total_lines", "current_total_lines", "line_rate_delta"]),
        "b": values(&["baseline_branch_rate", "current_branch_rate", "branch_rate_delta"]),
        "f": values(&["baseline_function_rate", "current_function_rate", "function_rate_delta"]),
        "r": values(&["baseline_region_rate", "current_region_rate", "region_rate_delta"])
    }))
}

fn review_next_action(changed_code: &Value, regions: &[Value]) -> Value {
    match changed_code.get("status").and_then(Value::as_str) {
        Some("measured") => {
            let has_uncovered = changed_code
                .get("files")
                .and_then(Value::as_array)
                .is_some_and(|files| {
                    files.iter().any(|file| {
                        file.get("uncovered")
                            .and_then(Value::as_array)
                            .is_some_and(|ranges| !ranges.is_empty())
                    })
                });
            if has_uncovered {
                json!({"kind":"add_tests","reason":"new executable lines are uncovered"})
            } else if regions
                .iter()
                .any(|region| region.get("status").and_then(Value::as_str) == Some("regressed"))
            {
                json!({"kind":"inspect_regression","reason":"a previously measured region regressed"})
            } else {
                json!({"kind":"review_existing_gaps","reason":"changed code is measured; inspect ranked gaps if more coverage is needed"})
            }
        }
        Some("no_source_changes") => {
            json!({"kind":"review_existing_gaps","reason":"no source lines changed between the selected commits"})
        }
        Some("no_baseline") => {
            json!({"kind":"establish_baseline","reason":"no compatible comparison baseline is available"})
        }
        _ => json!({"kind":"obtain_measurement","reason":"changed-code coverage is not measured"}),
    }
}

fn compact_region_groups(value: &Value) -> Value {
    let Some(files) = value.as_array() else {
        return Value::Array(Vec::new());
    };
    let mut result = Vec::new();
    for file in files {
        let Some(object) = file.as_object() else {
            continue;
        };
        let Some(path) = object.get("path").and_then(Value::as_str) else {
            continue;
        };
        let mut ranges = Vec::new();
        for (status, symbol) in [
            ("improved", "+"),
            ("new", "+"),
            ("regressed", "!"),
            ("removed", "-"),
            ("changed", "~"),
        ] {
            let Some(values) = object.get(status).and_then(Value::as_array) else {
                continue;
            };
            for range in values {
                let Some(items) = range.as_array() else {
                    continue;
                };
                let Some(start) = items.first().and_then(Value::as_i64) else {
                    continue;
                };
                let Some(end) = items.get(1).and_then(Value::as_i64) else {
                    continue;
                };
                ranges.push(json!([start, end, symbol]));
            }
        }
        ranges.sort_by_key(|range| {
            (
                range.get(0).and_then(Value::as_i64).unwrap_or_default(),
                range.get(1).and_then(Value::as_i64).unwrap_or_default(),
            )
        });
        result.push(json!({"p":path,"r":ranges}));
    }
    Value::Array(result)
}

fn expand_review_change(change: &mut Value) {
    change["representation"] = json!("audit");
    change["audit"] = json!({
        "regions_are_uncompressed": true,
        "line_records_are_available_via": "coverage_review(task=audit)"
    });
}

fn compact_file_change(value: &Value) -> Value {
    let keys = [
        "file_path",
        "baseline_total_lines",
        "current_total_lines",
        "baseline_covered_lines",
        "current_covered_lines",
        "line_rate_delta",
        "baseline_branch_rate",
        "current_branch_rate",
        "branch_rate_delta",
        "baseline_function_rate",
        "current_function_rate",
        "function_rate_delta",
        "baseline_region_rate",
        "current_region_rate",
        "region_rate_delta",
    ];
    let mut result = Map::new();
    for key in keys {
        if let Some(value) = value.get(key) {
            result.insert(key.to_owned(), value.clone());
        }
    }
    Value::Object(result)
}

fn compact_history_snapshot(value: &Value) -> Value {
    let keys = [
        "id",
        "created_at",
        "branch",
        "commit_sha",
        "suite",
        "file_path",
        "total_lines",
        "covered_lines",
        "line_rate",
        "total_branches",
        "covered_branches",
        "branch_rate",
        "total_functions",
        "covered_functions",
        "function_rate",
        "total_regions",
        "covered_regions",
        "region_rate",
    ];
    let mut result = Map::new();
    for key in keys {
        if let Some(value) = value.get(key) {
            result.insert(key.to_owned(), value.clone());
        }
    }
    Value::Object(result)
}

fn history_metric(points: &[Value], key: &str) -> Value {
    let chronological = points
        .iter()
        .rev()
        .filter_map(|point| point.get(key).and_then(Value::as_f64));
    let values = chronological.collect::<Vec<_>>();
    let Some(first) = values.first().copied() else {
        return Value::Null;
    };
    let last = values.last().copied().unwrap_or(first);
    let min = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let trend = if last > first {
        "improving"
    } else if last < first {
        "regressing"
    } else {
        "unchanged"
    };
    json!({"first":first,"last":last,"min":min,"max":max,"trend":trend})
}

fn summarize_history(points: &[Value]) -> Value {
    let chronological = points.iter().rev().collect::<Vec<_>>();
    let mut regression_runs = 0usize;
    let mut improvement_runs = 0usize;
    let mut unchanged_runs = 0usize;
    for pair in chronological.windows(2) {
        let before = pair[0].get("line_rate").and_then(Value::as_f64);
        let after = pair[1].get("line_rate").and_then(Value::as_f64);
        match (before, after) {
            (Some(before), Some(after)) if after > before => improvement_runs += 1,
            (Some(before), Some(after)) if after < before => regression_runs += 1,
            (Some(_), Some(_)) => unchanged_runs += 1,
            _ => {}
        }
    }
    json!({
        "window": points.len(),
        "available": points.len(),
        "line_rate": history_metric(points, "line_rate"),
        "branch_rate": history_metric(points, "branch_rate"),
        "function_rate": history_metric(points, "function_rate"),
        "region_rate": history_metric(points, "region_rate"),
        "regression_runs": regression_runs,
        "improvement_runs": improvement_runs,
        "unchanged_runs": unchanged_runs
    })
}

fn compact_command(value: &Value, detailed: bool) -> Value {
    let keys = [
        "id",
        "name",
        "command",
        "cwd",
        "shell",
        "artifact_specs",
        "enabled",
        "created_at",
        "duration_estimate_ms",
        "duration_p90_ms",
        "duration_sample_count",
    ];
    let mut result = Map::new();
    for key in keys {
        result.insert(
            key.to_owned(),
            value.get(key).cloned().unwrap_or(Value::Null),
        );
    }
    if result.get("artifact_specs").is_none_or(Value::is_null) {
        result.insert("artifact_specs".to_owned(), json!([]));
    }
    if detailed {
        for key in ["approved_by", "approval_note", "branch", "commit_sha"] {
            result.insert(
                key.to_owned(),
                value.get(key).cloned().unwrap_or(Value::Null),
            );
        }
    }
    Value::Object(result)
}

fn compact_run_result(value: &Value, detailed: bool) -> Value {
    if detailed {
        return value.clone();
    }
    let mut result = value.clone();
    if let Some(object) = result.as_object_mut() {
        object.remove("parsed_summary");
        object.remove("artifact_paths");
    }
    result
}

fn strip_log_metadata(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        object.remove("case_sensitive");
        object.remove("streams");
    }
}

fn annotate_source_lines(
    source: Vec<Value>,
    coverage: Option<&Vec<Value>>,
) -> (Vec<Value>, Vec<Value>) {
    let mut measurements = BTreeMap::new();
    for line in coverage.into_iter().flatten() {
        if let Some(number) = line.get("line_number").and_then(Value::as_i64) {
            measurements.insert(number, line);
        }
    }
    let mut red_lines = Vec::new();
    let mut result = Vec::new();
    for mut line in source {
        let Some(object) = line.as_object_mut() else {
            result.push(line);
            continue;
        };
        let Some(number) = object.get("line_number").and_then(Value::as_i64) else {
            result.push(line);
            continue;
        };
        let measurement = measurements.get(&number).copied();
        let count_line = measurement
            .and_then(|value| value.get("count_line"))
            .and_then(Value::as_bool);
        let covered = measurement
            .and_then(|value| value.get("covered"))
            .and_then(Value::as_bool);
        let branch_gap = measurement.and_then(|value| {
            let total = value.get("total_branches").and_then(Value::as_i64)?;
            let covered = value.get("covered_branches").and_then(Value::as_i64)?;
            total.checked_sub(covered).map(|gap| gap.max(0))
        });
        let (status, marker) = match (count_line, covered, branch_gap) {
            (Some(true), Some(false), _) => {
                red_lines.push(number);
                ("uncovered", "red")
            }
            (Some(true), Some(true), Some(gap)) if gap > 0 => ("branch_gap", "yellow"),
            (Some(true), Some(true), _) => ("covered", "green"),
            (Some(false), _, _) => ("non_executable", "gray"),
            _ => ("unmeasured", "gray"),
        };
        object.insert("status".to_owned(), json!(status));
        object.insert("marker".to_owned(), json!(marker));
        if let Some(gap) = branch_gap.filter(|gap| *gap > 0) {
            object.insert("uncovered_branches".to_owned(), json!(gap));
        }
        result.push(line);
    }
    (result, line_regions(&red_lines))
}

fn line_regions(numbers: &[i64]) -> Vec<Value> {
    let mut numbers = numbers.to_vec();
    numbers.sort_unstable();
    numbers.dedup();
    let mut regions = Vec::new();
    let mut current: Option<(i64, i64)> = None;
    for number in numbers {
        if let Some((start, end)) = current.as_mut() {
            if number <= end.saturating_add(1) {
                *end = (*end).max(number);
                continue;
            }
            let line_count = *end - *start + 1;
            regions.push(json!({
                "start": *start,
                "end": *end,
                "line_count": line_count,
            }));
        }
        current = Some((number, number));
    }
    if let Some((start, end)) = current {
        let line_count = end - start + 1;
        regions.push(json!({
            "start": start,
            "end": end,
            "line_count": line_count,
        }));
    }
    regions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServerConfig;
    use std::process::Command;

    #[test]
    fn cursor_and_budget_helpers_cover_valid_and_invalid_inputs() {
        assert!(serialized_word_count(&json!({"a": ["one", "two"]})) > 0);
        let anchor = "a".repeat(64);
        assert!(encode_cursor(&anchor, "scope", 0).is_err());
        let cursor = encode_cursor(&anchor, "scope", 1).expect("cursor");
        assert_eq!(
            decode_cursor(&cursor, "scope").unwrap(),
            (anchor.clone(), 1)
        );
        assert!(decode_cursor("not-base64", "scope").is_err());
        let invalid_json = URL_SAFE_NO_PAD.encode(b"not-json");
        assert!(decode_cursor(&invalid_json, "scope").is_err());
        let scalar_json = URL_SAFE_NO_PAD.encode(b"[]");
        assert!(decode_cursor(&scalar_json, "scope").is_err());
        let missing_anchor = URL_SAFE_NO_PAD.encode(br#"{"occurrence":1,"scope":"x"}"#);
        assert!(decode_cursor(&missing_anchor, "scope").is_err());
        let missing_occurrence = URL_SAFE_NO_PAD.encode(
            format!(
                r#"{{"after":"{}","scope":"{}"}}"#,
                anchor,
                cursor_scope("scope")
            )
            .as_bytes(),
        );
        assert!(decode_cursor(&missing_occurrence, "scope").is_err());
        let missing_scope = URL_SAFE_NO_PAD
            .encode(format!(r#"{{"after":"{}","occurrence":1}}"#, anchor).as_bytes());
        assert!(decode_cursor(&missing_scope, "scope").is_err());
        let wrong_scope = encode_cursor(&anchor, "other", 1).unwrap();
        assert!(decode_cursor(&wrong_scope, "scope").is_err());
        let short = encode_cursor(&"z".repeat(63), "scope", 1).unwrap();
        assert!(decode_cursor(&short, "scope").is_err());
        let non_hex = encode_cursor(&format!("{}g", "a".repeat(63)), "scope", 1).unwrap();
        assert!(decode_cursor(&non_hex, "scope").is_err());
        assert!(validate_max_words(49).is_err());
        assert!(validate_max_words(5001).is_err());
        assert!(validate_max_words(600).is_ok());
    }

    #[test]
    fn strict_projection_validation_and_comparison_errors_are_explicit() {
        assert!(required_value(&json!({}), "id", "projection").is_err());
        assert!(required_string_field(&json!({}), "id", "projection").is_err());
        assert!(required_string_field(&json!({"id":""}), "id", "projection").is_err());
        assert!(required_i64_field(&json!({}), "count", "projection").is_err());
        assert!(required_i64_field(&json!({"count":"1"}), "count", "projection").is_err());
        assert!(required_array_field(&json!({}), "items", "projection").is_err());
        assert!(required_array_field(&json!({"items":{}}), "items", "projection").is_err());
        let mut missing_worktree = json!({});
        assert!(update_worktree_progress(&mut missing_worktree, Vec::new(), false).is_err());

        let directory = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        std::fs::write(outside.path().join("report.lcov"), "TN:\n").expect("outside report");
        std::os::unix::fs::symlink(outside.path(), directory.path().join("outside-link"))
            .expect("outside symlink");
        assert!(
            validate_relative_report_path(
                "outside-link/report.lcov",
                directory.path().to_str().unwrap(),
            )
            .is_err()
        );
        assert!(validate_relative_report_path("report.lcov", "/missing-checkout").is_err());
        assert!(
            validate_relative_report_path(
                "missing/report.lcov",
                directory.path().to_str().unwrap()
            )
            .is_err()
        );
        std::fs::create_dir_all(directory.path().join("src")).expect("source directory");
        std::fs::write(directory.path().join("src/a.py"), "one\ntwo\n").expect("source file");
        for args in [
            vec!["init", "-b", "main"],
            vec!["config", "user.email", "rust@example.com"],
            vec!["config", "user.name", "Rust Tests"],
            vec!["add", "."],
            vec!["commit", "-m", "base"],
        ] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(directory.path())
                    .args(args)
                    .status()
                    .expect("git")
                    .success()
            );
        }
        for args in [
            vec!["init", "-b", "main"],
            vec!["config", "user.email", "foreign@example.com"],
            vec!["config", "user.name", "Foreign Tests"],
            vec!["add", "."],
            vec!["commit", "-m", "foreign base"],
        ] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(outside.path())
                    .args(args)
                    .output()
                    .expect("git")
                    .status
                    .success()
            );
        }
        let git_commit = |reference: &str| {
            String::from_utf8(
                Command::new("git")
                    .arg("-C")
                    .arg(directory.path())
                    .args(["rev-parse", reference])
                    .output()
                    .expect("git")
                    .stdout,
            )
            .expect("commit sha")
            .trim()
            .to_owned()
        };
        let base_commit = git_commit("HEAD");
        let report = directory.path().join("coverage.lcov");
        std::fs::write(&report, "TN:\nSF:src/a.py\nDA:1,1\nend_of_record\n")
            .expect("coverage report");
        let config = ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 59_471,
            default_repository_path: None,
            common_db_path: directory.path().join("common.duckdb"),
            run_retention: 100,
            run_concurrency: 1,
            mcp_http_concurrency: 16,
            db_pool_size: 4,
            db_acquire_timeout_ms: 5_000,
            db_query_timeout_ms: 30_000,
            http_request_timeout_seconds: 60,
            http_max_body_bytes: 1_048_576,
            run_log_max_bytes: 10 * 1024 * 1024,
            default_compaction_after_days: 30,
            default_compaction_interval_seconds: 3_600,
            default_compaction_batch_size: 100,
        };
        let store =
            CoverageStore::open(directory.path().join("coverage.duckdb"), config).expect("store");
        let project = store.ensure_project(directory.path()).expect("project");
        let baseline = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some(&base_commit),
                None,
                "unit",
            )
            .expect("baseline");
        std::fs::write(directory.path().join("src/a.py"), "one\ntwo\nthree\n")
            .expect("changed source");
        for args in [vec!["add", "."], vec!["commit", "-m", "change"]] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(directory.path())
                    .args(args)
                    .status()
                    .expect("git")
                    .success()
            );
        }
        let current_commit = git_commit("HEAD");
        std::fs::write(
            &report,
            "TN:\nSF:src/a.py\nDA:1,1\nDA:2,0\nBRDA:2,0,0,-\nFN:4,func\nFNDA:0,func\nend_of_record\n",
        )
        .expect("current coverage report");
        let current = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some(&current_commit),
                None,
                "unit",
            )
            .expect("current");
        let mut many_report = "TN:\nSF:src/a.py\nDA:1,1\n".to_owned();
        for line in 3..=15 {
            many_report.push_str(&format!("DA:{line},0\n"));
        }
        for line in 20..=25 {
            many_report.push_str(&format!("DA:{line},0\n"));
        }
        many_report.push_str("end_of_record\n");
        std::fs::write(&report, many_report).expect("bounded coverage report");
        let many_current = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some("missing-current"),
                None,
                "unit",
            )
            .expect("bounded current");
        let many_source = (1..=25).fold(String::new(), |mut source, line| {
            source.push_str(&format!("line {line}\n"));
            source
        });
        std::fs::write(directory.path().join("src/a.py"), many_source).expect("long source");
        let service = CoverageService::new(
            store.clone(),
            RequestContext {
                repo_key: project.repo_key,
                checkout_path: project.repo_path,
                suite: None,
            },
        );
        std::fs::write(
            &report,
            "TN:\nSF:src/a.py\nDA:1,1\nBRDA:1,0,0,-\nend_of_record\n",
        )
        .expect("branch-only coverage report");
        let branch_only = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some(&current_commit),
                None,
                "unit",
            )
            .expect("branch-only snapshot");
        let branch_only_insight = service
            .coverage_review(
                "insight",
                Some(branch_only["id"].as_str().unwrap()),
                None,
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                3,
                120,
                600,
                12_000,
                "review",
            )
            .expect("branch-only insight");
        assert_eq!(
            branch_only_insight["data"]["insight"]["items"][0]["severity"],
            "medium"
        );
        assert!(
            service
                .apply_budget(json!({"data": "word ".repeat(100)}), 50)
                .is_err()
        );
        assert!(service.apply_budget(json!({"data":"word"}), 49).is_err());
        assert!(service.apply_budget(json!({"data": "word"}), 600).is_ok());
        assert!(service.validate_repository_path(None).is_ok());
        assert!(
            service
                .validate_repository_path(Some(outside.path().to_str().unwrap()))
                .is_err()
        );
        assert!(service.validate_repository_path(Some("\0")).is_err());
        assert_ne!(
            inspect_git(outside.path()).unwrap().repo_key,
            service.context(None).repo_key
        );
        assert!(
            service
                .command_registration(
                    "foreign-command",
                    "true",
                    true,
                    "tester",
                    "foreign repository",
                    Some(outside.path().to_str().unwrap()),
                    "/bin/sh",
                    None,
                    false,
                )
                .is_err()
        );
        assert!(
            service
                .command_registration(
                    "missing-command-cwd",
                    "true",
                    true,
                    "tester",
                    "missing checkout",
                    Some("/tmp"),
                    "/bin/sh",
                    None,
                    false,
                )
                .is_err()
        );
        assert!(
            service
                .command_registration(
                    "invalid-command-cwd",
                    "true",
                    true,
                    "tester",
                    "invalid checkout path",
                    Some("\0"),
                    "/bin/sh",
                    None,
                    false,
                )
                .is_err()
        );
        assert!(service.project_context(None, 600, true).is_ok());
        assert!(service.page(&[], None, 600, "empty-page", None).is_ok());
        assert!(
            service
                .page(
                    &[json!({"value":"one"})],
                    Some("invalid"),
                    600,
                    "page",
                    None
                )
                .is_err()
        );
        assert!(
            service
                .page(&[json!({"value":"one"})], None, 49, "page", None)
                .is_err()
        );
        assert!(
            service
                .page(&[json!({"value":"one"})], None, 600, "page", Some(2))
                .is_err()
        );
        let over_limit = vec![json!({"value":"one"}); MAX_COLLECTION_RECORDS + 1];
        assert!(service.page(&over_limit, None, 600, "page", None).is_err());
        let paged_values = vec![
            json!({"value":"same ".repeat(60)}),
            json!({"value":"same ".repeat(60)}),
        ];
        let (_, bounded_page) = service
            .page(
                &[json!({"value":"one"}), json!({"value":"word ".repeat(60)})],
                None,
                50,
                "bounded-page",
                None,
            )
            .expect("bounded page");
        assert!(bounded_page["truncated"].as_bool().unwrap());
        let (_, page) = service
            .page(&paged_values, None, 50, "duplicate-page", None)
            .expect("first duplicate page");
        let next_cursor = page["next_cursor"].as_str().expect("next cursor");
        assert!(
            service
                .page(&paged_values, Some(next_cursor), 50, "duplicate-page", None,)
                .is_ok()
        );
        let missing_anchor = encode_cursor(&"f".repeat(64), "duplicate-page", 1).unwrap();
        assert!(
            service
                .page(
                    &paged_values,
                    Some(&missing_anchor),
                    50,
                    "duplicate-page",
                    None
                )
                .is_err()
        );
        let second_occurrence =
            encode_cursor(&cursor_anchor(&paged_values[0]), "duplicate-page", 2).unwrap();
        assert!(
            service
                .page(
                    &paged_values,
                    Some(&second_occurrence),
                    50,
                    "duplicate-page",
                    None
                )
                .is_ok()
        );
        let no_measurement = service
            .coverage_review(
                "all",
                None,
                None,
                None,
                Some("missing-suite"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                3,
                120,
                600,
                12_000,
                "review",
            )
            .expect("no measurement review");
        assert_eq!(no_measurement["data"]["change"]["status"], "not_measured");
        assert_eq!(no_measurement["data"]["history"]["status"], "not_measured");
        assert_eq!(no_measurement["data"]["insight"]["status"], "not_measured");
        assert_eq!(
            service
                .coverage_review(
                    "history",
                    None,
                    None,
                    None,
                    Some("missing-suite"),
                    Some("main"),
                    None,
                    2,
                    10,
                    10,
                    10,
                    false,
                    3,
                    120,
                    600,
                    12_000,
                    "review",
                )
                .unwrap()["data"]["claim_status"],
            "not_measured"
        );
        assert!(
            service
                .project_context(Some("invalid"), 600, false)
                .is_err()
        );
        assert_eq!(
            service
                .coverage_review(
                    "insight",
                    None,
                    None,
                    None,
                    Some("missing-suite"),
                    Some("main"),
                    None,
                    2,
                    10,
                    10,
                    10,
                    false,
                    3,
                    120,
                    600,
                    12_000,
                    "review",
                )
                .unwrap()["data"]["claim_status"],
            "not_measured"
        );
        assert_eq!(
            service
                .coverage_review(
                    "history",
                    None,
                    None,
                    None,
                    Some("unit"),
                    Some("missing-branch"),
                    None,
                    2,
                    10,
                    10,
                    10,
                    false,
                    3,
                    120,
                    600,
                    12_000,
                    "review",
                )
                .unwrap()["data"]["claim_status"],
            "not_measured"
        );
        let mut no_terminal = json!({"terminal":false});
        service.attach_terminal_review(&mut no_terminal);
        let mut successful_review = json!({
            "terminal":true,
            "coverage_ingest":{"snapshot_ids":[current["id"].clone()]}
        });
        service.attach_terminal_review(&mut successful_review);
        assert!(successful_review["coverage_review"].is_object());
        let mut failed_review = json!({
            "terminal":true,
            "coverage_ingest":{"snapshot_ids":["missing-snapshot"]}
        });
        service.attach_terminal_review(&mut failed_review);
        assert_eq!(failed_review["coverage_review"]["status"], "unavailable");
        let mut non_terminal_review = json!({
            "terminal":false,
            "coverage_ingest":{"snapshot_ids":["missing-snapshot"]}
        });
        service.attach_terminal_review(&mut non_terminal_review);
        assert!(service.source("missing", "a.py", 0, 1, None, 600).is_err());
        assert!(
            service
                .source("missing", "a.py", 1, 201, None, 600)
                .is_err()
        );
        assert!(service.source("missing", "a.py", 1, 0, None, 600).is_err());
        assert!(
            service
                .source("missing", "a.py", i64::MIN, i64::MAX, None, 600)
                .is_err()
        );
        assert!(
            service
                .coverage_comparison(
                    "overview",
                    current["id"].as_str(),
                    baseline["id"].as_str(),
                    None,
                    Some("other"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_err()
        );
        assert!(
            service
                .coverage_comparison(
                    "regions",
                    None,
                    None,
                    None,
                    Some("unit"),
                    Some("src/a.py"),
                    false,
                    None,
                    600,
                    false,
                )
                .is_ok()
        );
        assert!(
            service
                .coverage_comparison(
                    "regions",
                    None,
                    None,
                    None,
                    Some("missing-suite"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_err()
        );
        store.inject_query_fault();
        assert!(
            service
                .coverage_comparison(
                    "regions",
                    None,
                    None,
                    None,
                    Some("unit"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_err()
        );
        store.inject_query_fault_after(1);
        assert!(
            service
                .coverage_comparison(
                    "regions",
                    None,
                    None,
                    None,
                    Some("unit"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_err()
        );
        let bounded_response =
            json!({"context":{"repo_key":"repo"},"data":{"value":"payload"},"page":null});
        let serialized_size = serde_json::to_vec(&bounded_response).unwrap().len();
        assert!(
            service
                .apply_byte_budget(bounded_response.clone(), serialized_size - 1)
                .is_err()
        );
        assert!(
            service
                .apply_byte_budget(bounded_response, serialized_size)
                .is_ok()
        );
        let current_id = current["id"].as_str().expect("current id");
        let no_baseline = service
            .coverage_review(
                "change",
                Some(baseline["id"].as_str().unwrap()),
                None,
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                3,
                120,
                600,
                12_000,
                "review",
            )
            .expect("no-baseline review");
        assert_eq!(no_baseline["data"]["change"]["status"], "no_baseline");
        store.inject_query_fault();
        assert!(
            service
                .review_change(
                    Some(current_id),
                    Some("none"),
                    None,
                    None,
                    2,
                    10,
                    false,
                    3,
                    120,
                    false
                )
                .is_err()
        );
        assert!(
            service
                .source(current_id, "src/a.py", 1, 1, None, 600)
                .is_ok()
        );
        assert!(service.snapshot_summary(current_id, 49, false).is_err());
        assert!(
            service
                .source_ranges(current_id, "src/a.py", vec![(1, 1)], None, 600,)
                .is_ok()
        );
        assert!(
            service
                .source_ranges(current_id, "src/a.py", Vec::new(), None, 600)
                .is_err()
        );
        assert!(
            service
                .source_ranges(current_id, "src/a.py", vec![(1, 1)], Some("invalid"), 600)
                .is_err()
        );
        assert!(
            service
                .source_review(current_id, Vec::new(), 120, 600, 12_000)
                .is_err()
        );
        assert!(
            service
                .source_review(
                    current_id,
                    vec![("src/a.py".to_owned(), 1, 1)],
                    120,
                    49,
                    12_000
                )
                .is_err()
        );
        assert!(
            service
                .source_review(
                    current_id,
                    vec![("src/a.py".to_owned(), 1, 1)],
                    120,
                    600,
                    999
                )
                .is_err()
        );
        let valid_progress = &mut json!({
            "worktree":{"id":"worktree","path":"/tmp/worktree","branch":"main"},
            "points":[]
        });
        update_worktree_progress(valid_progress, vec![], true).expect("detailed progress");
        update_worktree_progress(valid_progress, vec![], false).expect("compact progress");
        let worktree = service
            .ensure_lineage_baseline(directory.path().to_str().unwrap(), "main", None)
            .expect("worktree registration");
        assert!(
            service
                .ensure_lineage_baseline("/missing-worktree", "main", None)
                .is_err()
        );
        assert!(
            service
                .ensure_lineage_baseline(outside.path().to_str().unwrap(), "main", None)
                .is_err()
        );
        assert!(
            service
                .coverage_comparison(
                    "overview",
                    None,
                    None,
                    Some(worktree["data"]["id"].as_str().unwrap()),
                    Some("unit"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_ok()
        );
        store.inject_query_fault();
        assert!(
            service
                .coverage_comparison(
                    "overview",
                    None,
                    None,
                    Some(worktree["data"]["id"].as_str().unwrap()),
                    Some("unit"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_err()
        );
        assert!(
            service
                .coverage_comparison(
                    "lines",
                    Some(current_id),
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    None,
                    None,
                    true,
                    None,
                    600,
                    false,
                )
                .is_ok()
        );
        let worktree_review = service
            .coverage_review(
                "change",
                Some(current_id),
                None,
                Some(worktree["data"]["id"].as_str().unwrap()),
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                0,
                120,
                600,
                12_000,
                "review",
            )
            .expect("worktree review");
        assert_eq!(worktree_review["data"]["change"]["status"], "measured");
        for view in ["overview", "files", "lines", "regions"] {
            assert!(
                service
                    .coverage_comparison(
                        view,
                        Some(current_id),
                        Some(baseline["id"].as_str().unwrap()),
                        None,
                        Some("unit"),
                        Some("src/a.py"),
                        view == "lines",
                        None,
                        600,
                        view == "overview",
                    )
                    .is_ok()
            );
        }
        assert!(
            service
                .coverage_comparison(
                    "progress",
                    None,
                    None,
                    Some(worktree["data"]["id"].as_str().unwrap()),
                    Some("unit"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_ok()
        );
        assert!(
            service
                .coverage_comparison(
                    "progress", None, None, None, None, None, false, None, 600, false,
                )
                .is_err()
        );
        assert!(
            service
                .coverage_comparison(
                    "invalid",
                    Some(current_id),
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    Some("unit"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_err()
        );
        assert!(
            service
                .coverage_comparison(
                    "overview",
                    Some(current_id),
                    None,
                    None,
                    Some("unit"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_err()
        );
        let bounded_source_change = service
            .coverage_review(
                "change",
                Some(many_current["id"].as_str().unwrap()),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                Some("main"),
                Some("src/a.py"),
                2,
                10,
                10,
                20,
                true,
                0,
                10,
                600,
                12_000,
                "review",
            )
            .expect("bounded source change");
        assert_eq!(
            bounded_source_change["data"]["change"]["source"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let missing_commit = service
            .review_changed_code(
                current_id,
                &json!({"repo_path":directory.path().to_string_lossy()}),
                &current,
                10,
            )
            .expect("missing baseline commit");
        assert_eq!(missing_commit["status"], "unavailable");
        let invalid_git = service
            .review_changed_code(
                current_id,
                &json!({
                    "repo_path":directory.path().to_string_lossy(),
                    "commit_sha":"invalid-baseline"
                }),
                &json!({
                    "repo_path":directory.path().to_string_lossy(),
                    "commit_sha":"invalid-current"
                }),
                10,
            )
            .expect("invalid git comparison");
        assert_eq!(invalid_git["status"], "unavailable");
        let invalid_snapshot = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some("invalid-current"),
                None,
                "unit",
            )
            .expect("invalid snapshot");
        let no_commit_snapshot = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .expect("snapshot without commit");
        let no_commit_id = no_commit_snapshot["id"].as_str().unwrap();
        store
            .clear_snapshot_commit_for_test(no_commit_id)
            .expect("clear snapshot commit");
        assert!(
            service
                .source(no_commit_id, "src/a.py", 1, 1, None, 600)
                .is_err()
        );
        assert!(
            service
                .source_ranges(no_commit_id, "src/a.py", vec![(1, 1)], None, 600)
                .is_err()
        );
        assert!(
            service
                .source_review(
                    no_commit_id,
                    vec![("src/a.py".to_owned(), 1, 1)],
                    120,
                    600,
                    12_000
                )
                .is_err()
        );
        let unavailable_review = service
            .coverage_review(
                "change",
                Some(invalid_snapshot["id"].as_str().unwrap()),
                Some(current_id),
                None,
                Some("unit"),
                Some("main"),
                Some("src/a.py"),
                2,
                10,
                10,
                10,
                false,
                0,
                120,
                600,
                12_000,
                "review",
            )
            .expect("unavailable changed-code review");
        assert_eq!(
            unavailable_review["data"]["change"]["changed_code"]["status"],
            "unavailable"
        );
        let mut classified = BTreeMap::new();
        service
            .classify_changed_range(
                current_id,
                &ChangedLineRange {
                    file_path: "src/a.py".to_owned(),
                    start: 1,
                    line_count: 4,
                },
                &mut classified,
            )
            .expect("classify changed range");
        assert!(classified["src/a.py"].contains_key("covered"));
        assert!(classified["src/a.py"].contains_key("uncovered"));
        assert!(classified["src/a.py"].contains_key("unmeasured"));
        assert!(classified["src/a.py"].contains_key("non_executable"));
        assert!(classified["src/a.py"].contains_key("branch_gap"));
        let insight = service
            .coverage_review(
                "insight",
                Some(current_id),
                None,
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                3,
                120,
                600,
                12_000,
                "review",
            )
            .expect("insight review");
        assert_eq!(insight["data"]["insight"]["status"], "measured");
        assert!(insight["data"]["insight"]["items"].is_array());
        let history = service
            .coverage_review(
                "history",
                None,
                None,
                None,
                Some("unit"),
                Some("main"),
                Some("src/a.py"),
                2,
                10,
                10,
                10,
                false,
                3,
                120,
                600,
                12_000,
                "review",
            )
            .expect("history review");
        assert!(history["data"]["history"]["detail"].is_array());
        let source_review = service
            .source_review(
                current_id,
                vec![("src/a.py".to_owned(), 1, 1)],
                10,
                600,
                12_000,
            )
            .expect("source review");
        assert!(source_review["data"]["source"].is_array());
        let change = service
            .coverage_review(
                "change",
                Some(current_id),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                Some("main"),
                Some("src/a.py"),
                2,
                10,
                10,
                10,
                true,
                0,
                120,
                1200,
                20_000,
                "compact",
            )
            .expect("change review");
        assert!(
            !store
                .changed_regions(
                    current_id,
                    baseline["id"].as_str().unwrap(),
                    None,
                    false,
                    10,
                )
                .expect("changed regions")
                .is_empty()
        );
        assert!(
            !change["data"]["change"]["source"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(change["data"]["claim_status"].is_string());
        assert!(change["data"]["change"]["changed_code"]["files"].is_array());
        let audit = service
            .coverage_review(
                "change",
                Some(current_id),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                0,
                120,
                1200,
                20_000,
                "audit",
            )
            .expect("audit review");
        assert_eq!(audit["data"]["change"]["representation"], "audit");
        let review = |focus: &str,
                      detail_snapshots: usize,
                      summary_window: usize,
                      max_files: usize,
                      max_regions: usize,
                      context_lines: usize,
                      max_source_lines: usize,
                      max_words: usize,
                      max_bytes: usize,
                      representation: &str| {
            service.coverage_review(
                focus,
                Some(current_id),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                Some("main"),
                None,
                detail_snapshots,
                summary_window,
                max_files,
                max_regions,
                false,
                context_lines,
                max_source_lines,
                max_words,
                max_bytes,
                representation,
            )
        };
        assert!(review("invalid", 2, 10, 10, 10, 3, 120, 600, 12_000, "review").is_err());
        assert!(review("change", 0, 10, 10, 10, 3, 120, 600, 12_000, "review").is_err());
        assert!(review("change", 2, 1, 10, 10, 3, 120, 600, 12_000, "review").is_err());
        assert!(review("change", 2, 10, 0, 10, 3, 120, 600, 12_000, "review").is_err());
        assert!(review("change", 2, 10, 10, 0, 3, 120, 600, 12_000, "review").is_err());
        assert!(review("change", 2, 10, 10, 10, 21, 120, 600, 12_000, "review").is_err());
        assert!(review("change", 2, 10, 10, 10, 3, 9, 600, 12_000, "review").is_err());
        assert!(review("change", 2, 10, 10, 10, 3, 120, 49, 12_000, "review").is_err());
        assert!(review("change", 2, 10, 10, 10, 3, 120, 600, 999, "review").is_err());
        assert!(review("change", 2, 10, 10, 10, 3, 120, 600, 12_000, "invalid").is_err());
        assert!(
            service
                .coverage_import(
                    "coverage.lcov",
                    "lcov",
                    "unit",
                    None,
                    None,
                    None,
                    49,
                    12_000
                )
                .is_err()
        );
        let long_suite = "suite-word ".repeat(60);
        assert!(
            service
                .coverage_import(
                    "coverage.lcov",
                    "lcov",
                    &long_suite,
                    None,
                    None,
                    None,
                    50,
                    12_000,
                )
                .is_err()
        );
        assert!(
            service
                .coverage_comparison(
                    "overview",
                    Some(current_id),
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    Some("unit"),
                    None,
                    false,
                    None,
                    49,
                    false,
                )
                .is_err()
        );
        assert!(
            service
                .run_review("missing", "logs", None, "both", 3, 5, false, 600, 999)
                .is_err()
        );
        assert!(
            service
                .run_review(
                    "missing", "status", None, "invalid", 3, 5, false, 600, 12_000
                )
                .is_err()
        );
        assert!(
            service
                .run_review("missing", "status", None, "both", 21, 5, false, 600, 12_000)
                .is_err()
        );
        assert!(
            service
                .run_review("missing", "status", None, "both", 3, 0, false, 600, 12_000)
                .is_err()
        );
        assert!(
            service
                .run_review("missing", "invalid", None, "both", 3, 5, false, 600, 12_000)
                .is_err()
        );
        assert!(
            service
                .run_review("missing", "logs", None, "both", 3, 5, false, 600, 12_000)
                .is_err()
        );
        assert!(
            service
                .run_review("missing", "status", None, "both", 3, 5, false, 49, 12_000)
                .is_err()
        );
        assert!(
            service
                .run_review(
                    "missing",
                    "logs",
                    Some(vec!["term".to_owned()]),
                    "both",
                    3,
                    5,
                    false,
                    600,
                    12_000,
                )
                .is_err()
        );
        assert!(
            service
                .source_review(current_id, Vec::new(), 120, 600, 12_000)
                .is_err()
        );
        assert!(
            service
                .source_review(
                    current_id,
                    (0..11).map(|_| ("src/a.py".to_owned(), 1, 1)).collect(),
                    120,
                    600,
                    12_000,
                )
                .is_err()
        );
        let (annotated, red_regions) = annotate_source_lines(
            vec![
                json!({"line_number":1}),
                json!({"line_number":2}),
                json!({"line_number":3}),
                json!({"line_number":4}),
                json!({"line_number":5}),
                json!("not-a-line"),
                json!({}),
            ],
            Some(&vec![
                json!({"line_number":1,"count_line":true,"covered":false}),
                json!({"line_number":2,"count_line":true,"covered":true,"total_branches":2,"covered_branches":1}),
                json!({"line_number":3,"count_line":true,"covered":true}),
                json!({"line_number":4,"count_line":false}),
            ]),
        );
        assert_eq!(annotated[0]["status"], "uncovered");
        assert_eq!(annotated[1]["status"], "branch_gap");
        assert_eq!(annotated[2]["status"], "covered");
        assert_eq!(annotated[3]["status"], "non_executable");
        assert_eq!(annotated[4]["status"], "unmeasured");
        assert_eq!(red_regions.len(), 1);
        assert_eq!(line_regions(&[5, 4, 3, 1, 2, 8]).len(), 2);
        assert_eq!(
            history_metric(
                &[json!({"line_rate":0.5}), json!({"line_rate":0.8})],
                "line_rate"
            )["trend"],
            "regressing"
        );
        assert_eq!(
            history_metric(
                &[json!({"line_rate":0.5}), json!({"line_rate":0.5})],
                "line_rate"
            )["trend"],
            "unchanged"
        );
        assert!(
            service
                .source_review(
                    current_id,
                    vec![("../bad".to_owned(), 1, 1)],
                    120,
                    600,
                    12_000
                )
                .is_err()
        );
        assert!(
            service
                .source_review(
                    current_id,
                    vec![("src/a.py".to_owned(), 1, 11)],
                    10,
                    600,
                    12_000
                )
                .is_err()
        );
        assert!(
            service
                .source_review(
                    current_id,
                    vec![("src/a.py".to_owned(), 1, 1)],
                    9,
                    600,
                    12_000
                )
                .is_err()
        );
        assert!(
            service
                .source_review(
                    current_id,
                    vec![("src/a.py".to_owned(), 2, 1)],
                    120,
                    600,
                    12_000
                )
                .is_err()
        );
        assert!(
            service
                .source_review(
                    current_id,
                    vec![("src/a.py".to_owned(), 1, 25)],
                    120,
                    50,
                    12_000,
                )
                .is_err()
        );
        assert!(
            service
                .run_review("missing", "status", None, "both", 3, 5, false, 600, 12_000)
                .is_err()
        );
        assert!(
            service
                .coverage_import(
                    "../missing.lcov",
                    "auto",
                    "unit",
                    None,
                    None,
                    None,
                    600,
                    12_000
                )
                .is_err()
        );
        assert!(
            service
                .ingest("coverage.lcov", "lcov", " ", None, None, None, false)
                .is_err()
        );
        let registered = service
            .command_registration(
                "service-fault-command",
                "true",
                true,
                "tester",
                "fault matrix",
                Some(directory.path().to_str().unwrap()),
                "/bin/sh",
                None,
                false,
            )
            .expect("fault matrix command");
        assert!(service.run_state("missing", "unknown", false).is_err());
        let terminal = service
            .run_submission_with_options(
                registered["data"]["id"].as_str().unwrap(),
                None,
                None,
                true,
                false,
                false,
            )
            .expect("terminal service run");
        assert!(
            service
                .run_state(terminal["data"]["id"].as_str().unwrap(), "status", false)
                .is_ok()
        );
        assert!(
            service
                .run_review(
                    terminal["data"]["id"].as_str().unwrap(),
                    "status",
                    None,
                    "both",
                    3,
                    5,
                    false,
                    600,
                    1_000,
                )
                .is_err()
        );
        let noisy_command = service
            .command_registration(
                "service-noisy-command",
                "i=0; while [ $i -lt 100 ]; do echo xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx; i=$((i+1)); done",
                true,
                "tester",
                "large log budget",
                Some(directory.path().to_str().unwrap()),
                "/bin/sh",
                None,
                false,
            )
            .expect("noisy command");
        let noisy_run = service
            .run_submission_with_options(
                noisy_command["data"]["id"].as_str().unwrap(),
                None,
                None,
                true,
                false,
                false,
            )
            .expect("noisy run");
        assert!(
            service
                .run_review(
                    noisy_run["data"]["id"].as_str().unwrap(),
                    "logs",
                    Some(vec!["x".to_owned()]),
                    "stdout",
                    0,
                    50,
                    false,
                    5_000,
                    1_000,
                )
                .is_err()
        );
        assert!(
            service
                .run_review(
                    noisy_run["data"]["id"].as_str().unwrap(),
                    "logs",
                    Some(vec!["x".to_owned()]),
                    "stdout",
                    0,
                    50,
                    false,
                    50,
                    12_000,
                )
                .is_err()
        );
        let active_command = service
            .command_registration(
                "service-active-command",
                "sleep 2",
                true,
                "tester",
                "active run matrix",
                Some(directory.path().to_str().unwrap()),
                "/bin/sh",
                None,
                false,
            )
            .expect("active command");
        let active_run = service
            .run_submission_with_options(
                active_command["data"]["id"].as_str().unwrap(),
                None,
                None,
                false,
                false,
                false,
            )
            .expect("active service run");
        let active_context = service
            .project_context(None, 600, false)
            .expect("active project context");
        assert!(
            !active_context["data"]["active_runs"]
                .as_array()
                .expect("active runs")
                .is_empty()
        );
        let _ = service.run_state(active_run["data"]["id"].as_str().unwrap(), "cancel", false);
        assert!(service.ensure_lineage_baseline("\0", "main", None).is_err());
        assert!(
            service
                .coverage_comparison(
                    "regions",
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    None,
                    Some("unit"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_err()
        );
        let measured_history = service
            .coverage_review(
                "history",
                Some(current["id"].as_str().unwrap()),
                None,
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                3,
                120,
                1_200,
                20_000,
                "review",
            )
            .expect("measured history review");
        assert_eq!(measured_history["data"]["history"]["status"], "measured");
        let measured_insight = service
            .coverage_review(
                "insight",
                Some(current["id"].as_str().unwrap()),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                3,
                120,
                1_200,
                20_000,
                "review",
            )
            .expect("measured insight review");
        assert_eq!(measured_insight["data"]["insight"]["status"], "measured");
        let measured_change = service
            .coverage_review(
                "change",
                Some(current["id"].as_str().unwrap()),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                true,
                3,
                120,
                1_200,
                20_000,
                "review",
            )
            .expect("measured change review");
        assert_eq!(measured_change["data"]["change"]["status"], "measured");
        macro_rules! service_fault {
            ($skip:expr, $expression:expr) => {{
                service.store().inject_query_fault_after($skip);
                let _ = $expression
                    .err()
                    .expect("injected service query fault should surface");
            }};
        }
        service_fault!(1, service.project_context(None, 600, false));
        service_fault!(2, service.project_context(None, 600, false));
        service_fault!(3, service.project_context(None, 600, false));
        service_fault!(4, service.project_context(None, 600, false));
        service_fault!(
            1,
            service.command_registration(
                "service-fault-second",
                "true",
                true,
                "tester",
                "fault matrix",
                Some(directory.path().to_str().unwrap()),
                "/bin/sh",
                None,
                false,
            )
        );
        service_fault!(
            1,
            service.run_submission_with_options(
                registered["data"]["id"].as_str().unwrap(),
                None,
                None,
                false,
                false,
                false,
            )
        );
        service_fault!(0, service.run_state("missing", "status", false));
        service_fault!(
            0,
            service.run_review(
                "missing",
                "logs",
                Some(vec!["term".to_owned()]),
                "both",
                3,
                5,
                false,
                600,
                12_000
            )
        );
        service_fault!(
            0,
            service.ingest("coverage.lcov", "lcov", "unit", None, None, None, false)
        );
        service_fault!(
            0,
            service.coverage_import(
                "coverage.lcov",
                "lcov",
                "unit",
                None,
                None,
                None,
                600,
                12_000
            )
        );
        service_fault!(
            1,
            service.coverage_comparison(
                "regions",
                Some(current_id),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                None,
                false,
                None,
                600,
                false,
            )
        );
        service_fault!(
            0,
            service.coverage_comparison(
                "progress",
                None,
                None,
                Some(worktree["data"]["id"].as_str().unwrap()),
                Some("unit"),
                None,
                false,
                None,
                600,
                false,
            )
        );
        service_fault!(
            0,
            service.coverage_review(
                "history",
                None,
                None,
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                3,
                120,
                600,
                12_000,
                "review",
            )
        );
        service_fault!(
            1,
            service.coverage_review(
                "insight",
                Some(current_id),
                None,
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                3,
                120,
                600,
                12_000,
                "review",
            )
        );
        service_fault!(
            1,
            service.coverage_comparison(
                "overview",
                Some(current_id),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                None,
                false,
                None,
                600,
                false,
            )
        );
        service_fault!(
            1,
            service.coverage_review(
                "change",
                Some(current_id),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                10,
                10,
                10,
                false,
                3,
                120,
                600,
                12_000,
                "review",
            )
        );
        service_fault!(1, service.source(current_id, "src/a.py", 1, 1, None, 600));
        service_fault!(
            1,
            service.source_ranges(current_id, "src/a.py", vec![(1, 1)], None, 600)
        );
        service_fault!(
            1,
            service.source_review(
                current_id,
                vec![("src/a.py".to_owned(), 1, 1)],
                120,
                600,
                12_000
            )
        );
        service_fault!(
            1,
            service.file_detail(current_id, "src/a.py", None, 600, false)
        );
        service_fault!(
            1,
            service.update_project_settings(ProjectSettingsPatch::default())
        );
        service_fault!(1, service.compact_now());

        macro_rules! service_query_sweep {
            ($expression:expr) => {{
                for skip in 0..=40 {
                    service.store().inject_query_fault_after(skip);
                    let _ = $expression;
                }
            }};
        }
        service_query_sweep!(service.project_context(None, 600, false));
        service_query_sweep!(service.coverage_comparison(
            "regions",
            Some(current_id),
            Some(baseline["id"].as_str().unwrap()),
            None,
            Some("unit"),
            None,
            false,
            None,
            600,
            false,
        ));
        service_query_sweep!(service.coverage_comparison(
            "overview",
            Some(current_id),
            Some(baseline["id"].as_str().unwrap()),
            Some(worktree["data"]["id"].as_str().unwrap()),
            Some("unit"),
            None,
            false,
            None,
            600,
            false,
        ));
        service_query_sweep!(service.coverage_review(
            "change",
            Some(current_id),
            Some(baseline["id"].as_str().unwrap()),
            None,
            Some("unit"),
            Some("main"),
            None,
            2,
            10,
            10,
            10,
            true,
            0,
            120,
            1200,
            20_000,
            "review",
        ));
        service_query_sweep!(service.coverage_review(
            "change",
            Some(current_id),
            None,
            Some(worktree["data"]["id"].as_str().unwrap()),
            Some("unit"),
            Some("main"),
            None,
            2,
            10,
            10,
            10,
            false,
            3,
            120,
            600,
            12_000,
            "review",
        ));
        service_query_sweep!(service.coverage_review(
            "change",
            Some(current_id),
            None,
            None,
            Some("unit"),
            Some("main"),
            None,
            2,
            10,
            10,
            10,
            false,
            3,
            120,
            600,
            12_000,
            "review",
        ));
        service_query_sweep!(service.coverage_review(
            "history",
            Some(current_id),
            None,
            None,
            Some("unit"),
            Some("main"),
            None,
            2,
            10,
            10,
            10,
            false,
            3,
            120,
            600,
            12_000,
            "review",
        ));
        service_query_sweep!(service.coverage_review(
            "insight",
            Some(current_id),
            Some(baseline["id"].as_str().unwrap()),
            None,
            Some("unit"),
            Some("main"),
            None,
            2,
            10,
            10,
            10,
            false,
            3,
            120,
            600,
            12_000,
            "review",
        ));
        service_query_sweep!(service.source(current_id, "src/a.py", 1, 2, None, 600));
        service_query_sweep!(service.source_ranges(
            current_id,
            "src/a.py",
            vec![(1, 1), (2, 2)],
            None,
            600
        ));
        service_query_sweep!(service.source_review(
            current_id,
            vec![("src/a.py".to_owned(), 1, 2)],
            120,
            600,
            12_000
        ));
        service_query_sweep!(service.file_detail(current_id, "src/a.py", None, 600, false));
        service_query_sweep!(service.snapshot_summary(current_id, 600, false));
        service_query_sweep!(service.run_review(
            terminal["data"]["id"].as_str().unwrap(),
            "status",
            None,
            "both",
            3,
            5,
            false,
            600,
            12_000
        ));

        let closed_service = service.clone();
        store.close().expect("close store");
        assert!(closed_service.project_context(None, 600, false).is_err());
        assert!(
            closed_service
                .command_registration(
                    "closed-command",
                    "true",
                    true,
                    "tester",
                    "closed store",
                    Some(directory.path().to_str().unwrap()),
                    "/bin/sh",
                    None,
                    false,
                )
                .is_err()
        );
        assert!(
            closed_service
                .run_submission_with_options("missing", None, None, true, false, false)
                .is_err()
        );
        assert!(
            closed_service
                .run_submission_with_options("missing", None, None, false, false, false)
                .is_err()
        );
        assert!(
            closed_service
                .run_state("missing", "status", false)
                .is_err()
        );
        assert!(
            closed_service
                .run_state("missing", "cancel", false)
                .is_err()
        );
        assert!(
            closed_service
                .run_review("missing", "status", None, "both", 3, 5, false, 600, 12_000)
                .is_err()
        );
        assert!(
            closed_service
                .search_logs("missing", vec!["term".to_owned()], "both", 3, 5, 600, false)
                .is_err()
        );
        assert!(
            closed_service
                .ingest("coverage.lcov", "lcov", "unit", None, None, None, false)
                .is_err()
        );
        assert!(
            closed_service
                .coverage_import(
                    "coverage.lcov",
                    "lcov",
                    "unit",
                    None,
                    None,
                    None,
                    600,
                    12_000
                )
                .is_err()
        );
        assert!(
            closed_service
                .ensure_lineage_baseline(directory.path().to_str().unwrap(), "main", None)
                .is_err()
        );
        assert!(
            closed_service
                .coverage_comparison(
                    "progress",
                    None,
                    None,
                    Some("missing"),
                    Some("unit"),
                    None,
                    false,
                    None,
                    600,
                    false,
                )
                .is_err()
        );
        assert!(
            closed_service
                .coverage_review(
                    "change",
                    None,
                    None,
                    None,
                    Some("unit"),
                    Some("main"),
                    None,
                    2,
                    10,
                    10,
                    10,
                    false,
                    3,
                    120,
                    600,
                    12_000,
                    "review",
                )
                .is_err()
        );
        assert!(
            closed_service
                .snapshot_summary("missing", 600, false)
                .is_err()
        );
        assert!(
            closed_service
                .source("missing", "src/a.py", 1, 1, None, 600)
                .is_err()
        );
        assert!(
            closed_service
                .source_ranges("missing", "src/a.py", vec![(1, 1)], None, 600)
                .is_err()
        );
        assert!(
            closed_service
                .source_review(
                    "missing",
                    vec![("src/a.py".to_owned(), 1, 1)],
                    120,
                    600,
                    12_000
                )
                .is_err()
        );
        assert!(
            closed_service
                .file_detail("missing", "src/a.py", None, 600, false)
                .is_err()
        );
        assert!(
            closed_service
                .update_project_settings(ProjectSettingsPatch::default())
                .is_err()
        );
        assert!(closed_service.compact_now().is_err());
    }

    #[test]
    fn compact_projections_keep_public_keys_and_hide_detail_by_default() {
        let project = compact_project(
            &json!({"id":"project-id","snapshot_count": 2, "compaction": {"enabled": true}}),
        );
        assert_eq!(project["id"], "project-id");
        assert_eq!(project["snapshot_count"], 2);
        assert!(project["line_rate"].is_null());
        assert!(project["compaction"].is_object());

        let snapshot = json!({"id":"id","repo_path":"/repo","warnings":null,"metadata":null,"report_path":"report"});
        let compact = compact_snapshot(&snapshot, false);
        assert_eq!(compact["measurement_checkout_path"], "/repo");
        assert_eq!(compact["warnings"], json!([]));
        assert!(compact.get("report_path").is_none());
        let detailed = compact_snapshot(&snapshot, true);
        assert_eq!(detailed["report_path"], "report");
        assert_eq!(detailed["metadata"], json!({}));

        let file = compact_file(&json!({"file_path":"a.py","raw_metrics":null}), false);
        assert!(file.get("raw_metrics").is_none());
        let detailed_file = compact_file(&json!({"raw_metrics":null}), true);
        assert_eq!(detailed_file["raw_metrics"], json!({}));
        let changed_regions = compact_changed_regions(&[
            json!({"file_path":"a.py","status":"regressed","start":4,"end":5,"line_count":2}),
            json!({"file_path":"a.py","status":"improved","start":1,"end":2,"line_count":2}),
            json!({"file_path":"a.py","status":"changed","start":9,"end":10}),
            json!({}),
            json!({"file_path":"a.py"}),
            json!({"file_path":"a.py","status":"changed"}),
            json!({"file_path":"a.py","status":"changed","start":1}),
        ]);
        assert_eq!(changed_regions[0]["path"], "a.py");
        assert!(changed_regions[0]["regressed"].is_array());
        let mut review_change = json!({
            "changed_code": {"status":"measured","files":[
                {"path":"a.py","covered":[[1,1,1]],"uncovered":[[2,2,1]],"branch_gap":[[3,3,1]]}
            ]},
            "files":[{"file_path":"a.py","baseline_total_lines":10,"current_total_lines":12,"line_rate_delta":0.1}],
            "regions":[{"path":"a.py","regressed":[[4,4,1]],"improved":[[5,5,1]]}]
        });
        compact_review_change(&mut review_change);
        assert_eq!(review_change["representation"], "compact");
        assert!(review_change["changed_code"]["legend"].is_object());
        assert_eq!(review_change["changed_code"]["files"][0]["p"], "a.py");
        assert_eq!(review_change["files"][0]["p"], "a.py");
        assert_eq!(review_change["files"][0]["l"][0], 10);
        assert!(review_change["file_legend"].is_object());
        assert_eq!(review_change["regions"][0]["r"][0][2], "!");
        let mut empty_change = json!({});
        compact_review_change(&mut empty_change);
        let mut scalar_change = json!("scalar");
        compact_review_change(&mut scalar_change);
        let mut missing_files = json!({"changed_code":{}});
        compact_review_change(&mut missing_files);
        let mut malformed_files = json!({
            "changed_code":{"files":[
                null,
                {"path":"a.py","covered":[[],[null,1],[1]]},
                {},
                {"path":7}
            ]}
        });
        compact_review_change(&mut malformed_files);
        assert_eq!(malformed_files["changed_code"]["files"][0]["p"], "a.py");
        assert_eq!(compact_region_groups(&json!("scalar")), json!([]));
        let malformed_region_groups = compact_region_groups(&json!([
            {},
            [],
            {"path":"a.py"},
            {"path":"a.py","x":1},
            {"path":"a.py","regressed":["not-a-range",[],[null,1],[1]]}
        ]));
        assert_eq!(malformed_region_groups.as_array().unwrap().len(), 3);
        assert!(
            malformed_region_groups
                .as_array()
                .unwrap()
                .iter()
                .all(|value| value["r"].as_array().unwrap().is_empty())
        );
        expand_review_change(&mut review_change);
        assert_eq!(review_change["representation"], "audit");
        assert!(review_change["audit"].is_object());
        assert_eq!(
            review_next_action(
                &json!({"status":"measured","files":[{"uncovered":[[1,1,1]]}]}),
                &[]
            )["kind"],
            "add_tests"
        );
        assert_eq!(
            review_next_action(
                &json!({"status":"measured","files":[]}),
                &[json!({"status":"regressed"})]
            )["kind"],
            "inspect_regression"
        );
        assert_eq!(
            review_next_action(&json!({"status":"measured","files":[]}), &[])["kind"],
            "review_existing_gaps"
        );
        assert_eq!(
            review_next_action(&json!({"status":"no_source_changes"}), &[])["kind"],
            "review_existing_gaps"
        );
        assert_eq!(
            review_next_action(&json!({"status":"no_baseline"}), &[])["kind"],
            "establish_baseline"
        );
        assert_eq!(
            review_next_action(&json!({"status":"not_measured"}), &[])["kind"],
            "obtain_measurement"
        );

        let command = compact_command(&json!({"artifact_specs":null}), false);
        assert_eq!(command["artifact_specs"], json!([]));
        let detailed_command = compact_command(&json!({"approved_by":"human"}), true);
        assert_eq!(detailed_command["approved_by"], "human");

        assert_eq!(
            compact_file_change(&json!({"file_path":"a.py","line_rate_delta":0.1}))["file_path"],
            "a.py"
        );
        assert!(compact_file_change_token(&json!({"line_rate_delta":0.1})).is_none());
        assert_eq!(
            compact_history_snapshot(&json!({"id":"s","line_rate":0.8}))["id"],
            "s"
        );
        let history_points = vec![
            json!({"line_rate":0.8,"branch_rate":0.7,"function_rate":0.9,"region_rate":0.6}),
            json!({"line_rate":0.7,"branch_rate":0.8,"function_rate":0.9,"region_rate":0.6}),
            json!({"line_rate":0.7}),
            json!({"branch_rate":0.8}),
        ];
        assert_eq!(
            history_metric(&history_points, "line_rate")["trend"],
            "improving"
        );
        assert_eq!(summarize_history(&history_points)["regression_runs"], 0);
        assert_eq!(summarize_history(&history_points)["improvement_runs"], 1);

        let run = json!({"parsed_summary":{},"artifact_paths":[],"status":"passed"});
        assert_eq!(compact_run_result(&run, true), run);
        let compact_run = compact_run_result(&run, false);
        assert!(compact_run.get("parsed_summary").is_none());
        assert_eq!(compact_run["status"], "passed");
        assert_eq!(compact_run_result(&json!("scalar"), false), json!("scalar"));
        let mut log_scalar = json!("scalar");
        strip_log_metadata(&mut log_scalar);
        assert_eq!(log_scalar, json!("scalar"));
        assert!(canonical_json(&json!({"b": [2, 1], "a": true})).starts_with("{\"a\""));
        assert_eq!(canonical_json(&json!(null)), "null");

        let mut scalar_progress = json!("scalar");
        assert!(update_worktree_progress(&mut scalar_progress, Vec::new(), false).is_err());
        let mut detailed_progress = json!({"worktree":{"id":"w","path":"/repo","branch":"main"}});
        update_worktree_progress(&mut detailed_progress, Vec::new(), true).expect("progress");
        assert!(detailed_progress["points"].is_array());

        let source = vec![
            json!({"line_number":1,"text":"missed"}),
            json!({"line_number":2,"text":"branch"}),
            json!({"line_number":3,"text":"covered"}),
            json!({"line_number":4,"text":"non-executable"}),
            json!({"line_number":5,"text":"missing measurement"}),
            json!({"line_number":6,"text":"unmeasured"}),
            json!({"line_number":7,"text":"missing branch fields"}),
            json!({"text":"missing number"}),
            json!("no line number"),
        ];
        let coverage = vec![
            json!({"line_number":1,"count_line":true,"covered":false,"total_branches":0,"covered_branches":0}),
            json!({"line_number":2,"count_line":true,"covered":true,"total_branches":2,"covered_branches":1}),
            json!({"line_number":3,"count_line":true,"covered":true}),
            json!({"line_number":4,"count_line":false,"covered":false}),
            json!({"line_number":5,"count_line":true}),
            json!({"line_number":7,"count_line":true,"covered":true,"total_branches":2}),
            json!({"total_branches":2,"covered_branches":1}),
        ];
        let (annotated, red_regions) = annotate_source_lines(source, Some(&coverage));
        assert_eq!(annotated[0]["marker"], "red");
        assert_eq!(annotated[1]["marker"], "yellow");
        assert_eq!(annotated[2]["marker"], "green");
        assert_eq!(annotated[3]["marker"], "gray");
        assert_eq!(annotated[4]["status"], "unmeasured");
        assert_eq!(annotated[5]["status"], "unmeasured");
        assert_eq!(red_regions[0]["line_count"], 1);
        let (_, no_coverage_regions) = annotate_source_lines(vec![json!({"line_number":8})], None);
        assert!(no_coverage_regions.is_empty());
        assert_eq!(line_regions(&[5, 1, 2, 2, 4])[0]["start"], 1);
        assert_eq!(line_regions(&[5, 1, 2, 2, 4])[1]["start"], 4);
        assert!(line_regions(&[]).is_empty());
    }
}
