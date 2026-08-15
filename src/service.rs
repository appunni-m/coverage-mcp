//! Transport-neutral orchestration and response projection.
//!
//! Keeping this layer independent of Hyper and the MCP wire format makes the
//! REST, dashboard, and agent-facing interfaces share the same validation,
//! pagination, response budgets, and compact projections.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};
use crate::git::inspect_git;
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
            let anchor = required_page_anchor(&selected)?;
            let occurrence = values[..consumed]
                .iter()
                .filter(|value| cursor_anchor(value) == anchor)
                .count();
            Some(encode_cursor(&anchor, scope, occurrence)?)
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
        let value = if wait {
            self.store
                .run_command(command_ref, timeout_seconds, idempotency_key, 20)?
        } else {
            self.store
                .submit_command(command_ref, timeout_seconds, idempotency_key, 20)?
        };
        Ok(self.envelope(compact_run_result(&value, detailed), None, None))
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
        Ok(self.envelope(compact_run_result(&value, detailed), None, None))
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
        if snapshot.get("repo_key").and_then(Value::as_str) != Some(context.repo_key.as_str()) {
            return Err(AppError::Validation(
                "coverage report does not belong to the selected repository".to_owned(),
            ));
        }
        Ok(self.envelope(compact_snapshot(&snapshot, detailed), Some(suite), None))
    }

    /// Registers a Git worktree against the selected repository.
    pub fn worktree_registration(
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
                .register_worktree(Path::new(&git.repo_path), base_ref.trim(), name)?;
        let compact = json!({"id": result["id"], "name": result["name"], "created_at": result["created_at"], "path": result["path"], "branch": result["branch"], "head_sha": result["head_sha"], "base_ref": result["base_ref"], "base_sha": result["base_sha"], "baseline_snapshot_id": result["baseline_snapshot_id"]});
        Ok(self.envelope(compact, None, None))
    }

    /// Executes a coverage query view.
    #[allow(clippy::too_many_arguments)]
    pub fn coverage_query(
        &self,
        view: &str,
        snapshot_id: Option<&str>,
        baseline_snapshot_id: Option<&str>,
        suite: Option<&str>,
        branch: Option<&str>,
        file_path: Option<&str>,
        line_number: Option<i64>,
        line_ranges: Option<Vec<LineRange>>,
        cursor: Option<&str>,
        max_words: usize,
        detailed: bool,
    ) -> AppResult<Value> {
        self.coverage_query_ordered(
            view,
            snapshot_id,
            baseline_snapshot_id,
            suite,
            branch,
            file_path,
            line_number,
            line_ranges,
            None,
            cursor,
            max_words,
            detailed,
        )
    }

    /// Executes a coverage query with an optional target ordering.
    #[allow(clippy::too_many_arguments)]
    pub fn coverage_query_ordered(
        &self,
        view: &str,
        snapshot_id: Option<&str>,
        baseline_snapshot_id: Option<&str>,
        suite: Option<&str>,
        branch: Option<&str>,
        file_path: Option<&str>,
        line_number: Option<i64>,
        line_ranges: Option<Vec<LineRange>>,
        order_by: Option<&str>,
        cursor: Option<&str>,
        max_words: usize,
        detailed: bool,
    ) -> AppResult<Value> {
        validate_max_words(max_words)?;
        let context = self.context(suite);
        let mut selected_snapshot = snapshot_id.map(|id| self.store.snapshot(id)).transpose()?;
        let mut selected_id = snapshot_id.map(str::to_owned);
        if selected_id.is_none() && view != "line_history" {
            let snapshot = self
                .store
                .latest_snapshot(Some(&context.checkout_path), branch, suite)?
                .ok_or_else(|| AppError::NotFound("no snapshots found".to_owned()))?;
            selected_id = Some(required_string_field(&snapshot, "id", "latest snapshot")?);
            selected_snapshot = Some(snapshot);
        }
        let selected_suite = match suite {
            Some(value) => Some(value.to_owned()),
            None => selected_snapshot
                .as_ref()
                .map(|value| required_string_field(value, "suite", "snapshot"))
                .transpose()?,
        };
        match view {
            "summary" => {
                let snapshot = required_snapshot(selected_snapshot.as_ref(), "summary")?;
                Ok(self.envelope(
                    compact_snapshot(snapshot, detailed),
                    selected_suite.as_deref(),
                    None,
                ))
            }
            "files" => {
                let id = required_snapshot_id(selected_id.as_deref(), "files")?;
                let values = self
                    .store
                    .files(id, COLLECTION_FETCH_LIMIT)?
                    .into_iter()
                    .map(|value| compact_file(&value, detailed))
                    .collect::<Vec<_>>();
                let (values, page) = self.page(
                    &values,
                    cursor,
                    max_words,
                    &format!("coverage-files:{id}:{detailed}"),
                    None,
                )?;
                Ok(self.envelope(Value::Array(values), selected_suite.as_deref(), Some(page)))
            }
            "targets" => {
                let id = required_snapshot_id(selected_id.as_deref(), "targets")?;
                let snapshot = required_snapshot(selected_snapshot.as_ref(), "targets")?;
                let order_by = order_by.unwrap_or("priority");
                let mut values = self.store.targets(id, order_by, COLLECTION_FETCH_LIMIT)?;
                if let Some(file_path) = file_path {
                    values.retain(|value| {
                        value.get("file_path").and_then(Value::as_str) == Some(file_path)
                    });
                }
                let (values, page) = self.page(
                    &values,
                    cursor,
                    max_words,
                    &format!("coverage-targets:{id}:{order_by}:{file_path:?}"),
                    None,
                )?;
                Ok(self.envelope(
                    json!({
                        "snapshot": compact_snapshot(snapshot, detailed),
                        "order_by": order_by,
                        "targets": values,
                    }),
                    selected_suite.as_deref(),
                    Some(page),
                ))
            }
            "file" => {
                let id = required_snapshot_id(selected_id.as_deref(), "file")?;
                let file_path = file_path.ok_or_else(|| {
                    AppError::Validation(
                        "snapshot_id and file_path are required for file view".to_owned(),
                    )
                })?;
                let file = self.store.file_coverage(id, file_path)?;
                let selected =
                    self.store
                        .lines_in_ranges(id, file_path, &line_ranges.unwrap_or_default())?;
                let mut gaps = self.store.file_gaps(id, file_path, 100)?;
                let ranges = required_array_field(&gaps, "ranges", "file gaps")?.to_vec();
                let (ranges, page) = self.page(
                    &ranges,
                    cursor,
                    max_words,
                    &format!("coverage-file:{id}:{file_path}"),
                    None,
                )?;
                let mut red_regions = Vec::new();
                for value in &ranges {
                    let start = required_i64_field(value, "start", "file gap")?;
                    let end = required_i64_field(value, "end", "file gap")?;
                    let line_count = end - start + 1;
                    red_regions.push(json!({"start": start, "end": end, "line_count": line_count}));
                }
                insert_paged_gaps(&mut gaps, ranges);
                let selected_lines = required_value(&selected, "lines", "line selection")?.clone();
                let line_selection = selection_without_lines(&selected)?;
                Ok(self.envelope(json!({"file": compact_file(&file, detailed), "red_regions": red_regions, "gaps": gaps, "selected_lines": selected_lines, "line_selection": line_selection}), selected_suite.as_deref(), Some(page)))
            }
            "insights" => {
                let id = required_snapshot_id(selected_id.as_deref(), "insights")?;
                let result =
                    self.store
                        .insights(id, baseline_snapshot_id, COLLECTION_FETCH_LIMIT)?;
                let items = required_array_field(&result, "items", "insights")?.to_vec();
                let (items, page) = self.page(
                    &items,
                    cursor,
                    max_words,
                    &format!("coverage-insights:{id}:{baseline_snapshot_id:?}:{detailed}"),
                    None,
                )?;
                Ok(self.envelope(json!({"snapshot": compact_snapshot(&result["snapshot"], detailed), "baseline": if result["baseline"].is_null() { Value::Null } else { compact_snapshot(&result["baseline"], detailed) }, "summary": result["summary"], "items": items}), selected_suite.as_deref(), Some(page)))
            }
            "line_history" => {
                let file_path = file_path.ok_or_else(|| {
                    AppError::Validation(
                        "file_path, line_number, and suite are required for line_history view"
                            .to_owned(),
                    )
                })?;
                let line_number = line_number.ok_or_else(|| {
                    AppError::Validation(
                        "file_path, line_number, and suite are required for line_history view"
                            .to_owned(),
                    )
                })?;
                let suite = suite.ok_or_else(|| {
                    AppError::Validation(
                        "file_path, line_number, and suite are required for line_history view"
                            .to_owned(),
                    )
                })?;
                let values = self
                    .store
                    .line_history(
                        file_path,
                        line_number,
                        branch,
                        Some(suite),
                        COLLECTION_FETCH_LIMIT,
                    )?
                    .into_iter()
                    .map(|value| compact_history_point(&value, detailed))
                    .collect::<Vec<_>>();
                let (values, page) = self.page(
                    &values,
                    cursor,
                    max_words,
                    &format!(
                        "line-history:{}:{suite}:{branch:?}:{file_path}:{line_number}:{detailed}",
                        context.repo_key
                    ),
                    None,
                )?;
                Ok(self.envelope(Value::Array(values), Some(suite), Some(page)))
            }
            _ => Err(AppError::Validation(
                "view must be summary, files, targets, file, insights, or line_history".to_owned(),
            )),
        }
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
            let points = required_array_field(&progress, "points", "worktree progress")?.to_vec();
            let (points, page) = self.page(
                &points,
                cursor,
                max_words,
                &format!("worktree-progress:{worktree_id}:{suite}:{file_path:?}"),
                None,
            )?;
            update_worktree_progress(&mut progress, points, detailed)?;
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
                        .map(|value| required_string_field(&value, "id", "latest snapshot"))
                        .transpose()?
                        .ok_or_else(|| AppError::NotFound("no snapshots found".to_owned()))?
                };
                let baseline_id = if let Some(baseline_snapshot_id) = baseline_snapshot_id {
                    baseline_snapshot_id.to_owned()
                } else {
                    self.store
                        .previous_snapshot(&current_id)?
                        .map(|value| required_string_field(&value, "id", "previous snapshot"))
                        .transpose()?
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
        let current_suite = comparison_current_suite(&comparison)?;
        if suite.is_some_and(|value| value != current_suite) {
            return Err(AppError::Validation(
                "requested suite does not match the current snapshot".to_owned(),
            ));
        }
        let mut base = json!({"baseline": compact_snapshot(&comparison["baseline"], detailed), "current": compact_snapshot(&comparison["current"], detailed), "overall": comparison["overall"]});
        if view == "overview" {
            base["file_change_count"] = json!(comparison_file_change_count(&comparison)?);
            base["line_change_count"] =
                json!(required_array_field(&comparison, "changed_lines", "comparison")?.len());
            return Ok(self.envelope(base, Some(&current_suite), None));
        }
        let mut values = match view {
            "files" => required_array_field(&comparison, "files", "comparison")?.to_vec(),
            "lines" => required_array_field(&comparison, "changed_lines", "comparison")?
                .to_vec()
                .into_iter()
                .filter(|value| {
                    !only_regressions
                        || value.get("status").and_then(Value::as_str) == Some("regressed")
                })
                .collect(),
            "regions" => {
                let regions = required_array_field(&comparison, "regions", "comparison")?;
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
        let coverage_lines = required_array_field(&coverage, "lines", "line coverage")?;
        let (lines, red_regions) = annotate_source_lines(lines, Some(coverage_lines))?;
        let (lines, page) = self.page(
            &lines,
            cursor,
            max_words,
            &format!("source:{snapshot_id}:{file_path}:{start}:{end}"),
            None,
        )?;
        Ok(self.envelope(
            json!({"snapshot_commit_sha": required_value(&snapshot, "commit_sha", "snapshot")?, "file_path": file_path, "red_regions": red_regions, "lines": lines}),
            Some(required_string_field(&snapshot, "suite", "snapshot")?.as_str()),
            Some(page),
        ))
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
        Ok(self.envelope(
            serde_json::to_value(self.store.update_project_settings(patch)?)?,
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
        let worktree_id = required_value(&worktree, "id", "worktree")?.clone();
        let worktree_path = required_value(&worktree, "path", "worktree")?.clone();
        let worktree_branch = required_value(&worktree, "branch", "worktree")?.clone();
        object.insert(
            "worktree".to_owned(),
            json!({"id":worktree_id,"path":worktree_path,"branch":worktree_branch}),
        );
    }
    Ok(())
}

fn required_snapshot<'a>(snapshot: Option<&'a Value>, view: &str) -> AppResult<&'a Value> {
    snapshot.ok_or_else(|| AppError::NotFound(format!("no snapshot is available for {view}")))
}

fn required_snapshot_id<'a>(id: Option<&'a str>, view: &str) -> AppResult<&'a str> {
    id.ok_or_else(|| AppError::NotFound(format!("no snapshot is available for {view}")))
}

fn required_value<'a>(value: &'a Value, key: &str, context: &str) -> AppResult<&'a Value> {
    value
        .get(key)
        .ok_or_else(|| AppError::Runtime(format!("{context} is missing required field '{key}'")))
}

fn selection_without_lines(value: &Value) -> AppResult<Value> {
    let mut object = value
        .as_object()
        .cloned()
        .ok_or_else(|| AppError::Runtime("line selection must be an object".to_owned()))?;
    object.remove("lines");
    Ok(Value::Object(object))
}

fn comparison_file_change_count(comparison: &Value) -> AppResult<usize> {
    Ok(required_array_field(comparison, "files", "comparison")?.len())
}

fn comparison_current_suite(comparison: &Value) -> AppResult<String> {
    required_string_field(
        required_value(comparison, "current", "comparison")?,
        "suite",
        "current snapshot",
    )
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

fn required_array_field<'a>(
    value: &'a Value,
    key: &str,
    context: &str,
) -> AppResult<&'a Vec<Value>> {
    required_value(value, key, context)?
        .as_array()
        .ok_or_else(|| AppError::Runtime(format!("{context} field '{key}' must be an array")))
}

fn required_page_anchor(values: &[Value]) -> AppResult<String> {
    values
        .last()
        .map(cursor_anchor)
        .ok_or_else(|| AppError::Runtime("selected page unexpectedly empty".to_owned()))
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

fn compact_history_point(value: &Value, detailed: bool) -> Value {
    if detailed {
        return value.clone();
    }
    let omitted = ["suite", "file_path", "line_number"];
    let mut result = Map::new();
    if let Some(object) = value.as_object() {
        for (key, item) in object {
            if !omitted.contains(&key.as_str()) {
                result.insert(key.clone(), item.clone());
            }
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

fn insert_paged_gaps(gaps: &mut Value, ranges: Vec<Value>) {
    if let Some(object) = gaps.as_object_mut() {
        object.insert("ranges".to_owned(), Value::Array(ranges));
        object.insert(
            "returned_range_count".to_owned(),
            json!(object["ranges"].as_array().map_or(0, Vec::len)),
        );
    }
}

fn annotate_source_lines(
    source: Vec<Value>,
    coverage: Option<&Vec<Value>>,
) -> AppResult<(Vec<Value>, Vec<Value>)> {
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
    Ok((result, line_regions(&red_lines)?))
}

fn line_regions(numbers: &[i64]) -> AppResult<Vec<Value>> {
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
    Ok(regions)
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
        assert!(required_string_field(&json!({"id":""}), "id", "projection").is_err());
        assert!(required_i64_field(&json!({"count":"1"}), "count", "projection").is_err());
        assert!(required_array_field(&json!({"items":{}}), "items", "projection").is_err());
        assert!(selection_without_lines(&json!("scalar")).is_err());
        assert_eq!(
            selection_without_lines(&json!({"lines":[1],"selected":true})).unwrap(),
            json!({"selected":true})
        );
        assert!(comparison_file_change_count(&json!({})).is_err());
        assert_eq!(
            comparison_file_change_count(&json!({"files":[]})).unwrap(),
            0
        );
        assert!(comparison_current_suite(&json!({"current":{}})).is_err());
        assert_eq!(
            comparison_current_suite(&json!({"current":{"suite":"unit"}})).unwrap(),
            "unit"
        );
        let mut missing_worktree = json!({});
        assert!(update_worktree_progress(&mut missing_worktree, Vec::new(), false).is_err());

        let directory = tempfile::tempdir().expect("tempdir");
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
                Some(&"a".repeat(40)),
                None,
                "unit",
            )
            .expect("baseline");
        let current = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                Some(&"b".repeat(40)),
                None,
                "unit",
            )
            .expect("current");
        let service = CoverageService::new(
            store.clone(),
            RequestContext {
                repo_key: project.repo_key,
                checkout_path: project.repo_path,
                suite: None,
            },
        );
        assert!(service.source("missing", "a.py", 0, 1, None, 600).is_err());
        assert!(
            service
                .source("missing", "a.py", 1, 201, None, 600)
                .is_err()
        );
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
        store.close().expect("close store");
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

        let command = compact_command(&json!({"artifact_specs":null}), false);
        assert_eq!(command["artifact_specs"], json!([]));
        let detailed_command = compact_command(&json!({"approved_by":"human"}), true);
        assert_eq!(detailed_command["approved_by"], "human");

        let history = json!({"suite":"unit","file_path":"a.py","line_number":1,"hits":2});
        assert_eq!(compact_history_point(&history, true), history);
        let compact_history = compact_history_point(&history, false);
        assert!(compact_history.get("suite").is_none());
        assert_eq!(compact_history["hits"], 2);
        assert!(compact_history_point(&json!("not-an-object"), false).is_object());

        let run = json!({"parsed_summary":{},"artifact_paths":[],"status":"passed"});
        assert_eq!(compact_run_result(&run, true), run);
        let compact_run = compact_run_result(&run, false);
        assert!(compact_run.get("parsed_summary").is_none());
        assert_eq!(compact_run["status"], "passed");
        assert_eq!(compact_run_result(&json!("scalar"), false), json!("scalar"));
        let mut log_scalar = json!("scalar");
        strip_log_metadata(&mut log_scalar);
        assert_eq!(log_scalar, json!("scalar"));
        let mut scalar_gaps = json!("scalar");
        insert_paged_gaps(&mut scalar_gaps, Vec::new());
        assert_eq!(scalar_gaps, json!("scalar"));
        assert!(canonical_json(&json!({"b": [2, 1], "a": true})).starts_with("{\"a\""));
        assert_eq!(canonical_json(&json!(null)), "null");

        let mut scalar_progress = json!("scalar");
        assert!(update_worktree_progress(&mut scalar_progress, Vec::new(), false).is_err());
        let mut detailed_progress = json!({"worktree":{"id":"w","path":"/repo","branch":"main"}});
        update_worktree_progress(&mut detailed_progress, Vec::new(), true).expect("progress");
        assert!(detailed_progress["points"].is_array());
        assert!(required_snapshot(None, "summary").is_err());
        assert!(required_snapshot_id(None, "files").is_err());
        assert!(required_page_anchor(&[]).is_err());

        let source = vec![
            json!({"line_number":1,"text":"missed"}),
            json!({"line_number":2,"text":"branch"}),
            json!({"line_number":3,"text":"covered"}),
            json!({"line_number":4,"text":"non-executable"}),
            json!({"line_number":5,"text":"missing measurement"}),
            json!({"line_number":6,"text":"unmeasured"}),
            json!({"text":"missing number"}),
            json!("no line number"),
        ];
        let coverage = vec![
            json!({"line_number":1,"count_line":true,"covered":false,"total_branches":0,"covered_branches":0}),
            json!({"line_number":2,"count_line":true,"covered":true,"total_branches":2,"covered_branches":1}),
            json!({"line_number":3,"count_line":true,"covered":true}),
            json!({"line_number":4,"count_line":false,"covered":false}),
            json!({"line_number":5,"count_line":true}),
        ];
        let (annotated, red_regions) = annotate_source_lines(source, Some(&coverage)).unwrap();
        assert_eq!(annotated[0]["marker"], "red");
        assert_eq!(annotated[1]["marker"], "yellow");
        assert_eq!(annotated[2]["marker"], "green");
        assert_eq!(annotated[3]["marker"], "gray");
        assert_eq!(annotated[4]["status"], "unmeasured");
        assert_eq!(annotated[5]["status"], "unmeasured");
        assert_eq!(red_regions[0]["line_count"], 1);
        let (_, no_coverage_regions) =
            annotate_source_lines(vec![json!({"line_number":8})], None).unwrap();
        assert!(no_coverage_regions.is_empty());
        assert_eq!(line_regions(&[5, 1, 2, 2, 4]).unwrap()[0]["start"], 1);
        assert_eq!(line_regions(&[5, 1, 2, 2, 4]).unwrap()[1]["start"], 4);
        assert!(line_regions(&[]).unwrap().is_empty());
    }
}
