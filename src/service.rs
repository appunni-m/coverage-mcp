//! Transport-neutral orchestration and response projection.
//!
//! Keeping this layer independent of Hyper and the MCP wire format makes the
//! REST, dashboard, and agent-facing interfaces share the same validation,
//! pagination, response budgets, and compact projections.

// Service projections consume validated storage rows and use assertions for
// schema invariants. Boundary and storage failures remain typed AppErrors.
#![allow(clippy::expect_used, clippy::unwrap_in_result)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

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

const MAX_CONTEXT_ACTIVE_RUNS: usize = 10;
const MAX_CONTEXT_COMMANDS: usize = 8;
const MAX_COMPACT_INSIGHT_REGIONS: usize = 3;

#[cfg(test)]
static FORCE_BUDGET_FAILURE: AtomicBool = AtomicBool::new(false);

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

    /// Enforces a serialized-word budget on the complete `data` projection.
    ///
    /// The envelope's `context` and `page` metadata are excluded from this
    /// count. Callers that compose a paged collection with fixed summary data
    /// must reserve the fixed data before allocating words to the collection.
    /// [`Self::project_context`] exposes that accounting in its page metadata.
    pub fn apply_budget(&self, response: Value, max_words: usize) -> AppResult<Value> {
        validate_max_words(max_words)?;
        #[cfg(test)]
        if FORCE_BUDGET_FAILURE.swap(false, Ordering::SeqCst) {
            return Err(AppError::Validation(
                "injected response budget failure".to_owned(),
            ));
        }
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
    ///
    /// `page.word_count` is the serialized-word count of the selected
    /// collection items only. `page.max_words` is the budget supplied to this
    /// collection, while `page.truncated` and `page.next_cursor` describe how
    /// to continue the collection. A caller that embeds this page alongside
    /// fixed data must reserve that fixed data separately; `project_context`
    /// reports the combined accounting as `reserved_words` and
    /// `response_word_count`.
    pub fn page(
        &self,
        values: &[Value],
        cursor: Option<&str>,
        max_words: usize,
        scope: &str,
        total: Option<usize>,
    ) -> AppResult<(Vec<Value>, Value)> {
        self.page_with_item_limit(values, cursor, max_words, scope, total, None)
    }

    fn page_with_item_limit(
        &self,
        values: &[Value],
        cursor: Option<&str>,
        max_words: usize,
        scope: &str,
        total: Option<usize>,
        max_items: Option<usize>,
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
            if max_items.is_some_and(|limit| selected.len() >= limit) {
                break;
            }
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

    /// Returns a compact, budgeted project summary, command inventory, and run state.
    ///
    /// `max_words` applies to the complete `data` projection, not only the
    /// command collection. The fixed project/latest-run/active-run summary is
    /// reserved first, and the remaining budget is used for command pages.
    /// In the returned `page`, `reserved_words` is the fixed-summary count,
    /// `word_count` is the current command-page count, and
    /// `response_word_count` is their sum for the complete response budget.
    /// `page.next_cursor` continues only the command collection. Each page
    /// returns at most eight compact command summaries; the cursor continues
    /// the remaining inventory without inflating normal context responses.
    ///
    /// `data.active_runs` is capped at ten entries in both compact and detailed
    /// modes. `data.active_runs_truncated` is `true` when additional active runs
    /// were available, so callers must not interpret a complete-looking list as
    /// proof that no other active runs exist. `detailed=true` expands raw
    /// command, run, and provenance fields; it does not remove the active-run
    /// cap and may require a larger `max_words` value.
    ///
    /// # Errors
    ///
    /// Returns a validation error when the requested budget or cursor is
    /// invalid, when the fixed summary cannot fit, or when too little budget
    /// remains for a non-empty command collection. Storage failures are
    /// propagated unchanged.
    pub fn project_context(
        &self,
        cursor: Option<&str>,
        max_words: usize,
        detailed: bool,
    ) -> AppResult<Value> {
        validate_max_words(max_words)?;
        let context = self.context(None);
        let project = self.store.project_summary()?;
        let project = if detailed {
            project
        } else {
            compact_context_project(&project)
        };
        let commands = self
            .store
            .list_registered_commands(COLLECTION_FETCH_LIMIT)?
            .into_iter()
            .map(|value| compact_command(&value, detailed))
            .collect::<Vec<_>>();
        let latest = self
            .store
            .latest_run(None)?
            .map(|value| compact_run_result(&value, detailed));
        let active_values = self.store.list_run_queue(COLLECTION_FETCH_LIMIT)?;
        let active_runs_truncated = active_values.len() > MAX_CONTEXT_ACTIVE_RUNS;
        let active = active_values
            .into_iter()
            .take(MAX_CONTEXT_ACTIVE_RUNS)
            .map(|value| compact_run_result(&value, detailed))
            .collect::<Vec<_>>();
        let fixed_data = json!({
            "project": project,
            "commands": [],
            "latest_run": latest,
            "active_runs": active,
            "active_runs_truncated": active_runs_truncated,
        });
        let fixed_words = serialized_word_count(&fixed_data);
        if fixed_words > max_words {
            return Err(AppError::Validation(format!(
                "project context summary requires {fixed_words} words; increase max_words"
            )));
        }
        let command_budget = max_words - fixed_words;
        if !commands.is_empty() && command_budget < 50 {
            return Err(AppError::Validation(format!(
                "project context summary requires {fixed_words} words and leaves less than 50 words for commands; increase max_words"
            )));
        }
        let scope = format!("project-context:{}:{detailed}", context.repo_key);
        let (commands, mut page) = if commands.is_empty() {
            self.page_with_item_limit(&commands, cursor, max_words, &scope, None, None)?
        } else {
            self.page_with_item_limit(
                &commands,
                cursor,
                command_budget,
                &scope,
                None,
                Some(MAX_CONTEXT_COMMANDS),
            )?
        };
        page["response_word_count"] =
            json!(fixed_words + page["word_count"].as_u64().unwrap_or(0) as usize);
        page["reserved_words"] = json!(fixed_words);
        page["max_words"] = json!(max_words);
        let project = fixed_data
            .get("project")
            .cloned()
            .expect("project context fixed data always contains project");
        let latest = fixed_data
            .get("latest_run")
            .cloned()
            .expect("project context fixed data always contains latest_run");
        let active = fixed_data
            .get("active_runs")
            .cloned()
            .expect("project context fixed data always contains active_runs");
        let active_runs_truncated = fixed_data
            .get("active_runs_truncated")
            .cloned()
            .expect("project context fixed data always contains active_runs_truncated");
        let response = self.envelope(
            json!({
                "project": project,
                "commands": commands,
                "latest_run": latest,
                "active_runs": active,
                "active_runs_truncated": active_runs_truncated,
            }),
            None,
            Some(page),
        );
        self.apply_budget(response, max_words)
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
        self.run_submission_with_execution(
            command_ref,
            timeout_seconds,
            idempotency_key,
            wait,
            reuse_if_unchanged,
            detailed,
            None,
            None,
            None,
        )
    }

    /// Submits or waits for one registered command with a case-specific
    /// execution identity used to guard reuse.
    #[allow(clippy::too_many_arguments)]
    pub fn run_submission_with_execution(
        &self,
        command_ref: &str,
        timeout_seconds: Option<u64>,
        idempotency_key: Option<&str>,
        wait: bool,
        reuse_if_unchanged: bool,
        detailed: bool,
        execution: Option<&Value>,
        arguments: Option<&Value>,
        baseline_snapshot_id: Option<&str>,
    ) -> AppResult<Value> {
        let value = if wait {
            self.store.run_command_with_execution(
                command_ref,
                timeout_seconds,
                idempotency_key,
                20,
                reuse_if_unchanged,
                execution,
                arguments,
                baseline_snapshot_id,
            )?
        } else {
            self.store.submit_command_with_execution(
                command_ref,
                timeout_seconds,
                idempotency_key,
                20,
                reuse_if_unchanged,
                execution,
                arguments,
                baseline_snapshot_id,
            )?
        };
        let value = self.attach_automatic_incremental_review(value);
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
        let value = if action == "status" {
            self.attach_automatic_incremental_review(value)
        } else {
            value
        };
        Ok(self.envelope(compact_run_result(&value, detailed), None, None))
    }

    fn attach_automatic_incremental_review(&self, mut run: Value) -> Value {
        let Some(execution) = run.get("execution") else {
            return run;
        };
        if execution.get("mode").and_then(Value::as_str) != Some("incremental") {
            return run;
        }
        let baseline = execution.get("baseline").and_then(Value::as_object);
        let baseline_is_composite =
            baseline.is_some_and(|value| value.contains_key("composite_snapshot_id"));
        let baseline_snapshot_id = baseline.and_then(|value| {
            value
                .get("snapshot_id")
                .or_else(|| value.get("composite_snapshot_id"))
                .and_then(Value::as_str)
        });
        if let Some(composite_snapshot_id) =
            run.get("composite_snapshot_id").and_then(Value::as_str)
        {
            let Some(baseline_snapshot_id) = baseline_snapshot_id else {
                run["incremental_review"] = automatic_incremental_review_placeholder(
                    "not_measured",
                    None,
                    baseline_is_composite,
                    "incremental composite execution has no explicit composite baseline",
                );
                return run;
            };
            let terminal = run
                .get("terminal")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !terminal {
                run["incremental_review"] = automatic_incremental_review_placeholder(
                    "pending",
                    Some(baseline_snapshot_id),
                    true,
                    "incremental composite review waits for terminal coverage ingestion",
                );
                return run;
            }
            let review = self.composite_review(
                "incremental",
                composite_snapshot_id,
                Some(baseline_snapshot_id),
                None,
                5,
                DEFAULT_MAX_WORDS,
                12_000,
                "compact",
                10,
            );
            match review {
                Ok(review) => {
                    let mut data = review
                        .get("data")
                        .cloned()
                        .expect("composite review always returns a data projection");
                    let object = data
                        .as_object_mut()
                        .expect("composite review data is always an object");
                    object.insert("automatic".to_owned(), json!(true));
                    object.insert("status".to_owned(), json!("measured"));
                    object.insert(
                        "current_composite_snapshot_id".to_owned(),
                        json!(composite_snapshot_id),
                    );
                    run["incremental_review"] = data;
                }
                Err(error) => {
                    run["incremental_review"] = automatic_incremental_review_placeholder(
                        "not_measured",
                        Some(baseline_snapshot_id),
                        true,
                        &format!("incremental composite review was not measured: {error}"),
                    );
                }
            }
            return run;
        }
        let Some(baseline_snapshot_id) = baseline_snapshot_id else {
            run["incremental_review"] = automatic_incremental_review_placeholder(
                "not_measured",
                None,
                baseline_is_composite,
                "incremental execution has no explicit baseline snapshot",
            );
            return run;
        };
        let baseline = json!({
            "kind": "explicit",
            "snapshot_id": baseline_snapshot_id,
        });
        let terminal = run
            .get("terminal")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !terminal {
            run["incremental_review"] = automatic_incremental_review_placeholder(
                "pending",
                Some(baseline_snapshot_id),
                false,
                "incremental review waits for terminal coverage ingestion",
            );
            return run;
        }
        let snapshot_ids = run
            .get("coverage_ingest")
            .and_then(|value| value.get("snapshot_ids"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let Some(selected_current_snapshot_ids) = snapshot_ids
            .iter()
            .map(Value::as_str)
            .map(|value| value.map(str::to_owned))
            .collect::<Option<Vec<_>>>()
        else {
            run["incremental_review"] = automatic_incremental_review_placeholder(
                "not_measured",
                Some(baseline_snapshot_id),
                false,
                "incremental review received an invalid current snapshot identifier",
            );
            return run;
        };
        let Some(current_snapshot_id) = selected_current_snapshot_ids.first() else {
            run["incremental_review"] = automatic_incremental_review_placeholder(
                "not_measured",
                Some(baseline_snapshot_id),
                false,
                "incremental review requires at least one ingested current coverage snapshot",
            );
            return run;
        };
        let review = self.coverage_review_with_snapshot_ids(
            "incremental",
            Some(current_snapshot_id),
            Some(&selected_current_snapshot_ids),
            Some(baseline_snapshot_id),
            None,
            None,
            None,
            None,
            2,
            2,
            3,
            5,
            false,
            3,
            120,
            DEFAULT_MAX_WORDS,
            12_000,
            "compact",
            10,
        );
        match review {
            Ok(review) => {
                let mut data = review
                    .get("data")
                    .cloned()
                    .expect("coverage review always returns a data projection");
                let object = data
                    .as_object_mut()
                    .expect("coverage review data is always an object");
                object.insert("automatic".to_owned(), json!(true));
                object.insert("status".to_owned(), json!("measured"));
                object.insert("baseline_selector".to_owned(), baseline.clone());
                let incremental = object
                    .get_mut("incremental")
                    .expect("incremental review always contains incremental data");
                incremental["current_snapshot_id"] = json!(current_snapshot_id);
                incremental["current_snapshot_ids"] = json!(selected_current_snapshot_ids);
                run["incremental_review"] = data;
            }
            Err(error) => {
                run["incremental_review"] = automatic_incremental_review_placeholder(
                    "not_measured",
                    Some(baseline_snapshot_id),
                    false,
                    &format!("incremental review was not measured: {error}"),
                );
            }
        }
        run
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
        self.ingest_with_options(
            report_path,
            format,
            suite,
            branch,
            commit_sha,
            base_ref,
            detailed,
            None,
            "compactable",
        )
    }

    /// Parses and ingests a report with execution and retention metadata.
    #[allow(clippy::too_many_arguments)]
    pub fn ingest_with_options(
        &self,
        report_path: &str,
        format: &str,
        suite: &str,
        branch: Option<&str>,
        commit_sha: Option<&str>,
        base_ref: Option<&str>,
        detailed: bool,
        execution: Option<&Value>,
        detail_retention: &str,
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
        let snapshot = self.store.ingest_report_with_execution(
            &path,
            format,
            Some(Path::new(&context.checkout_path)),
            branch,
            commit_sha,
            base_ref,
            suite,
            execution,
            detail_retention,
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
        self.coverage_import_with_options(
            report_path,
            format,
            suite,
            branch,
            commit_sha,
            base_ref,
            max_words,
            max_bytes,
            None,
            "compactable",
        )
    }

    /// Imports an external report with case-specific execution context and
    /// an optional retained incremental-base policy.
    #[allow(clippy::too_many_arguments)]
    pub fn coverage_import_with_options(
        &self,
        report_path: &str,
        format: &str,
        suite: &str,
        branch: Option<&str>,
        commit_sha: Option<&str>,
        base_ref: Option<&str>,
        max_words: usize,
        max_bytes: usize,
        execution: Option<&Value>,
        detail_retention: &str,
    ) -> AppResult<Value> {
        validate_max_words(max_words)?;
        validate_review_byte_budget(max_bytes)?;
        validate_relative_report_path(report_path, &self.context(None).checkout_path)?;
        let response = self.ingest_with_options(
            report_path,
            format,
            suite,
            branch,
            commit_sha,
            base_ref,
            false,
            execution,
            detail_retention,
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
    ///
    /// Compact history points contain identity, lineage, and the four coverage
    /// rates; raw covered/total counters are available only for a non-compact
    /// representation. Compact insight items include at most three ranges per
    /// target as `[start, end, line_count]` arrays. `region_count` reports the
    /// full target count and `regions_truncated` reports whether those arrays
    /// were capped; audit representation retains the exact stored regions and
    /// omits those compact-only fields.
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
        self.coverage_review_with_max_test_ids(
            focus,
            snapshot_id,
            baseline_snapshot_id,
            worktree_id,
            suite,
            branch,
            file_path,
            detail_snapshots,
            summary_window,
            max_files,
            max_regions,
            include_source,
            context_lines,
            max_source_lines,
            max_words,
            max_bytes,
            representation,
            10,
        )
    }

    /// Reviews an immutable composite region snapshot without reparsing any
    /// child artifact. Incremental comparisons require an explicit composite
    /// baseline and validate repository, mapping, and inventory identity.
    #[allow(clippy::too_many_arguments)]
    pub fn composite_review(
        &self,
        focus: &str,
        composite_snapshot_id: &str,
        baseline_composite_snapshot_id: Option<&str>,
        file_path: Option<&str>,
        max_regions: usize,
        max_words: usize,
        max_bytes: usize,
        representation: &str,
        max_test_ids: usize,
    ) -> AppResult<Value> {
        if !matches!(
            focus,
            "change" | "history" | "insight" | "source" | "audit" | "all" | "incremental"
        ) {
            return Err(AppError::Validation(
                "composite task must be change, history, insight, source, audit, all, or incremental"
                    .to_owned(),
            ));
        }
        if !matches!(representation, "review" | "compact" | "audit") {
            return Err(AppError::Validation(
                "representation must be review, compact, or audit".to_owned(),
            ));
        }
        if !(1..=100).contains(&max_regions) {
            return Err(AppError::Validation(
                "max_regions must be between 1 and 100".to_owned(),
            ));
        }
        if !(1..=100).contains(&max_test_ids) {
            return Err(AppError::Validation(
                "max_test_ids must be between 1 and 100".to_owned(),
            ));
        }
        validate_max_words(max_words)?;
        validate_review_byte_budget(max_bytes)?;
        let current = self.store.composite_snapshot(composite_snapshot_id)?;
        self.validate_composite_context(&current)?;
        let current_summary = compact_composite_snapshot(&current, representation == "audit");
        let mut result = json!({
            "focus": focus,
            "task": focus,
            "representation": representation,
            "claim_status": if current["status"] == "complete" { "supported" } else { "limited" },
            "reasons": current["reasons"].clone(),
            "measurement": current_summary.clone(),
            "baseline": Value::Null,
            "composite": current_summary,
        });

        if focus == "incremental" {
            let baseline_id = baseline_composite_snapshot_id.ok_or_else(|| {
                AppError::Validation(
                    "incremental composite review requires an explicit composite baseline"
                        .to_owned(),
                )
            })?;
            let baseline = self.store.composite_snapshot(baseline_id)?;
            self.validate_composite_context(&baseline)?;
            ensure_compatible_composites(&current, &baseline)?;
            let current_regions = self.store.composite_regions_all(composite_snapshot_id)?;
            let baseline_regions = self.store.composite_regions_all(baseline_id)?;
            let incremental = composite_incremental_projection(
                &current,
                &baseline,
                current_regions,
                baseline_regions,
                file_path,
                max_regions,
                representation,
                self.store.composite_test_attribution(
                    baseline_id,
                    composite_snapshot_id,
                    max_test_ids,
                )?,
            );
            result["baseline"] = compact_composite_snapshot(&baseline, representation == "audit");
            result["incremental"] = incremental;
            result["claim_status"] = json!(if current["status"] == "complete"
                && baseline["status"] == "complete"
            {
                "supported"
            } else {
                "limited"
            });
        } else if matches!(focus, "insight" | "source" | "audit" | "all" | "change") {
            let all_regions = self
                .store
                .composite_regions(composite_snapshot_id, MAX_COLLECTION_RECORDS)?;
            let selected = all_regions
                .into_iter()
                .filter(|region| {
                    file_path
                        .is_none_or(|path| region.get("path").and_then(Value::as_str) == Some(path))
                })
                .filter(|region| {
                    focus != "insight"
                        || matches!(
                            region.get("state").and_then(Value::as_str),
                            Some(
                                "uncovered" | "unmeasured" | "missing_artifact" | "source_mismatch"
                            )
                        )
                })
                .collect::<Vec<_>>();
            let truncated = selected.len() > max_regions;
            let selected = selected.into_iter().take(max_regions).collect::<Vec<_>>();
            let regions = if representation == "compact" {
                grouped_composite_regions(&selected)
            } else {
                selected
            };
            result["regions"] = Value::Array(regions);
            result["regions_truncated"] = json!(truncated);
            result["detail_source"] = json!("composite_relational");
        }
        let response = self.apply_budget(self.envelope(result, None, None), max_words)?;
        self.apply_byte_budget(response, max_bytes)
    }

    fn validate_composite_context(&self, snapshot: &Value) -> AppResult<()> {
        let repo_key = snapshot
            .get("repo_key")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AppError::Runtime("composite snapshot is missing repo_key".to_owned())
            })?;
        if repo_key != self.context.repo_key {
            return Err(AppError::Validation(
                "composite snapshot does not belong to the selected repository".to_owned(),
            ));
        }
        Ok(())
    }

    /// Returns a bounded review with an explicit maximum number of affected
    /// named test IDs in incremental attribution.
    #[allow(clippy::too_many_arguments)]
    pub fn coverage_review_with_max_test_ids(
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
        max_test_ids: usize,
    ) -> AppResult<Value> {
        let current_snapshot_ids = snapshot_id.map(|id| vec![id.to_owned()]);
        self.coverage_review_with_snapshot_ids(
            focus,
            snapshot_id,
            current_snapshot_ids.as_deref(),
            baseline_snapshot_id,
            worktree_id,
            suite,
            branch,
            file_path,
            detail_snapshots,
            summary_window,
            max_files,
            max_regions,
            include_source,
            context_lines,
            max_source_lines,
            max_words,
            max_bytes,
            representation,
            max_test_ids,
        )
    }

    /// Returns a bounded review for one or more ordinary snapshots selected
    /// by a managed run. Multiple artifacts are merged by canonical coverage
    /// identity before the incremental result is returned.
    #[allow(clippy::too_many_arguments)]
    pub fn coverage_review_with_snapshot_ids(
        &self,
        focus: &str,
        snapshot_id: Option<&str>,
        current_snapshot_ids: Option<&[String]>,
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
        max_test_ids: usize,
    ) -> AppResult<Value> {
        if !matches!(
            focus,
            "change" | "history" | "insight" | "all" | "incremental"
        ) {
            return Err(AppError::Validation(
                "focus must be change, history, insight, all, or incremental".to_owned(),
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
        if !(1..=100).contains(&max_test_ids) {
            return Err(AppError::Validation(
                "max_test_ids must be between 1 and 100".to_owned(),
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
        let current = if let Some(snapshot_ids) = current_snapshot_ids {
            let snapshot_id = snapshot_ids.first().ok_or_else(|| {
                AppError::Validation(
                    "selected snapshot set must contain at least one snapshot".to_owned(),
                )
            })?;
            Some(self.store.snapshot(snapshot_id)?)
        } else if let Some(snapshot_id) = snapshot_id {
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

        if focus == "incremental" {
            let baseline_id = baseline_snapshot_id.ok_or_else(|| {
                AppError::Validation(
                    "incremental review requires an explicit baseline snapshot".to_owned(),
                )
            })?;
            let Some(current_id) = current_id.as_deref() else {
                result["incremental"] = json!({
                    "status": "not_measured",
                    "comparison_mode": "additive_union",
                    "measurement_scope": Value::Null,
                    "run": Value::Null,
                    "current_snapshot_id": Value::Null,
                    "current_snapshot_ids": [],
                    "merge": Value::Null,
                    "metric_deltas": {},
                    "files": [],
                    "changed_lines": [],
                    "changed_lines_truncated": false,
                    "regions": [],
                    "regions_truncated": false,
                    "diff": Value::Null,
                    "coverage_gain": {
                        "newly_covered": 0,
                        "regressed": 0,
                        "hit_count_only": 0,
                        "added": 0,
                        "removed": 0,
                        "branch_state_changed": 0
                    },
                    "test_attribution": {"status":"unavailable","affected_test_count":0,"affected_tests":[],"tests_truncated":false,"max_test_ids":max_test_ids},
                    "detail_source": Value::Null,
                    "next_action": {"kind":"obtain_measurement","reason":"no compatible coverage snapshot is available"},
                });
                let response = self.apply_budget(
                    self.envelope(result, selected_suite.as_deref(), None),
                    max_words,
                )?;
                return self.apply_byte_budget(response, max_bytes);
            };
            let scope = incremental_measurement_scope(
                current
                    .as_ref()
                    .expect("current snapshot is present when an incremental review is measured"),
            );
            let selected_current_snapshot_ids = current_snapshot_ids
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| vec![current_id.to_owned()]);
            let selected_current_snapshot_refs = selected_current_snapshot_ids
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let mut incremental = self.store.incremental_union_compare_many_with_scope(
                &selected_current_snapshot_refs,
                baseline_id,
                file_path,
                max_files,
                max_regions,
                max_test_ids,
                scope,
            )?;
            let baseline = incremental["baseline"].clone();
            let current = incremental["current"].clone();
            let run = incremental["run"].clone();
            incremental["status"] = json!("measured");
            result["measurement"] = compact_snapshot(&current, representation == "audit");
            result["baseline"] = compact_snapshot(&baseline, representation == "audit");
            result["claim_status"] = json!("supported");
            if incremental["test_attribution"]["status"] == "unavailable" {
                result["reasons"] =
                    json!(["test attribution unavailable; union measurement remains measured"]);
            }
            if scope == "selected_subset" && result["reasons"].as_array().is_some_and(Vec::is_empty)
            {
                result["reasons"] = json!([
                    "selected subset: missing baseline hits are not regressions",
                    "diagnostic diff is scope-limited"
                ]);
            }
            incremental["run"] = compact_snapshot(&run, representation == "audit");
            let object = incremental
                .as_object_mut()
                .expect("incremental comparison projection is an object");
            object.remove("baseline");
            object.remove("current");
            object.remove("overall");
            if representation == "compact" {
                incremental["files"] = Value::Array(
                    incremental["files"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(compact_file_change)
                        .collect(),
                );
                let regions = incremental["regions"].clone();
                incremental["regions"] =
                    compact_changed_regions(regions.as_array().map_or(&[][..], Vec::as_slice));
                compact_incremental_diff(&mut incremental["diff"]);
                incremental["line_legend"] = json!({
                    "newly_covered": "+",
                    "regressed": "!",
                    "added": "added",
                    "removed": "removed",
                    "hit_count_only": "hits",
                    "branch_state_changed": "branch",
                });
                remove_incremental_changed_lines(&mut incremental);
            }
            incremental["next_action"] = incremental_next_action(&incremental["coverage_gain"]);
            result["incremental"] = incremental;
            let response = self.apply_budget(
                self.envelope(result, selected_suite.as_deref(), None),
                max_words,
            )?;
            return self.apply_byte_budget(response, max_bytes);
        }

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
                representation != "compact",
            )?;
            if focus == "history" && result["history"]["status"] == "measured" {
                result["claim_status"] = json!("supported");
            }
        }

        if matches!(focus, "insight" | "all") {
            result["insight"] = self.review_insight(
                current_id.as_deref(),
                baseline_snapshot_id,
                max_regions,
                representation == "compact",
                representation == "audit",
            )?;
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

    /// Returns the bounded, single-request projection used by the live
    /// dashboard.
    ///
    /// The dashboard intentionally uses a rolling previous snapshot when no
    /// baseline is supplied. That convenience belongs to this presentation
    /// projection only; the public incremental MCP task still requires an
    /// explicit baseline and never infers one.
    pub fn dashboard(
        &self,
        snapshot_id: Option<&str>,
        baseline_snapshot_id: Option<&str>,
        history_limit: usize,
    ) -> AppResult<Value> {
        self.dashboard_with_snapshot_ids(
            None,
            snapshot_id,
            baseline_snapshot_id,
            None,
            None,
            history_limit,
        )
    }

    /// Returns the bounded dashboard projection for ordinary and composite
    /// coverage layers.
    ///
    /// Ordinary snapshot selection remains compatible with the original
    /// dashboard API. Composite selection is independent because a managed
    /// run can produce both child snapshots and one immutable combined region
    /// snapshot. When composite IDs are omitted, the newest composite and its
    /// rolling predecessor are used for presentation only; agent-facing
    /// incremental tasks still require an explicit baseline.
    pub fn dashboard_with_composite(
        &self,
        snapshot_id: Option<&str>,
        baseline_snapshot_id: Option<&str>,
        composite_snapshot_id: Option<&str>,
        baseline_composite_snapshot_id: Option<&str>,
        history_limit: usize,
    ) -> AppResult<Value> {
        self.dashboard_with_snapshot_ids(
            None,
            snapshot_id,
            baseline_snapshot_id,
            composite_snapshot_id,
            baseline_composite_snapshot_id,
            history_limit,
        )
    }

    /// Returns the bounded dashboard projection for one or more ordinary
    /// snapshots and the independent composite layer.
    ///
    /// `current_snapshot_ids` is the additive measurement set. Its first ID
    /// remains the compatibility `current` snapshot used by existing callers;
    /// all IDs are merged into the incremental union when a baseline exists.
    #[allow(clippy::too_many_arguments)]
    pub fn dashboard_with_snapshot_ids(
        &self,
        current_snapshot_ids: Option<&[String]>,
        snapshot_id: Option<&str>,
        baseline_snapshot_id: Option<&str>,
        composite_snapshot_id: Option<&str>,
        baseline_composite_snapshot_id: Option<&str>,
        history_limit: usize,
    ) -> AppResult<Value> {
        if !(2..=50).contains(&history_limit) {
            return Err(AppError::Validation(
                "history_limit must be between 2 and 50".to_owned(),
            ));
        }
        if current_snapshot_ids.is_some() && snapshot_id.is_some() {
            return Err(AppError::Validation(
                "ordinary snapshot IDs must be supplied either as a set or as snapshot_id"
                    .to_owned(),
            ));
        }
        let selected_snapshot_ids = current_snapshot_ids
            .map(|values| {
                if values.is_empty() {
                    return Err(AppError::Validation(
                        "current snapshot set must contain at least one snapshot".to_owned(),
                    ));
                }
                Ok(values.to_vec())
            })
            .transpose()?
            .or_else(|| snapshot_id.map(|value| vec![value.to_owned()]));
        let project = self.store.project_summary()?;
        let compaction_policy = project.get("compaction").cloned().unwrap_or(Value::Null);
        let raw_snapshots =
            self.store
                .list_snapshots(None, None, None, history_limit.saturating_add(1))?;
        let snapshots_truncated = raw_snapshots.len() > history_limit;
        let snapshots = raw_snapshots
            .iter()
            .take(history_limit)
            .cloned()
            .collect::<Vec<_>>();
        let selected_snapshot_id = selected_snapshot_ids
            .as_ref()
            .and_then(|values| values.first())
            .map(String::as_str);
        let current = if let Some(snapshot_id) = selected_snapshot_id {
            let snapshot = self.store.snapshot(snapshot_id)?;
            if snapshot.get("repo_key").and_then(Value::as_str)
                != Some(self.context.repo_key.as_str())
            {
                return Err(AppError::Validation(
                    "snapshot does not belong to the selected repository".to_owned(),
                ));
            }
            Some(snapshot)
        } else {
            snapshots.first().cloned()
        };
        if let Some(snapshot_ids) = selected_snapshot_ids.as_deref() {
            for snapshot_id in snapshot_ids.iter().skip(1) {
                let snapshot = self.store.snapshot(snapshot_id)?;
                if snapshot.get("repo_key").and_then(Value::as_str)
                    != Some(self.context.repo_key.as_str())
                {
                    return Err(AppError::Validation(
                        "snapshot does not belong to the selected repository".to_owned(),
                    ));
                }
            }
        }
        let initial_current_id = current.as_ref().and_then(|value| value["id"].as_str());
        let (baseline, baseline_kind) = if let Some(snapshot_id) = baseline_snapshot_id {
            let snapshot = self.store.snapshot(snapshot_id)?;
            if snapshot.get("repo_key").and_then(Value::as_str)
                != Some(self.context.repo_key.as_str())
            {
                return Err(AppError::Validation(
                    "baseline snapshot does not belong to the selected repository".to_owned(),
                ));
            }
            (Some(snapshot), "explicit")
        } else if let Some(current_id) = initial_current_id {
            let rolling = snapshots
                .iter()
                .find(|value| value.get("id").and_then(Value::as_str) != Some(current_id))
                .cloned()
                .or(self.store.previous_snapshot(current_id)?)
                .filter(|value| value.get("id").and_then(Value::as_str) != Some(current_id));
            (rolling, "rolling_previous")
        } else {
            (None, "none")
        };
        let current_id = current.as_ref().and_then(|value| value["id"].as_str());
        let baseline_id = baseline.as_ref().and_then(|value| value["id"].as_str());
        let measurement_snapshot_ids = selected_snapshot_ids.unwrap_or_else(|| {
            current_id
                .map(|value| vec![value.to_owned()])
                .unwrap_or_default()
        });
        let raw_incremental = match (current_id, baseline_id) {
            (Some(_current_id), Some(baseline_id)) => {
                let scope = incremental_measurement_scope(
                    current
                        .as_ref()
                        .expect("dashboard current snapshot is present when IDs are selected"),
                );
                let snapshot_refs = measurement_snapshot_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                match self.store.incremental_union_compare_many_with_scope(
                    &snapshot_refs,
                    baseline_id,
                    None,
                    24,
                    120,
                    8,
                    scope,
                ) {
                    Ok(value) => Some(value),
                    Err(AppError::Validation(message)) => Some(json!({
                        "status": "unmeasured",
                        "reason": message,
                        "baseline": baseline.as_ref().map(|value| compact_snapshot(value, false)),
                        "current": current.as_ref().map(|value| compact_snapshot(value, false)),
                    })),
                    Err(error) => return Err(error),
                }
            }
            _ => None,
        };
        let dashboard_current = raw_incremental
            .as_ref()
            .filter(|value| value.get("status").and_then(Value::as_str) == Some("measured"))
            .and_then(|value| value.get("current"))
            .or(current.as_ref());
        let files = if let Some(current_id) = current_id {
            if let Some(incremental) = raw_incremental
                .as_ref()
                .filter(|value| value.get("status").and_then(Value::as_str) != Some("unmeasured"))
            {
                dashboard_incremental_files(incremental)
            } else {
                dashboard_current_files(&self.store.files(current_id, MAX_COLLECTION_RECORDS)?)
            }
        } else {
            json!({"items": [], "total": 0, "truncated": false})
        };
        let latest_run = self
            .store
            .latest_run(None)?
            .map(|value| compact_run_result(&value, false));
        let active_runs = self
            .store
            .list_run_queue(11)?
            .into_iter()
            .map(|value| compact_run_result(&value, false))
            .collect::<Vec<_>>();
        let active_runs_truncated = active_runs.len() > 10;
        let active_runs = active_runs.into_iter().take(10).collect::<Vec<_>>();
        let history = snapshots
            .iter()
            .rev()
            .map(dashboard_history_point)
            .collect::<Vec<_>>();
        let incremental = raw_incremental
            .as_ref()
            .map(dashboard_incremental)
            .unwrap_or_else(|| {
                json!({
                    "status": "unmeasured",
                    "reason": "select a current snapshot and a baseline to calculate progression"
                })
            });
        let raw_composite_history = self
            .store
            .list_composite_snapshots(history_limit.saturating_add(1))?;
        let composite_history_truncated = raw_composite_history.len() > history_limit;
        let composite_history = raw_composite_history
            .iter()
            .take(history_limit)
            .cloned()
            .collect::<Vec<_>>();
        let composite_current = if let Some(composite_snapshot_id) = composite_snapshot_id {
            let snapshot = self.store.composite_snapshot(composite_snapshot_id)?;
            self.validate_composite_context(&snapshot)?;
            Some(snapshot)
        } else {
            composite_history.first().cloned()
        };
        let initial_composite_current_id = composite_current
            .as_ref()
            .and_then(|value| value.get("id").and_then(Value::as_str));
        let (composite_baseline, composite_baseline_kind) =
            if let Some(baseline_composite_snapshot_id) = baseline_composite_snapshot_id {
                let snapshot = self
                    .store
                    .composite_snapshot(baseline_composite_snapshot_id)?;
                self.validate_composite_context(&snapshot)?;
                (Some(snapshot), "explicit")
            } else if let Some(current_id) = initial_composite_current_id {
                let rolling = composite_history
                    .iter()
                    .find(|value| value.get("id").and_then(Value::as_str) != Some(current_id))
                    .cloned()
                    .or(self.store.previous_composite_snapshot(current_id)?)
                    .filter(|value| value.get("id").and_then(Value::as_str) != Some(current_id));
                (rolling, "rolling_previous")
            } else {
                (None, "none")
            };
        let composite_current_id = composite_current
            .as_ref()
            .and_then(|value| value.get("id").and_then(Value::as_str));
        let composite_baseline_id = composite_baseline
            .as_ref()
            .and_then(|value| value.get("id").and_then(Value::as_str));
        let raw_composite_incremental = match (composite_current_id, composite_baseline_id) {
            (Some(current_id), Some(baseline_id)) => {
                let review = self.composite_review(
                    "incremental",
                    current_id,
                    Some(baseline_id),
                    None,
                    24,
                    DEFAULT_MAX_WORDS,
                    12_000,
                    "compact",
                    10,
                );
                match review {
                    Ok(value) => value
                        .get("data")
                        .and_then(|data| data.get("incremental"))
                        .cloned(),
                    Err(AppError::Validation(message)) => Some(json!({
                        "status": "unmeasured",
                        "reason": message,
                        "baseline": composite_baseline.as_ref().map(|value| compact_composite_snapshot(value, false)),
                        "current": composite_current.as_ref().map(|value| compact_composite_snapshot(value, false)),
                    })),
                    Err(error) => return Err(error),
                }
            }
            _ => None,
        };
        let composite_incremental = raw_composite_incremental
            .as_ref()
            .map(dashboard_composite_incremental)
            .unwrap_or_else(|| {
                json!({
                    "status": "unmeasured",
                    "reason": "select a current composite snapshot and baseline to calculate production region progression"
                })
            });
        let composite_history_points = composite_history
            .iter()
            .rev()
            .map(dashboard_composite_history_point)
            .collect::<Vec<_>>();
        let data = json!({
            "project": project,
            "selection": {
                "snapshot_id": current_id,
                "current_snapshot_ids": measurement_snapshot_ids,
                "baseline_snapshot_id": baseline_id,
                "baseline_kind": baseline_kind,
                "composite_snapshot_id": composite_current_id,
                "baseline_composite_snapshot_id": composite_baseline_id,
                "composite_baseline_kind": composite_baseline_kind,
            },
            "current": dashboard_current.map(|value| compact_snapshot(value, false)),
            "baseline": baseline.as_ref().map(|value| compact_snapshot(value, false)),
            "history": {
                "points": history,
                "returned": snapshots.len(),
                "truncated": snapshots_truncated,
                "limit": history_limit,
            },
            "incremental": incremental,
            "composite": {
                "current": composite_current.as_ref().map(|value| compact_composite_snapshot(value, false)),
                "baseline": composite_baseline.as_ref().map(|value| compact_composite_snapshot(value, false)),
                "incremental": composite_incremental,
                "history": {
                    "points": composite_history_points,
                    "returned": composite_history.len(),
                    "truncated": composite_history_truncated,
                    "limit": history_limit,
                },
            },
            "files": files,
            "latest_run": latest_run,
            "active_runs": active_runs,
            "active_runs_truncated": active_runs_truncated,
            "compaction": {
                "policy": compaction_policy,
                "inventory": self.store.compaction_summary()?,
            },
        });
        Ok(self.envelope(
            data,
            current.as_ref().and_then(|value| value["suite"].as_str()),
            None,
        ))
    }

    /// Returns bounded groups of named tests with exactly equal coverage
    /// observation sets.
    ///
    /// The selector chooses one immutable snapshot. Equality is coverage-only:
    /// it includes normalized kind, path, line, and region identity while
    /// ignoring hit counts and test logic. The result is a candidate list for
    /// human or tool-assisted review, never proof that tests are semantically
    /// interchangeable. Named observations are currently available from LCOV
    /// `TN:` records; other formats return an explicit unavailable status.
    #[allow(clippy::too_many_arguments)]
    pub fn find_duplicate_coverage_tests(
        &self,
        snapshot_id: Option<&str>,
        suite: Option<&str>,
        cursor: Option<&str>,
        max_groups: usize,
        max_tests_per_group: usize,
        max_words: usize,
        max_bytes: usize,
    ) -> AppResult<Value> {
        if !(1..=500).contains(&max_groups) {
            return Err(AppError::Validation(
                "max_groups must be between 1 and 500".to_owned(),
            ));
        }
        if !(2..=1_000).contains(&max_tests_per_group) {
            return Err(AppError::Validation(
                "max_tests_per_group must be between 2 and 1000".to_owned(),
            ));
        }
        validate_max_words(max_words)?;
        if !(1_000..=2_000_000).contains(&max_bytes) {
            return Err(AppError::Validation(
                "max_bytes must be between 1000 and 2000000".to_owned(),
            ));
        }
        let context = self.context(suite);
        let snapshot = if let Some(snapshot_id) = snapshot_id {
            let snapshot = self.store.snapshot(snapshot_id)?;
            if snapshot.get("repo_key").and_then(Value::as_str) != Some(context.repo_key.as_str()) {
                return Err(AppError::Validation(
                    "snapshot does not belong to the selected repository".to_owned(),
                ));
            }
            if suite
                .is_some_and(|value| snapshot.get("suite").and_then(Value::as_str) != Some(value))
            {
                return Err(AppError::Validation(
                    "suite does not match the selected snapshot".to_owned(),
                ));
            }
            snapshot
        } else {
            self.store
                .latest_snapshot(Some(&context.checkout_path), None, suite)?
                .ok_or_else(|| AppError::NotFound("no snapshots found".to_owned()))?
        };
        let selected_id = snapshot
            .get("id")
            .and_then(Value::as_str)
            .expect("stored snapshot projections always contain id");
        let selected_suite = snapshot
            .get("suite")
            .and_then(Value::as_str)
            .expect("stored snapshot projections always contain suite");
        let scope =
            format!("duplicate-coverage:{selected_id}:{selected_suite}:{max_tests_per_group}");
        let scope_digest = Sha256::digest(scope.as_bytes());
        let expected_anchor = hex_prefix(&scope_digest, scope_digest.len());
        let offset = if let Some(cursor) = cursor {
            let (anchor, occurrence) = decode_cursor(cursor, &scope)?;
            if anchor != expected_anchor {
                return Err(AppError::Validation(
                    "duplicate coverage cursor does not belong to this query".to_owned(),
                ));
            }
            occurrence.saturating_sub(1)
        } else {
            0
        };
        let projection = self.store.duplicate_coverage_test_groups(
            selected_id,
            offset,
            max_groups,
            max_tests_per_group,
        )?;
        let returned = projection
            .data
            .get("duplicate_test_groups")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let next_cursor = if projection.has_more && returned > 0 {
            Some(
                encode_cursor(
                    &expected_anchor,
                    &scope,
                    projection.offset.saturating_add(returned).saturating_add(1),
                )
                .expect("duplicate coverage page cursor occurrence is positive"),
            )
        } else {
            None
        };
        let collection_words = projection
            .data
            .get("duplicate_test_groups")
            .map_or(0, serialized_word_count);
        let page = json!({
            "returned": returned,
            "total": projection.total_groups,
            "word_count": collection_words,
            "max_words": max_words,
            "truncated": projection.has_more,
            "next_cursor": next_cursor,
        });
        let response = self.envelope(projection.data, Some(selected_suite), Some(page));
        let response = self.apply_budget(response, max_words)?;
        self.apply_byte_budget(response, max_bytes)
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

    #[allow(clippy::too_many_arguments)]
    fn review_history(
        &self,
        branch: Option<&str>,
        suite: Option<&str>,
        file_path: Option<&str>,
        worktree_id: Option<&str>,
        detail_snapshots: usize,
        summary_window: usize,
        detailed: bool,
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
            .map(|value| compact_history_snapshot(value, detailed))
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
        compact: bool,
        audit_regions: bool,
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
                let (compact_regions, region_count, regions_truncated) =
                    compact_insight_regions(&target["regions"]);
                let mut item = if compact {
                    json!({
                        "file_path": path,
                        "priority": priority,
                        "uncovered_lines": uncovered_lines,
                        "uncovered_branches": target["uncovered_branches"],
                        "uncovered_functions": target["uncovered_functions"],
                        "regions": compact_regions,
                        "region_count": region_count,
                        "regions_truncated": regions_truncated,
                    })
                } else {
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
                        "regions": compact_regions,
                        "region_count": region_count,
                        "regions_truncated": regions_truncated,
                    })
                };
                if audit_regions {
                    item["regions"] = target["regions"].clone();
                    item.as_object_mut()
                        .expect("insight items are JSON objects")
                        .remove("region_count");
                    item.as_object_mut()
                        .expect("insight items are JSON objects")
                        .remove("regions_truncated");
                }
                item
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

    /// Reads bounded source ranges for an immutable composite snapshot.
    ///
    /// Composite regions are stored independently from ordinary file/line
    /// rows, so source context is resolved from the snapshot's recorded
    /// checkout path and annotated with the canonical region states.
    pub fn composite_source_review(
        &self,
        composite_snapshot_id: &str,
        ranges: Vec<(String, i64, i64)>,
        max_source_lines: usize,
        max_words: usize,
        max_bytes: usize,
    ) -> AppResult<Value> {
        validate_max_words(max_words)?;
        validate_review_byte_budget(max_bytes)?;
        if !(10..=500).contains(&max_source_lines) {
            return Err(AppError::Validation(
                "source ranges max_source_lines must be between 10 and 500".to_owned(),
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
        let snapshot = self.store.composite_snapshot(composite_snapshot_id)?;
        self.validate_composite_context(&snapshot)?;
        let repo_path = snapshot["repo_path"]
            .as_str()
            .expect("stored composite snapshots always contain repo_path")
            .to_owned();
        let mut grouped = BTreeMap::<String, Vec<(i64, i64)>>::new();
        let mut requested_lines = 0usize;
        for (file_path, start, end) in ranges {
            validate_source_file_path(&file_path)?;
            if start < 1 || end < start {
                return Err(AppError::Validation(
                    "source range must have positive bounds with end >= start".to_owned(),
                ));
            }
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
        let regions = self.store.composite_regions_all(composite_snapshot_id)?;
        let mut sources = Vec::new();
        for (file_path, file_ranges) in grouped {
            let path = Path::new(&repo_path).join(&file_path);
            let content = fs::read_to_string(&path)?;
            let lines = content.lines().collect::<Vec<_>>();
            let selected_ranges = file_ranges
                .iter()
                .map(|(start, end)| {
                    let start_index = (*start as usize).saturating_sub(1).min(lines.len());
                    let end_index = (*end as usize).min(lines.len());
                    json!({
                        "start": start,
                        "end": end,
                        "lines": (start_index..end_index)
                            .map(|index| json!({"line_number": index + 1, "text": lines[index]}))
                            .collect::<Vec<_>>(),
                    })
                })
                .collect::<Vec<_>>();
            let annotated_regions = regions
                .iter()
                .filter(|region| {
                    region.get("path").and_then(Value::as_str) == Some(file_path.as_str())
                        && file_ranges.iter().any(|(start, end)| {
                            region
                                .get("end_line")
                                .and_then(Value::as_i64)
                                .is_some_and(|region_end| region_end >= *start)
                                && region
                                    .get("start_line")
                                    .and_then(Value::as_i64)
                                    .is_some_and(|region_start| region_start <= *end)
                        })
                })
                .cloned()
                .collect::<Vec<_>>();
            sources.push(json!({
                "file_path": file_path,
                "source_resolution": {"status":"checkout","path":path},
                "ranges": selected_ranges,
                "regions": annotated_regions,
            }));
        }
        let response = self.envelope(
            json!({
                "focus": "source",
                "task": "source",
                "representation": "review",
                "claim_status": "supported",
                "reasons": [],
                "measurement": compact_composite_snapshot(&snapshot, false),
                "baseline": Value::Null,
                "source": sources,
            }),
            None,
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

fn compact_context_project(value: &Value) -> Value {
    let keys = [
        "id",
        "snapshot_count",
        "composite_snapshot_count",
        "command_count",
        "run_count",
        "latest_snapshot_id",
        "latest_composite_snapshot_id",
        "latest_snapshot_age_seconds",
        "latest_run_age_seconds",
        "latest_branch",
        "latest_commit_sha",
        "latest_suite",
        "line_rate",
        "branch_rate",
        "function_rate",
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
        "detail_retention",
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
    if let Some(execution) = value
        .get("metadata")
        .and_then(|metadata| metadata.get("execution"))
    {
        result.insert("execution".to_owned(), execution.clone());
    }
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
        let Some(status) = value
            .get("category")
            .or_else(|| value.get("status"))
            .and_then(Value::as_str)
        else {
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

fn remove_incremental_changed_lines(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        object.remove("changed_lines");
    }
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

fn incremental_next_action(gain: &Value) -> Value {
    if gain
        .get("regressed")
        .and_then(Value::as_u64)
        .is_some_and(|count| count > 0)
    {
        return json!({
            "kind": "inspect_regression",
            "reason": "the selected incremental run regressed previously covered lines",
        });
    }
    if gain
        .get("newly_covered")
        .and_then(Value::as_u64)
        .is_some_and(|count| count > 0)
    {
        return json!({
            "kind": "review_incremental_gain",
            "reason": "the selected incremental run added newly covered lines",
        });
    }
    json!({
        "kind": "compare_selected_runs",
        "reason": "no newly covered or regressed line was observed in the selected snapshots",
    })
}

fn compact_insight_regions(value: &Value) -> (Vec<Value>, usize, bool) {
    let regions = value.as_array().map_or(&[][..], Vec::as_slice);
    let compact = regions
        .iter()
        .take(MAX_COMPACT_INSIGHT_REGIONS)
        .filter_map(|region| {
            let start = region.get("start").and_then(Value::as_i64)?;
            let end = region.get("end").and_then(Value::as_i64)?;
            let line_count = region
                .get("line_count")
                .and_then(Value::as_i64)
                .unwrap_or_else(|| end.saturating_sub(start).saturating_add(1));
            Some(json!([start, end, line_count]))
        })
        .collect::<Vec<_>>();
    (
        compact,
        regions.len(),
        regions.len() > MAX_COMPACT_INSIGHT_REGIONS,
    )
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
        "baseline_total_lines",
        "current_total_lines",
        "baseline_covered_lines",
        "current_covered_lines",
        "baseline_line_rate",
        "current_line_rate",
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

fn compact_incremental_diff(value: &mut Value) {
    // The primary incremental projection already carries union files and
    // attribution. Keep the nested compact diagnostic focused on its purpose:
    // replacement-style deltas and affected ranges. `review` and `audit`
    // retain the full diff, including file and attribution records.
    if let Some(object) = value.as_object_mut() {
        object.remove("files");
        object.remove("test_attribution");
        object.remove("detail_source");
        object.remove("reasons");
    }
    if let Some(metric_deltas) = value.get("metric_deltas").cloned() {
        value["metric_deltas"] = compact_incremental_rate_deltas(&metric_deltas);
    }
    if let Some(regions) = value.get("regions").and_then(Value::as_array) {
        let compact = compact_changed_regions(regions);
        value["regions"] = compact;
    }
    remove_incremental_changed_lines(value);
}

fn compact_incremental_rate_deltas(value: &Value) -> Value {
    let keys = [
        "line_rate_delta",
        "branch_rate_delta",
        "function_rate_delta",
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

fn incremental_measurement_scope(snapshot: &Value) -> &'static str {
    let mode = snapshot
        .get("metadata")
        .and_then(|metadata| metadata.get("execution"))
        .and_then(|execution| execution.get("mode"))
        .and_then(Value::as_str);
    if mode == Some("incremental") {
        "selected_subset"
    } else {
        "complete_snapshot"
    }
}

fn dashboard_history_point(value: &Value) -> Value {
    let keys = [
        "id",
        "created_at",
        "branch",
        "commit_sha",
        "suite",
        "format",
        "detail_retention",
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
    if let Some(execution) = value
        .get("metadata")
        .and_then(|metadata| metadata.get("execution"))
    {
        result.insert("execution".to_owned(), execution.clone());
    }
    Value::Object(result)
}

fn dashboard_current_files(values: &[Value]) -> Value {
    let mut items = values
        .iter()
        .map(|value| compact_file(value, false))
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        left.get("line_rate")
            .and_then(Value::as_f64)
            .partial_cmp(&right.get("line_rate").and_then(Value::as_f64))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.get("file_path")
                    .and_then(Value::as_str)
                    .cmp(&right.get("file_path").and_then(Value::as_str))
            })
    });
    let total = items.len();
    let truncated = total > 16;
    items.truncate(16);
    json!({"source":"current_snapshot","items":items,"total":total,"truncated":truncated})
}

fn dashboard_incremental_files(value: &Value) -> Value {
    let mut items = value
        .get("files")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(compact_file_change)
        .collect::<Vec<_>>();
    let total = items.len();
    items.truncate(16);
    json!({
        "source":if value.get("comparison_mode").and_then(Value::as_str) == Some("additive_union") { "current_union" } else { "current_vs_baseline" },
        "items":items,
        "total":total,
        "truncated": value.get("files_truncated").and_then(Value::as_bool).unwrap_or(total > 16 || total == 24),
    })
}

fn dashboard_incremental(value: &Value) -> Value {
    if value.get("status").and_then(Value::as_str) == Some("unmeasured") {
        return json!({
            "status":"unmeasured",
            "reason":value.get("reason").cloned().unwrap_or(Value::Null),
            "run":value.get("current").cloned().unwrap_or(Value::Null),
        });
    }
    let regions = value
        .get("regions")
        .and_then(Value::as_array)
        .map(|regions| compact_changed_regions(regions))
        .unwrap_or_else(|| json!([]));
    let diff = value
        .get("diff")
        .map(|value| {
            let mut diff = value.clone();
            compact_incremental_diff(&mut diff);
            diff
        })
        .unwrap_or(Value::Null);
    json!({
        "status":"measured",
        "comparison_mode":value.get("comparison_mode").cloned().unwrap_or_else(|| json!("snapshot_diff")),
        "measurement_scope":value.get("measurement_scope").cloned().unwrap_or(Value::Null),
        "run":value.get("run").map(|snapshot| compact_snapshot(snapshot, false)),
        "current_snapshot_ids":value.get("current_snapshot_ids").cloned().unwrap_or_else(|| json!([])),
        "merge":value.get("merge").cloned().unwrap_or(Value::Null),
        "metric_deltas":value.get("metric_deltas").cloned().unwrap_or(Value::Null),
        "coverage_gain":value.get("coverage_gain").cloned().unwrap_or(Value::Null),
        "regions":regions,
        "regions_truncated":value.get("regions_truncated").cloned().unwrap_or(Value::Null),
        "diff":diff,
        "test_attribution":value.get("test_attribution").cloned().unwrap_or(Value::Null),
        "detail_source":value.get("detail_source").cloned().unwrap_or(Value::Null),
        "next_action":incremental_next_action(value.get("coverage_gain").unwrap_or(&Value::Null)),
    })
}

fn dashboard_composite_history_point(value: &Value) -> Value {
    json!({
        "id": value.get("id"),
        "created_at": value.get("created_at"),
        "status": value.get("status"),
        "covered_regions": value.get("covered_regions"),
        "total_regions": value.get("total_regions"),
        "region_rate": value.get("region_rate"),
        "checkout_revision": value.get("checkout_revision"),
        "checkout_dirty": value.get("checkout_dirty"),
    })
}

fn dashboard_composite_incremental(value: &Value) -> Value {
    if value.get("status").and_then(Value::as_str) == Some("unmeasured") {
        return json!({
            "status": "unmeasured",
            "reason": value.get("reason").cloned().unwrap_or(Value::Null),
            "baseline": value.get("baseline").cloned().unwrap_or(Value::Null),
            "current": value.get("current").cloned().unwrap_or(Value::Null),
        });
    }
    json!({
        "status": value.get("status").cloned().unwrap_or_else(|| json!("measured")),
        "baseline": value.get("baseline").cloned().unwrap_or(Value::Null),
        "current": value.get("current").cloned().unwrap_or(Value::Null),
        "overall": value.get("overall").cloned().unwrap_or(Value::Null),
        "metric_deltas": value.get("metric_deltas").cloned().unwrap_or(Value::Null),
        "components": value.get("components").cloned().unwrap_or_else(|| json!([])),
        "coverage_gain": value.get("coverage_gain").cloned().unwrap_or_else(|| json!({})),
        "affected_lines": value.get("affected_lines").cloned().unwrap_or_else(|| json!([])),
        "regions": value.get("regions").cloned().unwrap_or_else(|| json!([])),
        "regions_truncated": value.get("regions_truncated").cloned().unwrap_or(Value::Null),
        "test_attribution": value.get("test_attribution").cloned().unwrap_or(Value::Null),
        "detail_source": value.get("detail_source").cloned().unwrap_or(Value::Null),
        "next_action": value.get("next_action").cloned().unwrap_or(Value::Null),
    })
}

fn compact_history_snapshot(value: &Value, detailed: bool) -> Value {
    let compact_keys = [
        "id",
        "created_at",
        "branch",
        "commit_sha",
        "suite",
        "file_path",
        "line_rate",
        "branch_rate",
        "function_rate",
        "region_rate",
    ];
    let detailed_keys = [
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
    let keys: &[&str] = if detailed {
        &detailed_keys
    } else {
        &compact_keys
    };
    let mut result = Map::new();
    for key in keys {
        if let Some(value) = value.get(key) {
            result.insert((*key).to_owned(), value.clone());
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
    if !detailed {
        let artifact_count = value
            .get("artifact_specs")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        return json!({
            "id": value.get("id").cloned().unwrap_or(Value::Null),
            "name": value.get("name").cloned().unwrap_or(Value::Null),
            "enabled": value.get("enabled").cloned().unwrap_or(Value::Null),
            "created_at": value.get("created_at").cloned().unwrap_or(Value::Null),
            "artifact_count": artifact_count,
            "duration_estimate_ms": value.get("duration_estimate_ms").cloned().unwrap_or(Value::Null),
            "duration_p90_ms": value.get("duration_p90_ms").cloned().unwrap_or(Value::Null),
            "duration_sample_count": value.get("duration_sample_count").cloned().unwrap_or(Value::Null),
        });
    }
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
    for key in ["approved_by", "approval_note", "branch", "commit_sha"] {
        result.insert(
            key.to_owned(),
            value.get(key).cloned().unwrap_or(Value::Null),
        );
    }
    Value::Object(result)
}

fn compact_run_result(value: &Value, detailed: bool) -> Value {
    if detailed {
        return value.clone();
    }
    let Some(object) = value.as_object() else {
        return value.clone();
    };
    let keys = [
        "id",
        "command_id",
        "command_name",
        "idempotency_key",
        "branch",
        "commit_sha",
        "queued_at",
        "started_at",
        "ended_at",
        "duration_ms",
        "timeout_seconds",
        "queue_duration_ms",
        "exit_code",
        "status",
        "terminal",
        "poll_after_ms",
        "queue_position",
        "execution_mode",
        "execution",
        "cancellation_requested",
        "cancellation_requested_at",
        "submission_reused",
        "reuse_reason",
        "coverage_ingest",
        "composite_snapshot_id",
        "incremental_review",
        "error",
    ];
    let mut result = Map::new();
    for key in keys {
        if let Some(value) = object.get(key) {
            result.insert(key.to_owned(), value.clone());
        }
    }
    Value::Object(result)
}

fn compact_composite_snapshot(snapshot: &Value, detailed: bool) -> Value {
    let status = snapshot
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("incomplete");
    let reasons = snapshot
        .get("reasons")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let complete = status == "complete" && reasons.as_array().is_none_or(Vec::is_empty);
    let covered = snapshot
        .get("covered_regions")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let total = snapshot
        .get("total_regions")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let mut result = json!({
        "id": snapshot.get("id"),
        "created_at": snapshot.get("created_at"),
        "run_id": snapshot.get("run_id"),
        "command_id": snapshot.get("command_id"),
        "status": status,
        "complete": complete,
        "covered_regions": covered,
        "total_regions": total,
        "region_rate": snapshot.get("region_rate"),
        "coverage_percent": snapshot.get("region_rate").and_then(Value::as_f64).map(|rate| rate * 100.0),
        "mapping_version": snapshot.get("mapping_version"),
        "inventory_hash": snapshot.get("inventory_hash"),
        "checkout_revision": snapshot.get("checkout_revision"),
        "checkout_dirty": snapshot.get("checkout_dirty"),
        "blocking_reasons": reasons,
        "remediation": snapshot.get("remediation").cloned().unwrap_or_else(|| json!({})),
        "components": snapshot.get("components").cloned().unwrap_or_else(|| json!([])),
    });
    if detailed {
        let object = result
            .as_object_mut()
            .expect("composite compact projection is always an object");
        for key in ["inventory", "provenance", "component_evidence"] {
            object.insert(
                key.to_owned(),
                snapshot.get(key).cloned().unwrap_or(Value::Null),
            );
        }
    }
    result
}

fn ensure_compatible_composites(current: &Value, baseline: &Value) -> AppResult<()> {
    if current.get("repo_key") != baseline.get("repo_key") {
        return Err(AppError::Validation(
            "incremental composite comparisons require snapshots from the same repository"
                .to_owned(),
        ));
    }
    if current.get("mapping_version") != baseline.get("mapping_version") {
        return Err(AppError::Validation(
            "incremental composite comparisons require the same mapping version".to_owned(),
        ));
    }
    let current_inventory = current.get("inventory_hash").and_then(Value::as_str);
    let baseline_inventory = baseline.get("inventory_hash").and_then(Value::as_str);
    if current_inventory.is_none()
        || baseline_inventory.is_none()
        || current_inventory != baseline_inventory
    {
        return Err(AppError::Validation(
            "incremental composite comparisons require the same non-null inventory hash".to_owned(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn composite_incremental_projection(
    current: &Value,
    baseline: &Value,
    current_regions: Vec<Value>,
    baseline_regions: Vec<Value>,
    file_path: Option<&str>,
    max_regions: usize,
    representation: &str,
    test_attribution: Value,
) -> Value {
    let filter = |region: &Value| {
        file_path.is_none_or(|path| region.get("path").and_then(Value::as_str) == Some(path))
    };
    let current_map = current_regions
        .into_iter()
        .filter(filter)
        .filter_map(|region| {
            let key = region
                .get("region_key")
                .and_then(Value::as_str)
                .map(str::to_owned)?;
            Some((key, region))
        })
        .collect::<BTreeMap<_, _>>();
    let baseline_map = baseline_regions
        .into_iter()
        .filter(filter)
        .filter_map(|region| {
            let key = region
                .get("region_key")
                .and_then(Value::as_str)
                .map(str::to_owned)?;
            Some((key, region))
        })
        .collect::<BTreeMap<_, _>>();
    let mut changed = Vec::new();
    let mut counts = BTreeMap::from([
        ("newly_covered".to_owned(), 0_i64),
        ("regressed".to_owned(), 0_i64),
        ("hit_count_only".to_owned(), 0_i64),
        ("added".to_owned(), 0_i64),
        ("removed".to_owned(), 0_i64),
    ]);
    let mut emit = |before: Option<&Value>, after: Option<&Value>, category: &str| {
        *counts.entry(category.to_owned()).or_default() += 1;
        let source = after
            .or(before)
            .expect("changed composite region has a source row");
        let mut row = source.clone();
        let object = row
            .as_object_mut()
            .expect("stored composite region rows are objects");
        object.insert("category".to_owned(), json!(category));
        object.insert(
            "baseline_state".to_owned(),
            before
                .and_then(|value| value.get("state"))
                .cloned()
                .unwrap_or(Value::Null),
        );
        object.insert(
            "current_state".to_owned(),
            after
                .and_then(|value| value.get("state"))
                .cloned()
                .unwrap_or(Value::Null),
        );
        object.insert(
            "baseline_hits".to_owned(),
            before
                .map(|value| json!(composite_region_hits(value)))
                .unwrap_or(Value::Null),
        );
        object.insert(
            "current_hits".to_owned(),
            after
                .map(|value| json!(composite_region_hits(value)))
                .unwrap_or(Value::Null),
        );
        changed.push(row);
    };
    for (key, after) in &current_map {
        let before = baseline_map.get(key);
        let category = before.and_then(|before| {
            let before_covered = composite_region_covered(before);
            let after_covered = composite_region_covered(after);
            if !before_covered && after_covered {
                Some("newly_covered")
            } else if before_covered && !after_covered {
                Some("regressed")
            } else if composite_region_hits(before) != composite_region_hits(after) {
                Some("hit_count_only")
            } else {
                None
            }
        });
        if let Some(category) = category {
            emit(
                Some(before.expect("category has a baseline region")),
                Some(after),
                category,
            );
        } else if before.is_none() {
            emit(None, Some(after), "added");
        }
    }
    for (key, before) in &baseline_map {
        if !current_map.contains_key(key) {
            emit(Some(before), None, "removed");
        }
    }
    changed.sort_by(|left, right| {
        composite_category_order(left)
            .cmp(&composite_category_order(right))
            .then_with(|| {
                left.get("path")
                    .and_then(Value::as_str)
                    .cmp(&right.get("path").and_then(Value::as_str))
            })
            .then_with(|| {
                left.get("start_line")
                    .and_then(Value::as_i64)
                    .cmp(&right.get("start_line").and_then(Value::as_i64))
            })
    });
    let regions_truncated = changed.len() > max_regions;
    let selected = changed.into_iter().take(max_regions).collect::<Vec<_>>();
    let affected_lines = grouped_composite_regions(&selected);
    let regions = if representation == "compact" {
        affected_lines.clone()
    } else {
        selected
    };
    let covered_delta = current
        .get("covered_regions")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        - baseline
            .get("covered_regions")
            .and_then(Value::as_i64)
            .unwrap_or(0);
    let total_delta = current
        .get("total_regions")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        - baseline
            .get("total_regions")
            .and_then(Value::as_i64)
            .unwrap_or(0);
    let current_rate = current.get("region_rate").and_then(Value::as_f64);
    let baseline_rate = baseline.get("region_rate").and_then(Value::as_f64);
    let component_deltas = component_deltas(current, baseline);
    let snapshot_reference = |snapshot: &Value| {
        if representation == "audit" {
            compact_composite_snapshot(snapshot, true)
        } else {
            json!({
                "id": snapshot.get("id"),
                "status": snapshot.get("status"),
                "covered_regions": snapshot.get("covered_regions"),
                "total_regions": snapshot.get("total_regions"),
                "region_rate": snapshot.get("region_rate"),
            })
        }
    };
    json!({
        "status": "measured",
        "baseline": snapshot_reference(baseline),
        "current": snapshot_reference(current),
        "overall": {
            "covered_regions_delta": covered_delta,
            "total_regions_delta": total_delta,
            "region_rate_delta": current_rate.zip(baseline_rate).map(|(now, before)| now - before),
            "coverage_percent_delta": current_rate.zip(baseline_rate).map(|(now, before)| (now - before) * 100.0),
        },
        "metric_deltas": {
            "covered_regions_delta": covered_delta,
            "total_regions_delta": total_delta,
            "region_rate_delta": current_rate.zip(baseline_rate).map(|(now, before)| now - before),
            "coverage_percent_delta": current_rate.zip(baseline_rate).map(|(now, before)| (now - before) * 100.0),
        },
        "components": component_deltas,
        "coverage_gain": counts,
        "affected_lines": affected_lines,
        "regions": regions,
        "regions_truncated": regions_truncated,
        "test_attribution": test_attribution,
        "detail_source": "composite_relational",
        "next_action": if counts.get("newly_covered").copied().unwrap_or(0) > 0 {
            json!({"kind":"retain_baseline","reason":"new canonical regions are covered"})
        } else {
            json!({"kind":"inspect_uncovered","reason":"no newly covered canonical regions were observed"})
        },
    })
}

fn component_deltas(current: &Value, baseline: &Value) -> Vec<Value> {
    ["rust", "python", "javascript"]
        .into_iter()
        .map(|component| {
            let find = |snapshot: &Value| -> Option<Value> {
                snapshot
                    .get("components")
                    .and_then(Value::as_array)
                    .and_then(|values| {
                        values.iter().find(|value| {
                            value.get("component").and_then(Value::as_str) == Some(component)
                        })
                    })
                    .cloned()
            };
            let before = find(baseline);
            let after = find(current);
            let covered_before = before
                .as_ref()
                .and_then(|value| value.get("covered_regions"))
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let covered_after = after
                .as_ref()
                .and_then(|value| value.get("covered_regions"))
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let total_before = before
                .as_ref()
                .and_then(|value| value.get("total_regions"))
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let total_after = after
                .as_ref()
                .and_then(|value| value.get("total_regions"))
                .and_then(Value::as_i64)
                .unwrap_or(0);
            json!({
                "component": component,
                "covered_regions_delta": covered_after - covered_before,
                "total_regions_delta": total_after - total_before,
                "covered_regions": covered_after,
                "total_regions": total_after,
                "region_rate": after.as_ref().and_then(|value| value.get("region_rate")).cloned().unwrap_or(Value::Null),
            })
        })
        .collect()
}

fn composite_region_covered(region: &Value) -> bool {
    region.get("state").and_then(Value::as_str) == Some("covered")
}

fn composite_region_hits(region: &Value) -> i64 {
    region
        .get("variants")
        .and_then(Value::as_array)
        .map(|variants| {
            variants
                .iter()
                .filter_map(|variant| variant.get("hits").and_then(Value::as_i64))
                .sum()
        })
        .unwrap_or_else(|| region.get("hits").and_then(Value::as_i64).unwrap_or(0))
}

fn composite_category_order(region: &Value) -> usize {
    match region.get("category").and_then(Value::as_str) {
        Some("newly_covered") => 0,
        Some("regressed") => 1,
        Some("added") => 2,
        Some("removed") => 3,
        Some("hit_count_only") => 4,
        _ => 5,
    }
}

fn grouped_composite_regions(regions: &[Value]) -> Vec<Value> {
    let mut grouped = BTreeMap::<(String, String), Vec<i64>>::new();
    for region in regions {
        let Some(path) = region.get("path").and_then(Value::as_str) else {
            continue;
        };
        let Some(start) = region.get("start_line").and_then(Value::as_i64) else {
            continue;
        };
        let category = region
            .get("category")
            .or_else(|| region.get("state"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        grouped
            .entry((path.to_owned(), category.to_owned()))
            .or_default()
            .push(start);
    }
    let mut result = Vec::new();
    for ((path, category), mut lines) in grouped {
        lines.sort_unstable();
        lines.dedup();
        let mut start = None;
        let mut end: Option<i64> = None;
        for line in lines {
            match (start, end) {
                (Some(_first), Some(last)) if line <= last.saturating_add(1) => end = Some(line),
                (Some(first), Some(last)) => {
                    result.push(json!({
                        "file_path": path,
                        "category": category,
                        "status": category,
                        "start": first,
                        "end": last,
                        "line_count": last - first + 1,
                    }));
                    start = Some(line);
                    end = Some(line);
                }
                _ => {
                    start = Some(line);
                    end = Some(line);
                }
            }
        }
        let first = start.expect("grouped composite region has at least one line");
        let last = end.expect("grouped composite region has a final line");
        result.push(json!({
            "file_path": path,
            "category": category,
            "status": category,
            "start": first,
            "end": last,
            "line_count": last - first + 1,
        }));
    }
    result.sort_by(|left, right| {
        left.get("file_path")
            .and_then(Value::as_str)
            .cmp(&right.get("file_path").and_then(Value::as_str))
            .then_with(|| {
                left.get("start")
                    .and_then(Value::as_i64)
                    .cmp(&right.get("start").and_then(Value::as_i64))
            })
    });
    result
}

fn automatic_incremental_review_placeholder(
    status: &str,
    baseline_snapshot_id: Option<&str>,
    baseline_is_composite: bool,
    reason: &str,
) -> Value {
    let baseline = baseline_snapshot_id
        .map(|snapshot_id| {
            if baseline_is_composite {
                json!({
                    "kind": "explicit",
                    "composite_snapshot_id": snapshot_id,
                })
            } else {
                json!({
                    "kind": "explicit",
                    "snapshot_id": snapshot_id,
                })
            }
        })
        .unwrap_or(Value::Null);
    json!({
        "automatic": true,
        "task": "incremental",
        "status": status,
        "claim_status": "not_measured",
        "reasons": [reason],
        "measurement": Value::Null,
        "baseline": Value::Null,
        "baseline_selector": baseline,
        "next_action": {
            "kind": "obtain_measurement",
            "reason": reason,
        },
    })
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
    fn composite_projection_helpers_cover_categories_budgets_and_history_shapes() {
        let baseline = json!({
            "id":"base",
            "status":"complete",
            "repo_key":"repo",
            "mapping_version":"mapping",
            "inventory_hash":"inventory",
            "covered_regions":2,
            "total_regions":6,
            "region_rate":1.0 / 3.0,
            "components":[
                {"component":"rust","covered_regions":1,"total_regions":2,"region_rate":0.5}
            ],
            "reasons":[],
            "remediation":{}
        });
        let current = json!({
            "id":"current",
            "status":"complete",
            "repo_key":"repo",
            "mapping_version":"mapping",
            "inventory_hash":"inventory",
            "covered_regions":3,
            "total_regions":7,
            "region_rate":3.0 / 7.0,
            "components":[
                {"component":"rust","covered_regions":1,"total_regions":3,"region_rate":1.0 / 3.0},
                {"component":"python","covered_regions":1,"total_regions":1,"region_rate":1.0}
            ],
            "reasons":[],
            "remediation":{}
        });
        let region = |key: &str, path: &str, line: i64, state: &str, hits: i64| {
            json!({
                "region_key":key,
                "path":path,
                "start_line":line,
                "state":state,
                "variants":[{"hits":hits}]
            })
        };
        let baseline_regions = vec![
            region("new", "src/a.py", 1, "uncovered", 0),
            region("regress", "src/a.py", 2, "covered", 1),
            region("hit", "src/a.py", 3, "covered", 1),
            region("removed", "src/a.py", 4, "covered", 1),
            region("same", "src/a.py", 5, "uncovered", 0),
            json!({"path":"src/a.py"}),
        ];
        let current_regions = vec![
            region("new", "src/a.py", 1, "covered", 1),
            region("regress", "src/a.py", 2, "uncovered", 0),
            region("hit", "src/a.py", 3, "covered", 2),
            region("added", "src/a.py", 6, "covered", 1),
            region("added-adjacent", "src/a.py", 7, "covered", 1),
            region("added-gap", "src/a.py", 9, "covered", 1),
            region("other-file", "src/b.py", 1, "covered", 1),
            region("same", "src/a.py", 5, "uncovered", 0),
            json!({"path":"src/a.py"}),
        ];
        let compact = composite_incremental_projection(
            &current,
            &baseline,
            current_regions.clone(),
            baseline_regions.clone(),
            None,
            4,
            "compact",
            json!({"status":"measured"}),
        );
        assert_eq!(compact["coverage_gain"]["newly_covered"], 1);
        assert_eq!(compact["coverage_gain"]["regressed"], 1);
        assert_eq!(compact["coverage_gain"]["hit_count_only"], 1);
        assert_eq!(compact["coverage_gain"]["added"], 4);
        assert_eq!(compact["coverage_gain"]["removed"], 1);
        assert_eq!(compact["next_action"]["kind"], "retain_baseline");
        assert_eq!(compact["regions_truncated"], true);

        let audit = composite_incremental_projection(
            &current,
            &baseline,
            current_regions,
            baseline_regions,
            Some("src/a.py"),
            20,
            "audit",
            json!({"status":"unavailable"}),
        );
        assert_eq!(audit["baseline"]["inventory_hash"], "inventory");
        assert_eq!(audit["test_attribution"]["status"], "unavailable");

        let inspect = composite_incremental_projection(
            &json!({"id":"only","status":"complete","covered_regions":0,"total_regions":1,"region_rate":0.0}),
            &json!({"id":"only-base","status":"complete","covered_regions":0,"total_regions":1,"region_rate":0.0}),
            vec![region("r", "src/a.py", 1, "uncovered", 0)],
            vec![region("r", "src/a.py", 1, "uncovered", 0)],
            None,
            5,
            "review",
            Value::Null,
        );
        assert_eq!(inspect["next_action"]["kind"], "inspect_uncovered");
        assert!(inspect["regions"].is_array());

        assert!(
            ensure_compatible_composites(
                &json!({"repo_key":"one","mapping_version":"mapping","inventory_hash":"inventory"}),
                &json!({"repo_key":"two","mapping_version":"mapping","inventory_hash":"inventory"})
            )
            .is_err()
        );
        assert!(
            ensure_compatible_composites(
                &json!({"repo_key":"one","mapping_version":"one","inventory_hash":"inventory"}),
                &json!({"repo_key":"one","mapping_version":"two","inventory_hash":"inventory"})
            )
            .is_err()
        );
        assert!(
            ensure_compatible_composites(
                &json!({"repo_key":"one","mapping_version":"one","inventory_hash":null}),
                &json!({"repo_key":"one","mapping_version":"one","inventory_hash":"inventory"})
            )
            .is_err()
        );
        assert!(
            ensure_compatible_composites(
                &json!({"repo_key":"one","mapping_version":"one","inventory_hash":"current"}),
                &json!({"repo_key":"one","mapping_version":"one","inventory_hash":"base"})
            )
            .is_err()
        );

        assert_eq!(
            dashboard_composite_incremental(&json!({"status":"unmeasured"}))["status"],
            "unmeasured"
        );
        assert_eq!(
            dashboard_composite_incremental(&json!({}))["status"],
            "measured"
        );
        assert_eq!(
            dashboard_composite_incremental(&compact)["status"],
            "measured"
        );
        assert_eq!(
            compact_history_snapshot(&json!({"id":"one","line_rate":0.5}), false)["id"],
            "one"
        );
        assert_eq!(
            compact_history_snapshot(&json!({"id":"one","total_lines":1}), true)["total_lines"],
            1
        );
        assert_eq!(history_metric(&[], "line_rate"), Value::Null);
        assert_eq!(
            history_metric(
                &[json!({"line_rate":0.2}), json!({"line_rate":0.1})],
                "line_rate"
            )["trend"],
            "improving"
        );
        assert_eq!(
            history_metric(
                &[json!({"line_rate":0.1}), json!({"line_rate":0.2})],
                "line_rate"
            )["trend"],
            "regressing"
        );
        assert_eq!(
            history_metric(
                &[json!({"line_rate":0.1}), json!({"line_rate":0.1})],
                "line_rate"
            )["trend"],
            "unchanged"
        );
        let history = summarize_history(&[
            json!({"line_rate":0.2}),
            json!({"line_rate":0.3}),
            json!({"line_rate":0.3}),
            json!({"line_rate":0.1}),
            json!({}),
        ]);
        assert_eq!(history["regression_runs"], 1);
        assert_eq!(history["improvement_runs"], 1);
        assert_eq!(history["unchanged_runs"], 1);

        let command = json!({"id":"command","name":"name","artifact_specs":[]});
        assert_eq!(compact_command(&command, false)["artifact_count"], 0);
        assert_eq!(
            compact_command(&json!({"id":"command"}), true)["artifact_specs"],
            json!([])
        );
        assert_eq!(compact_run_result(&json!(true), false), true);
        assert_eq!(compact_run_result(&json!({"id":"run"}), true)["id"], "run");
        assert_eq!(
            compact_composite_snapshot(&json!({"status":"complete"}), false)["total_regions"],
            0
        );
        assert!(
            compact_composite_snapshot(
                &json!({"status":"complete","reasons":[],"inventory":{}}),
                true
            )
            .get("inventory")
            .is_some()
        );
        assert_eq!(
            composite_region_hits(&json!({"variants":[{"hits":2},{"hits":3}]})),
            5
        );
        assert_eq!(composite_region_hits(&json!({"hits":4})), 4);
        assert_eq!(
            composite_category_order(&json!({"category":"newly_covered"})),
            0
        );
        assert_eq!(
            composite_category_order(&json!({"category":"regressed"})),
            1
        );
        assert_eq!(composite_category_order(&json!({"category":"added"})), 2);
        assert_eq!(composite_category_order(&json!({"category":"removed"})), 3);
        assert_eq!(
            composite_category_order(&json!({"category":"hit_count_only"})),
            4
        );
        assert_eq!(composite_category_order(&json!({"category":"other"})), 5);
        assert_eq!(
            grouped_composite_regions(&[
                json!({"path":"src/a.py","start_line":1,"category":"added"}),
                json!({"path":"src/a.py","start_line":2,"category":"added"}),
                json!({"path":"src/a.py","start_line":4,"category":"added"}),
                json!({"path":"src/a.py","start_line":4,"category":"added"}),
                json!({"path":"src/a.py"}),
                json!({"start_line":8})
            ])
            .len(),
            2
        );
        assert_eq!(
            automatic_incremental_review_placeholder("pending", Some("base"), true, "wait")["baseline_selector"]
                ["composite_snapshot_id"],
            "base"
        );
        assert_eq!(
            automatic_incremental_review_placeholder("pending", Some("base"), false, "wait")["baseline_selector"]
                ["snapshot_id"],
            "base"
        );
        assert!(
            automatic_incremental_review_placeholder("pending", None, false, "wait")["baseline_selector"]
                .is_null()
        );
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
        std::fs::write(
            &report,
            "TN:service-test\nSF:src/a.py\nDA:1,1\nend_of_record\n",
        )
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
        let store = CoverageStore::open(directory.path().join("coverage.duckdb"), config.clone())
            .expect("store");
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
            "TN:service-test\nSF:src/a.py\nDA:1,1\nDA:2,0\nBRDA:2,0,0,-\nFN:4,func\nFNDA:0,func\nend_of_record\n",
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
        let mut many_report = "TN:service-test\nSF:src/a.py\nDA:1,1\n".to_owned();
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
        let dashboard_repo_path = project.repo_path.replace('\'', "''");
        let dashboard_repo_key = project.repo_key.replace('\'', "''");
        store
            .execute_sql_for_test(&format!(
                "INSERT INTO runs (id, command_id, command_name, command, cwd, repo_path, repo_key, branch, commit_sha, started_at, ended_at, duration_ms, exit_code, status, stdout_path, stderr_path, parsed_summary, artifact_paths, queued_at, queue_duration_ms, cancellation_requested_at, execution_mode, execution_identity, execution_label) VALUES ('dashboard-terminal', 'dashboard-command', 'dashboard-command', 'true', '{dashboard_repo_path}', '{dashboard_repo_path}', '{dashboard_repo_key}', NULL, NULL, current_timestamp, current_timestamp, 1, 0, 'passed', 'stdout.log', 'stderr.log', '{{}}', '[]', current_timestamp, 0, NULL, 'full', 'full:v1', 'full'); INSERT INTO run_jobs (id, command_id, command_name, command, idempotency_key, cwd, repo_path, repo_key, branch, commit_sha, queued_at, started_at, ended_at, timeout_seconds, max_summary_lines, status, stdout_path, stderr_path, error, cancellation_requested_at, execution_mode, execution_identity, execution_label) VALUES ('dashboard-active', 'dashboard-command', 'dashboard-command', 'true', NULL, '{dashboard_repo_path}', '{dashboard_repo_path}', '{dashboard_repo_key}', NULL, NULL, current_timestamp, NULL, NULL, 30, 20, 'queued', 'stdout.log', 'stderr.log', '', NULL, 'incremental', 'case:v1', 'case')"
            ))
            .expect("dashboard run fixtures");
        let service = CoverageService::new(
            store.clone(),
            RequestContext {
                repo_key: project.repo_key,
                checkout_path: project.repo_path,
                suite: None,
            },
        );
        let empty_command_context = service
            .project_context(None, 600, false)
            .expect("project context without registered commands");
        assert!(
            empty_command_context["data"]["commands"]
                .as_array()
                .is_some_and(Vec::is_empty)
        );
        assert!(
            service
                .project_context(Some("invalid"), 600, false)
                .is_err()
        );
        assert!(
            service
                .attach_automatic_incremental_review(json!({
                    "execution":{"mode":"full"}
                }))
                .get("incremental_review")
                .is_none()
        );
        let pending = service.attach_automatic_incremental_review(json!({
            "execution":{"mode":"incremental","baseline":{"snapshot_id":"base"}}
        }));
        assert_eq!(pending["incremental_review"]["status"], "pending");
        let no_baseline = service.attach_automatic_incremental_review(json!({
            "execution":{"mode":"incremental"}
        }));
        assert_eq!(no_baseline["incremental_review"]["status"], "not_measured");
        let composite_no_baseline = service.attach_automatic_incremental_review(json!({
            "composite_snapshot_id":"missing-composite",
            "execution":{"mode":"incremental"},
            "terminal":true
        }));
        assert_eq!(
            composite_no_baseline["incremental_review"]["status"],
            "not_measured"
        );
        let composite_pending = service.attach_automatic_incremental_review(json!({
            "composite_snapshot_id":"missing-composite",
            "execution":{"mode":"incremental","baseline":{"composite_snapshot_id":"base"}},
            "terminal":false
        }));
        assert_eq!(composite_pending["incremental_review"]["status"], "pending");
        let composite_error = service.attach_automatic_incremental_review(json!({
            "composite_snapshot_id":"missing-composite",
            "execution":{"mode":"incremental","baseline":{"composite_snapshot_id":"base"}},
            "terminal":true
        }));
        assert_eq!(
            composite_error["incremental_review"]["status"],
            "not_measured"
        );
        assert!(service.validate_composite_context(&json!({})).is_err());
        for run in [
            json!({
                "execution":{"mode":"incremental","baseline":{"snapshot_id":"base"}},
                "terminal":true,
                "coverage_ingest":{"snapshot_ids":["one","two"]}
            }),
            json!({
                "execution":{"mode":"incremental","baseline":{"snapshot_id":"base"}},
                "terminal":true,
                "coverage_ingest":{"snapshot_ids":[7]}
            }),
            json!({
                "execution":{"mode":"incremental","baseline":{"snapshot_id":"missing"}},
                "terminal":true,
                "coverage_ingest":{"snapshot_ids":["missing"]}
            }),
        ] {
            let automatic = service.attach_automatic_incremental_review(run);
            assert_eq!(automatic["incremental_review"]["status"], "not_measured");
        }
        let incremental_input = outside.path().join("incremental-current.lcov");
        std::fs::write(
            &incremental_input,
            "TN:incremental\nSF:src/a.py\nDA:1,0\nDA:2,1\nend_of_record\n",
        )
        .expect("incremental input report");
        let incremental_command = service
            .command_registration(
                "service-automatic-incremental",
                "cp {{args}} incremental-output.lcov",
                true,
                "tester",
                "automatic incremental coverage review",
                Some(directory.path().to_str().unwrap()),
                "/bin/sh",
                Some(json!({
                    "coverage": {
                        "path": "incremental-output.lcov",
                        "required": true,
                        "coverage_format": "lcov",
                        "suite": "unit",
                        "detail_retention": "incremental_base"
                    }
                })),
                false,
            )
            .expect("incremental command");
        let incremental_execution = json!({"mode":"incremental","label":"case-specific"});
        let incremental_arguments = json!([incremental_input.to_string_lossy()]);
        let automatic_incremental = service
            .run_submission_with_execution(
                incremental_command["data"]["id"].as_str().unwrap(),
                None,
                Some("automatic-incremental-v1"),
                true,
                false,
                false,
                Some(&incremental_execution),
                Some(&incremental_arguments),
                Some(baseline["id"].as_str().unwrap()),
            )
            .expect("automatic incremental run");
        assert_eq!(
            automatic_incremental["data"]["execution"]["mode"],
            "incremental"
        );
        assert_eq!(
            automatic_incremental["data"]["execution"]["arguments"][0],
            incremental_input.to_string_lossy().as_ref()
        );
        assert_eq!(
            automatic_incremental["data"]["execution"]["baseline"]["snapshot_id"],
            baseline["id"]
        );
        assert_eq!(
            automatic_incremental["data"]["incremental_review"]["status"],
            "measured"
        );
        assert_eq!(
            automatic_incremental["data"]["incremental_review"]["automatic"],
            true
        );
        let automatic_status = service
            .run_review(
                automatic_incremental["data"]["id"].as_str().unwrap(),
                "status",
                None,
                "both",
                3,
                5,
                false,
                1_200,
                20_000,
            )
            .expect("automatic incremental status");
        assert_eq!(
            automatic_status["data"]["incremental_review"]["status"],
            "measured"
        );
        assert!(automatic_status["data"]["incremental_review"]["measurement"].is_object());
        assert!(automatic_status["data"]["incremental_review"]["baseline"].is_object());
        assert!(automatic_status["data"]["incremental_review"]["baseline_selector"].is_object());
        assert!(
            automatic_status["data"]["incremental_review"]["incremental"]
                .get("run")
                .is_some_and(Value::is_object)
        );
        assert!(
            automatic_status["data"]["incremental_review"]["incremental"]
                .get("aggregate")
                .is_none()
        );
        let current_id = many_current["id"].as_str().unwrap();
        let dashboard = service
            .dashboard(Some(current_id), Some(baseline["id"].as_str().unwrap()), 18)
            .expect("dashboard projection");
        assert_eq!(dashboard["data"]["incremental"]["status"], "measured");
        assert_eq!(
            dashboard["data"]["incremental"]["comparison_mode"],
            "additive_union"
        );
        assert_eq!(
            dashboard["data"]["incremental"]["coverage_gain"]["regressed"],
            0
        );
        assert!(dashboard["data"]["current"]["covered_lines"].is_number());
        assert!(dashboard["data"]["incremental"].get("aggregate").is_none());
        assert_eq!(dashboard["data"]["incremental"]["run"]["id"], current_id);
        assert_eq!(dashboard["data"]["files"]["source"], "current_union");
        let selected_dashboard_ids = vec![
            current_id.to_owned(),
            many_current["id"].clone().as_str().unwrap().to_owned(),
        ];
        let selected_dashboard = service
            .dashboard_with_snapshot_ids(
                Some(&selected_dashboard_ids),
                None,
                Some(baseline["id"].as_str().unwrap()),
                None,
                None,
                18,
            )
            .expect("dashboard union projection");
        assert_eq!(
            selected_dashboard["data"]["selection"]["current_snapshot_ids"],
            json!(selected_dashboard_ids)
        );
        assert_eq!(
            selected_dashboard["data"]["incremental"]["comparison_mode"],
            "additive_union"
        );
        assert!(
            service
                .dashboard_with_snapshot_ids(
                    Some(&selected_dashboard_ids),
                    Some(current_id),
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    None,
                    18,
                )
                .is_err()
        );
        let empty_dashboard_ids: &[String] = &[];
        assert!(
            service
                .dashboard_with_snapshot_ids(
                    Some(empty_dashboard_ids),
                    None,
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    None,
                    18,
                )
                .is_err()
        );
        let empty_snapshot_ids: &[String] = &[];
        assert!(
            service
                .coverage_review_with_snapshot_ids(
                    "incremental",
                    None,
                    Some(empty_snapshot_ids),
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    Some("unit"),
                    Some("main"),
                    None,
                    2,
                    2,
                    3,
                    5,
                    false,
                    3,
                    120,
                    600,
                    12_000,
                    "compact",
                    10,
                )
                .is_err()
        );
        assert!(
            service
                .coverage_review_with_snapshot_ids(
                    "incremental",
                    Some(current_id),
                    None,
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    Some("unit"),
                    Some("main"),
                    None,
                    2,
                    2,
                    3,
                    5,
                    false,
                    3,
                    120,
                    600,
                    12_000,
                    "compact",
                    10,
                )
                .is_ok()
        );
        assert!(dashboard["data"]["history"]["points"].is_array());
        assert!(dashboard["data"]["compaction"]["inventory"].is_object());
        let rolling_dashboard = service
            .dashboard(Some(current_id), None, 2)
            .expect("rolling dashboard projection");
        assert_eq!(
            rolling_dashboard["data"]["selection"]["baseline_kind"],
            "rolling_previous"
        );
        assert_eq!(rolling_dashboard["data"]["history"]["limit"], 2);
        let older_dashboard = service
            .dashboard(Some(baseline["id"].as_str().unwrap()), None, 2)
            .expect("older rolling dashboard projection");
        assert_eq!(
            older_dashboard["data"]["selection"]["baseline_kind"],
            "rolling_previous"
        );
        assert!(service.dashboard(Some(current_id), None, 1).is_err());

        let incompatible_report = directory.path().join("dashboard-cobertura.xml");
        std::fs::write(
            &incompatible_report,
            r#"<coverage><packages><package><classes><class filename="src/a.py"><lines><line number="1" hits="1"/></lines></class></classes></package></packages></coverage>"#,
        )
        .expect("incompatible dashboard report");
        let incompatible = store
            .ingest_report(
                &incompatible_report,
                "cobertura",
                Some(directory.path()),
                Some("main"),
                Some("incompatible"),
                None,
                "unit",
            )
            .expect("incompatible dashboard snapshot");
        let unmeasured_dashboard = service
            .dashboard(
                Some(incompatible["id"].as_str().unwrap()),
                Some(baseline["id"].as_str().unwrap()),
                18,
            )
            .expect("unmeasured dashboard projection");
        assert_eq!(
            unmeasured_dashboard["data"]["incremental"]["status"],
            "unmeasured"
        );
        assert_eq!(
            unmeasured_dashboard["data"]["files"]["source"],
            "current_snapshot"
        );
        for skip in 0..=24 {
            store.inject_query_fault_after(skip);
            let _ = service.dashboard(Some(current_id), Some(baseline["id"].as_str().unwrap()), 18);
        }
        for skip in 0..=24 {
            store.inject_query_fault_after(skip);
            let _ = service.dashboard(Some(current_id), None, 18);
        }
        for skip in 0..=24 {
            store.inject_query_fault_after(skip);
            let _ = service.dashboard(
                Some(incompatible["id"].as_str().unwrap()),
                Some(baseline["id"].as_str().unwrap()),
                18,
            );
        }
        store.clear_query_fault();

        store
            .execute_sql_for_test(&format!(
                "INSERT INTO composite_snapshots (id, created_at, run_id, command_id, repo_path, repo_key, status, mapping_version, inventory_hash, checkout_revision, checkout_dirty, covered_regions, total_regions, region_rate, reasons, remediation, inventory, provenance, components, component_evidence) VALUES ('svc-composite-base', current_timestamp - INTERVAL '2 seconds', 'svc-composite-run-base', 'dashboard-command', '{dashboard_repo_path}', '{dashboard_repo_key}', 'complete', 'coverage-mcp-regions-v1', 'svc-inventory', NULL, false, 0, 1, 0.0, '[]', '{{}}', '{{}}', '{{}}', '[]', '[]'); INSERT INTO composite_snapshots (id, created_at, run_id, command_id, repo_path, repo_key, status, mapping_version, inventory_hash, checkout_revision, checkout_dirty, covered_regions, total_regions, region_rate, reasons, remediation, inventory, provenance, components, component_evidence) VALUES ('svc-composite-current', current_timestamp, 'svc-composite-run-current', 'dashboard-command', '{dashboard_repo_path}', '{dashboard_repo_key}', 'complete', 'coverage-mcp-regions-v1', 'svc-inventory', NULL, false, 1, 1, 1.0, '[]', '{{}}', '{{}}', '{{}}', '[]', '[]'); INSERT INTO composite_regions (composite_snapshot_id, region_key, region) VALUES ('svc-composite-base', 'python:src/a.py:1:1:line-1', '{{\"region_key\":\"python:src/a.py:1:1:line-1\",\"component_id\":\"python\",\"logical_source_id\":\"python:src/a.py\",\"path\":\"src/a.py\",\"start_line\":1,\"end_line\":1,\"discriminator\":\"line-1\",\"state\":\"uncovered\",\"variants\":[{{\"package_variant\":\"cpython\",\"state\":\"uncovered\",\"hits\":0}}]}}'); INSERT INTO composite_regions (composite_snapshot_id, region_key, region) VALUES ('svc-composite-current', 'python:src/a.py:1:1:line-1', '{{\"region_key\":\"python:src/a.py:1:1:line-1\",\"component_id\":\"python\",\"logical_source_id\":\"python:src/a.py\",\"path\":\"src/a.py\",\"start_line\":1,\"end_line\":1,\"discriminator\":\"line-1\",\"state\":\"covered\",\"variants\":[{{\"package_variant\":\"cpython\",\"state\":\"covered\",\"hits\":1}}]}}')"
            ))
            .expect("service composite fixtures");
        assert!(
            service
                .composite_review(
                    "incremental",
                    "svc-composite-current",
                    Some("svc-composite-base"),
                    None,
                    24,
                    600,
                    12_000,
                    "compact",
                    10,
                )
                .is_ok()
        );
        assert!(
            service
                .composite_review(
                    "audit",
                    "svc-composite-current",
                    None,
                    None,
                    24,
                    600,
                    12_000,
                    "audit",
                    10,
                )
                .is_ok()
        );
        for skip in 0..=12 {
            store.inject_query_fault_after(skip);
            let _ = service.composite_review(
                "incremental",
                "svc-composite-current",
                Some("svc-composite-base"),
                None,
                24,
                600,
                12_000,
                "compact",
                10,
            );
        }
        for skip in 0..=6 {
            store.inject_query_fault_after(skip);
            let _ = service.composite_review(
                "audit",
                "svc-composite-current",
                None,
                None,
                24,
                600,
                12_000,
                "audit",
                10,
            );
        }
        store.clear_query_fault();
        store
            .execute_sql_for_test(
                "UPDATE composite_snapshots SET repo_key = 'foreign-composite' WHERE id = 'svc-composite-base'",
            )
            .expect("foreign composite fixture");
        assert!(
            service
                .composite_review(
                    "incremental",
                    "svc-composite-current",
                    Some("svc-composite-base"),
                    None,
                    24,
                    600,
                    12_000,
                    "compact",
                    10,
                )
                .is_err()
        );
        store
            .execute_sql_for_test(&format!(
                "UPDATE composite_snapshots SET repo_key = '{dashboard_repo_key}', mapping_version = 'different-mapping' WHERE id = 'svc-composite-base'"
            ))
            .expect("incompatible composite fixture");
        assert!(
            service
                .composite_review(
                    "incremental",
                    "svc-composite-current",
                    Some("svc-composite-base"),
                    None,
                    24,
                    600,
                    12_000,
                    "compact",
                    10,
                )
                .is_err()
        );
        store
            .execute_sql_for_test(
                "UPDATE composite_snapshots SET mapping_version = 'coverage-mcp-regions-v1' WHERE id = 'svc-composite-base'",
            )
            .expect("restore composite mapping");
        assert!(
            service
                .composite_review(
                    "audit",
                    "missing-composite",
                    None,
                    None,
                    24,
                    600,
                    12_000,
                    "audit",
                    10,
                )
                .is_err()
        );
        FORCE_BUDGET_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            service
                .composite_review(
                    "audit",
                    "svc-composite-current",
                    None,
                    None,
                    24,
                    600,
                    12_000,
                    "audit",
                    10,
                )
                .is_err()
        );
        assert!(
            service
                .composite_source_review(
                    "svc-composite-current",
                    vec![("src/a.py".to_owned(), 1, 2)],
                    120,
                    49,
                    12_000,
                )
                .is_err()
        );
        assert!(
            service
                .composite_source_review(
                    "svc-composite-current",
                    vec![("src/a.py".to_owned(), 1, 2)],
                    120,
                    600,
                    999,
                )
                .is_err()
        );
        assert!(
            service
                .composite_source_review(
                    "missing-composite",
                    vec![("src/a.py".to_owned(), 1, 2)],
                    120,
                    600,
                    12_000,
                )
                .is_err()
        );
        assert!(
            service
                .composite_source_review(
                    "svc-composite-current",
                    vec![("../outside.py".to_owned(), 1, 2)],
                    120,
                    600,
                    12_000,
                )
                .is_err()
        );
        assert!(
            service
                .composite_source_review(
                    "svc-composite-current",
                    vec![("missing.py".to_owned(), 1, 2)],
                    120,
                    600,
                    12_000,
                )
                .is_err()
        );
        for skip in 0..=8 {
            store.inject_query_fault_after(skip);
            let _ = service.composite_source_review(
                "svc-composite-current",
                vec![("src/a.py".to_owned(), 1, 2)],
                120,
                600,
                12_000,
            );
        }
        store.clear_query_fault();
        FORCE_BUDGET_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            service
                .composite_source_review(
                    "svc-composite-current",
                    vec![("src/a.py".to_owned(), 1, 2)],
                    120,
                    600,
                    12_000,
                )
                .is_err()
        );

        let empty_store = CoverageStore::open(
            outside.path().join("empty-dashboard.duckdb"),
            config.clone(),
        )
        .expect("empty dashboard store");
        let empty_project = empty_store
            .ensure_project(outside.path())
            .expect("empty dashboard project");
        let empty_service = CoverageService::new(
            empty_store.clone(),
            RequestContext {
                repo_key: empty_project.repo_key,
                checkout_path: empty_project.repo_path,
                suite: None,
            },
        );
        let empty_dashboard = empty_service
            .dashboard(None, None, 2)
            .expect("empty dashboard projection");
        assert_eq!(
            empty_dashboard["data"]["selection"]["baseline_kind"],
            "none"
        );
        assert_eq!(
            empty_dashboard["data"]["incremental"]["status"],
            "unmeasured"
        );
        assert_eq!(empty_dashboard["data"]["files"]["total"], 0);
        empty_store.close().expect("close empty dashboard store");

        std::fs::write(
            outside.path().join("foreign.lcov"),
            "TN:foreign\nSF:src/a.py\nDA:1,1\nend_of_record\n",
        )
        .expect("foreign dashboard report");
        let foreign = store
            .ingest_report(
                &outside.path().join("foreign.lcov"),
                "lcov",
                Some(outside.path()),
                Some("main"),
                Some("foreign"),
                None,
                "foreign",
            )
            .expect("foreign dashboard snapshot");
        let foreign_dashboard_ids = vec![
            current_id.to_owned(),
            foreign["id"].as_str().unwrap().to_owned(),
        ];
        assert!(
            service
                .dashboard_with_snapshot_ids(
                    Some(&foreign_dashboard_ids),
                    None,
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    None,
                    18,
                )
                .is_err()
        );
        assert!(
            service
                .dashboard(Some(foreign["id"].as_str().unwrap()), None, 18)
                .is_err()
        );
        store.inject_query_fault();
        assert!(service.dashboard(Some(current_id), None, 18).is_err());
        store.clear_query_fault();
        assert!(
            service
                .dashboard(Some("missing-dashboard-snapshot"), None, 18)
                .is_err()
        );
        let selected_missing_id = vec![
            current_id.to_owned(),
            "missing-dashboard-snapshot".to_owned(),
        ];
        assert!(
            service
                .dashboard_with_snapshot_ids(
                    Some(&selected_missing_id),
                    None,
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    None,
                    18,
                )
                .is_err()
        );
        assert!(
            service
                .dashboard(Some(current_id), Some(foreign["id"].as_str().unwrap()), 18,)
                .is_err()
        );
        assert!(
            service
                .coverage_review_with_max_test_ids(
                    "incremental",
                    Some(current_id),
                    None,
                    None,
                    Some("unit"),
                    Some("main"),
                    None,
                    2,
                    2,
                    3,
                    5,
                    false,
                    3,
                    120,
                    600,
                    12_000,
                    "compact",
                    10,
                )
                .is_err()
        );
        let measured = service
            .coverage_review_with_max_test_ids(
                "incremental",
                Some(current_id),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                2,
                3,
                5,
                false,
                3,
                120,
                600,
                12_000,
                "compact",
                10,
            )
            .expect("measured incremental response");
        assert_eq!(measured["data"]["incremental"]["status"], "measured");
        assert_eq!(
            measured["data"]["incremental"]["test_attribution"]["status"],
            "measured"
        );
        assert!(
            service
                .coverage_review_with_snapshot_ids(
                    "change",
                    Some("missing-scalar-review-snapshot"),
                    None,
                    None,
                    None,
                    Some("unit"),
                    Some("main"),
                    None,
                    2,
                    2,
                    3,
                    5,
                    false,
                    3,
                    120,
                    600,
                    12_000,
                    "compact",
                    10,
                )
                .is_err()
        );
        let mut compact_projection = json!({"changed_lines":[{"line":1}]});
        remove_incremental_changed_lines(&mut compact_projection);
        assert!(compact_projection.get("changed_lines").is_none());
        let mut non_object_projection = json!(true);
        remove_incremental_changed_lines(&mut non_object_projection);
        let review_projection = service
            .coverage_review_with_max_test_ids(
                "incremental",
                Some(current_id),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                2,
                3,
                5,
                false,
                3,
                120,
                1200,
                12_000,
                "review",
                10,
            )
            .expect("review incremental response");
        assert_eq!(
            review_projection["data"]["incremental"]["status"],
            "measured"
        );
        store.inject_query_fault_after(1);
        assert!(
            service
                .coverage_review_with_max_test_ids(
                    "incremental",
                    Some(current_id),
                    Some(baseline["id"].as_str().unwrap()),
                    None,
                    Some("unit"),
                    Some("main"),
                    None,
                    2,
                    2,
                    3,
                    5,
                    false,
                    3,
                    120,
                    600,
                    12_000,
                    "compact",
                    10,
                )
                .is_err()
        );
        let _ = service
            .coverage_review_with_max_test_ids(
                "incremental",
                Some(current_id),
                Some(baseline["id"].as_str().unwrap()),
                None,
                Some("unit"),
                Some("main"),
                None,
                2,
                2,
                3,
                5,
                false,
                3,
                120,
                50,
                12_000,
                "compact",
                10,
            )
            .expect_err("small word budget should reject the measured response");
        assert!(
            service
                .coverage_review_with_max_test_ids(
                    "incremental",
                    Some(current_id),
                    Some(current_id),
                    None,
                    Some("unit"),
                    Some("main"),
                    None,
                    2,
                    2,
                    3,
                    5,
                    false,
                    3,
                    120,
                    600,
                    12_000,
                    "compact",
                    0,
                )
                .is_err()
        );
        let empty_service = CoverageService::new(
            store.clone(),
            RequestContext {
                repo_key: service.context(None).repo_key.clone(),
                checkout_path: service.context(None).checkout_path.clone(),
                suite: None,
            },
        );
        let no_measurement = empty_service
            .coverage_review_with_max_test_ids(
                "incremental",
                None,
                Some(current_id),
                None,
                Some("suite-without-a-snapshot"),
                Some("main"),
                None,
                2,
                2,
                3,
                5,
                false,
                3,
                120,
                600,
                12_000,
                "compact",
                10,
            )
            .expect("incremental no-measurement response");
        assert_eq!(no_measurement["data"]["claim_status"], "not_measured");
        let wordy_suite = "word ".repeat(100);
        FORCE_BUDGET_FAILURE.store(true, Ordering::SeqCst);
        assert!(
            empty_service
                .coverage_review_with_max_test_ids(
                    "incremental",
                    None,
                    Some(current_id),
                    None,
                    Some(&wordy_suite),
                    Some("main"),
                    None,
                    2,
                    2,
                    3,
                    5,
                    false,
                    3,
                    120,
                    50,
                    12_000,
                    "compact",
                    10,
                )
                .is_err()
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
        assert!(service.project_context(None, 49, false).is_err());
        assert!(service.project_context(None, 50, true).is_err());
        let compact_context = service
            .project_context(None, 600, false)
            .expect("compact project context");
        assert!(serialized_word_count(&compact_context["data"]) <= 600);
        assert!(
            compact_context["page"]["response_word_count"]
                .as_u64()
                .is_some_and(|count| count <= 600)
        );
        assert!(
            compact_context["page"]["returned"]
                .as_u64()
                .is_some_and(|count| count <= MAX_CONTEXT_COMMANDS as u64)
        );
        let capped_values = (0..=MAX_CONTEXT_COMMANDS)
            .map(|index| json!({"id":index,"value":"command"}))
            .collect::<Vec<_>>();
        let (capped_page_values, capped_page) = service
            .page_with_item_limit(
                &capped_values,
                None,
                600,
                "capped-page",
                None,
                Some(MAX_CONTEXT_COMMANDS),
            )
            .expect("capped page");
        assert_eq!(capped_page_values.len(), MAX_CONTEXT_COMMANDS);
        assert!(capped_page["truncated"].as_bool().unwrap());
        assert!(capped_page["next_cursor"].is_string());
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
        let compact_insight = service
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
                "compact",
            )
            .expect("compact insight review");
        let compact_items = compact_insight["data"]["insight"]["items"]
            .as_array()
            .expect("compact insight items");
        assert!(!compact_items.is_empty());
        assert!(compact_items.iter().all(|item| {
            item["regions"]
                .as_array()
                .is_some_and(|regions| regions.len() <= MAX_COMPACT_INSIGHT_REGIONS)
                && item["region_count"].is_number()
                && item["regions_truncated"].is_boolean()
        }));
        let audit_insight = service
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
                1200,
                20_000,
                "audit",
            )
            .expect("audit insight review");
        let audit_items = audit_insight["data"]["insight"]["items"]
            .as_array()
            .expect("audit insight items");
        assert!(!audit_items.is_empty());
        assert!(audit_items[0]["regions"][0]["start"].is_number());
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
        assert!(terminal["data"].get("coverage_review").is_none());
        assert!(
            service
                .run_state(terminal["data"]["id"].as_str().unwrap(), "status", false)
                .is_ok()
        );
        let terminal_status = service
            .run_state(terminal["data"]["id"].as_str().unwrap(), "status", false)
            .expect("terminal service status");
        assert!(terminal_status["data"].get("coverage_review").is_none());
        let compact_terminal_status = service
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
            .expect("compact terminal status fits the byte budget");
        assert!(
            compact_terminal_status["data"]
                .get("coverage_review")
                .is_none()
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
        assert!(
            active_context["data"]["commands"]
                .as_array()
                .expect("project commands")
                .iter()
                .all(|command| command.get("command").is_none() && command.get("cwd").is_none())
        );
        assert!(
            !active_context["data"]["commands"]
                .as_array()
                .expect("project commands")
                .is_empty()
        );
        assert!(
            service
                .project_context(Some("invalid"), 600, false)
                .is_err()
        );
        assert_eq!(active_context["data"]["active_runs_truncated"], false);
        let reserved_words = active_context["page"]["reserved_words"]
            .as_u64()
            .expect("reserved project context words") as usize;
        assert!(reserved_words < 4_951);
        assert!(
            service
                .project_context(None, reserved_words + 49, false)
                .is_err()
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

        std::fs::write(
            &report,
            "TN:duplicate-a\nSF:src/a.py\nDA:1,1\nend_of_record\nTN:duplicate-b\nSF:src/a.py\nDA:1,9\nend_of_record\nTN:duplicate-c\nSF:src/a.py\nDA:2,1\nend_of_record\nTN:duplicate-d\nSF:src/a.py\nDA:2,9\nend_of_record\n",
        )
        .expect("duplicate report");
        let duplicate_snapshot = store
            .ingest_report(
                &report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .expect("duplicate snapshot");
        let duplicate_id = duplicate_snapshot["id"].as_str().unwrap();
        assert!(
            service
                .find_duplicate_coverage_tests(None, None, None, 0, 10, 600, 12_000)
                .is_err()
        );
        assert!(
            service
                .find_duplicate_coverage_tests(None, None, None, 10, 1, 600, 12_000)
                .is_err()
        );
        assert!(
            service
                .find_duplicate_coverage_tests(None, None, None, 10, 10, 600, 999)
                .is_err()
        );
        assert!(
            service
                .find_duplicate_coverage_tests(None, None, None, 10, 10, 49, 12_000)
                .is_err()
        );
        assert!(
            service
                .find_duplicate_coverage_tests(
                    Some("missing-snapshot"),
                    Some("unit"),
                    None,
                    10,
                    10,
                    600,
                    12_000,
                )
                .is_err()
        );
        let latest_duplicates = service
            .find_duplicate_coverage_tests(None, Some("unit"), None, 10, 10, 600, 12_000)
            .expect("latest duplicate snapshot");
        assert_eq!(latest_duplicates["data"]["status"], "measured");
        assert!(
            service
                .find_duplicate_coverage_tests(
                    None,
                    Some("missing-suite"),
                    None,
                    10,
                    10,
                    600,
                    12_000,
                )
                .is_err()
        );
        assert!(
            service
                .find_duplicate_coverage_tests(
                    Some(duplicate_id),
                    Some("other"),
                    None,
                    10,
                    10,
                    600,
                    12_000,
                )
                .is_err()
        );
        let first_duplicate_page = service
            .find_duplicate_coverage_tests(
                Some(duplicate_id),
                Some("unit"),
                None,
                1,
                10,
                600,
                12_000,
            )
            .expect("first duplicate service page");
        let duplicate_cursor = first_duplicate_page["page"]["next_cursor"]
            .as_str()
            .expect("duplicate cursor");
        assert!(
            service
                .find_duplicate_coverage_tests(
                    Some(duplicate_id),
                    Some("unit"),
                    Some(duplicate_cursor),
                    1,
                    10,
                    600,
                    12_000,
                )
                .is_ok()
        );
        let duplicate_scope = format!("duplicate-coverage:{duplicate_id}:unit:10");
        let wrong_duplicate_cursor =
            encode_cursor(&"0".repeat(64), &duplicate_scope, 2).expect("wrong duplicate cursor");
        assert!(
            service
                .find_duplicate_coverage_tests(
                    Some(duplicate_id),
                    Some("unit"),
                    Some(&wrong_duplicate_cursor),
                    1,
                    10,
                    600,
                    12_000,
                )
                .is_err()
        );
        assert!(
            service
                .find_duplicate_coverage_tests(
                    Some(duplicate_id),
                    Some("unit"),
                    Some("invalid"),
                    1,
                    10,
                    600,
                    12_000,
                )
                .is_err()
        );
        store.inject_query_fault_after(2);
        assert!(
            service
                .find_duplicate_coverage_tests(
                    Some(duplicate_id),
                    Some("unit"),
                    None,
                    1,
                    10,
                    600,
                    12_000,
                )
                .is_err()
        );
        store.clear_query_fault();

        let long_test_name = (0..80)
            .map(|index| format!("word{index}"))
            .collect::<Vec<_>>()
            .join(" ");
        let long_test_name_two = format!("{long_test_name} second");
        let budget_report = directory.path().join("duplicate-budget.lcov");
        std::fs::write(
            &budget_report,
            format!(
                "TN:{long_test_name}\nSF:src/a.py\nDA:1,1\nend_of_record\nTN:{long_test_name_two}\nSF:src/a.py\nDA:1,9\nend_of_record\n"
            ),
        )
        .expect("duplicate budget report");
        let budget_snapshot = store
            .ingest_report(
                &budget_report,
                "lcov",
                Some(directory.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .expect("duplicate budget snapshot");
        assert!(
            service
                .find_duplicate_coverage_tests(
                    Some(budget_snapshot["id"].as_str().unwrap()),
                    Some("unit"),
                    None,
                    10,
                    10,
                    50,
                    12_000,
                )
                .is_err()
        );
        assert!(
            service
                .find_duplicate_coverage_tests(
                    Some(budget_snapshot["id"].as_str().unwrap()),
                    Some("unit"),
                    None,
                    10,
                    10,
                    600,
                    1_000,
                )
                .is_err()
        );

        std::fs::write(
            outside.path().join("report.lcov"),
            "TN:foreign\nSF:src/a.py\nDA:1,1\nend_of_record\n",
        )
        .expect("foreign duplicate report");
        let foreign_snapshot = store
            .ingest_report(
                &outside.path().join("report.lcov"),
                "lcov",
                Some(outside.path()),
                Some("main"),
                None,
                None,
                "unit",
            )
            .expect("foreign snapshot");
        assert!(
            service
                .find_duplicate_coverage_tests(
                    Some(foreign_snapshot["id"].as_str().unwrap()),
                    Some("unit"),
                    None,
                    10,
                    10,
                    600,
                    12_000,
                )
                .is_err()
        );

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
        let project = compact_context_project(
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
        let execution_snapshot = compact_snapshot(
            &json!({
                "repo_path":"/repo",
                "metadata":{"execution":{"mode":"incremental","identity":"case:v1"}}
            }),
            false,
        );
        assert_eq!(execution_snapshot["execution"]["identity"], "case:v1");
        assert_eq!(
            incremental_next_action(&json!({"newly_covered": 1}))["kind"],
            "review_incremental_gain"
        );
        assert_eq!(
            incremental_next_action(&json!({"regressed": 1}))["kind"],
            "inspect_regression"
        );
        let mut null_incremental_diff = Value::Null;
        compact_incremental_diff(&mut null_incremental_diff);
        let mut empty_incremental_diff = json!({});
        compact_incremental_diff(&mut empty_incremental_diff);
        let mut sparse_incremental_diff = json!({"metric_deltas":{}});
        compact_incremental_diff(&mut sparse_incremental_diff);
        assert_eq!(compact_incremental_rate_deltas(&json!({})), json!({}));
        assert_eq!(
            dashboard_incremental_files(&json!({"comparison_mode":"snapshot_diff"}))["source"],
            "current_vs_baseline"
        );

        let file = compact_file(&json!({"file_path":"a.py","raw_metrics":null}), false);
        assert!(file.get("raw_metrics").is_none());
        let dashboard_files = dashboard_current_files(&[
            json!({"file_path":"z.py","line_rate":null}),
            json!({"file_path":"b.py","line_rate":0.5}),
            json!({"file_path":"a.py","line_rate":0.5}),
        ]);
        assert_eq!(dashboard_files["source"], "current_snapshot");
        assert_eq!(dashboard_files["total"], 3);
        assert_eq!(
            dashboard_incremental(&json!({"status":"measured"}))["regions"],
            json!([])
        );
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

        let context_project = compact_context_project(&json!({"id":"project-id","line_rate":0.8}));
        assert_eq!(context_project["id"], "project-id");
        assert!(context_project.get("total_lines").is_none());

        let command = compact_command(
            &json!({"id":"command-id","name":"unit","command":"cargo test","artifact_specs":[{"path":"coverage.lcov"}]}),
            false,
        );
        assert_eq!(command["id"], "command-id");
        assert_eq!(command["artifact_count"], 1);
        assert!(command.get("command").is_none());
        let detailed_command = compact_command(&json!({"approved_by":"human"}), true);
        assert_eq!(detailed_command["approved_by"], "human");

        assert_eq!(
            compact_file_change(&json!({"file_path":"a.py","line_rate_delta":0.1}))["file_path"],
            "a.py"
        );
        assert!(compact_file_change_token(&json!({"line_rate_delta":0.1})).is_none());
        assert_eq!(
            compact_history_snapshot(&json!({"id":"s","line_rate":0.8}), false)["id"],
            "s"
        );
        let history_value = json!({"id":"s","line_rate":0.8,"total_lines":10});
        assert!(
            compact_history_snapshot(&history_value, false)
                .get("total_lines")
                .is_none()
        );
        assert_eq!(
            compact_history_snapshot(&history_value, true)["total_lines"],
            10
        );
        let (regions, region_count, regions_truncated) = compact_insight_regions(&json!([
            {"start":1,"end":2,"line_count":2},
            {"start":4,"end":4},
            {"start":7,"end":7},
            {"start":9,"end":9}
        ]));
        assert_eq!(regions.len(), MAX_COMPACT_INSIGHT_REGIONS);
        assert_eq!(regions[1], json!([4, 4, 1]));
        assert_eq!(region_count, 4);
        assert!(regions_truncated);
        let (invalid_regions, invalid_count, invalid_truncated) = compact_insight_regions(&json!([
            {"end":2},
            {"start":2},
            {"start":3,"end":3}
        ]));
        assert_eq!(invalid_regions, vec![json!([3, 3, 1])]);
        assert_eq!(invalid_count, 3);
        assert!(!invalid_truncated);
        assert_eq!(
            compact_insight_regions(&json!(null)),
            (Vec::new(), 0, false)
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

        let run = json!({
            "command":"cargo test --all-targets",
            "cwd":"/repo",
            "repo_path":"/repo",
            "stdout_path":"/tmp/stdout",
            "stderr_path":"/tmp/stderr",
            "parsed_summary":{},
            "artifact_paths":[],
            "coverage_review":{},
            "status":"passed"
        });
        assert_eq!(compact_run_result(&run, true), run);
        let compact_run = compact_run_result(&run, false);
        assert!(compact_run.get("command").is_none());
        assert!(compact_run.get("cwd").is_none());
        assert!(compact_run.get("repo_path").is_none());
        assert!(compact_run.get("stdout_path").is_none());
        assert!(compact_run.get("parsed_summary").is_none());
        assert!(compact_run.get("coverage_review").is_none());
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
