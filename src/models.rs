use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Returns a ratio or null when the denominator is empty.
pub fn rate(covered: i64, total: i64) -> Option<f64> {
    (total > 0).then_some(covered as f64 / total as f64)
}

/// Normalizes a report path to the repository-relative spelling used by queries.
pub fn normalize_report_path(path: &str, repo_path: Option<&str>) -> String {
    let raw = PathBuf::from(path);
    if let Some(repo_path) = repo_path {
        if raw.is_absolute() {
            if let (Ok(file), Ok(repo)) = (raw.canonicalize(), Path::new(repo_path).canonicalize())
            {
                if let Ok(relative) = file.strip_prefix(repo) {
                    return relative.to_string_lossy().replace('\\', "/");
                }
            }
        }
    }
    raw.to_string_lossy().replace('\\', "/")
}

/// One normalized source-line measurement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LineCoverage {
    /// Repository-relative path.
    pub file_path: String,
    /// One-based line number.
    pub line_number: i64,
    /// Execution count.
    pub hits: i64,
    /// Whether the line is covered.
    pub covered: bool,
    /// Whether this row contributes to line totals.
    pub count_line: bool,
    /// Branch outcomes attached to the line.
    pub total_branches: i64,
    /// Covered branch outcomes attached to the line.
    pub covered_branches: i64,
    /// Functions attached to the line.
    pub total_functions: i64,
    /// Covered functions attached to the line.
    pub covered_functions: i64,
    /// Parser-specific detail.
    pub details: Value,
}

/// One covered source line attributed to a named test.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TestCoverageLine {
    /// Test name as recorded by the coverage format.
    pub test_name: String,
    /// Repository-relative path.
    pub file_path: String,
    /// One-based line number.
    pub line_number: i64,
}

impl LineCoverage {
    /// Merges duplicate records emitted by a coverage format.
    pub fn merge(&mut self, other: &Self) {
        if other.count_line {
            self.hits = self.hits.max(other.hits);
            self.covered |= other.covered;
        }
        self.count_line |= other.count_line;
        self.total_branches += other.total_branches;
        self.covered_branches += other.covered_branches;
        self.total_functions += other.total_functions;
        self.covered_functions += other.covered_functions;
        if let (Value::Object(current), Value::Object(extra)) = (&mut self.details, &other.details)
        {
            for (key, value) in extra {
                current.insert(key.clone(), value.clone());
            }
        }
    }
}

/// Per-file coverage totals.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileCoverage {
    /// Repository-relative path.
    pub file_path: String,
    /// Instrumented lines.
    pub total_lines: i64,
    /// Covered lines.
    pub covered_lines: i64,
    /// Instrumented branches.
    pub total_branches: i64,
    /// Covered branches.
    pub covered_branches: i64,
    /// Instrumented functions.
    pub total_functions: i64,
    /// Covered functions.
    pub covered_functions: i64,
    /// Instrumented regions.
    pub total_regions: i64,
    /// Covered regions.
    pub covered_regions: i64,
    /// Format-specific raw metrics.
    pub raw_metrics: Value,
}

impl FileCoverage {
    /// Line rate.
    pub fn line_rate(&self) -> Option<f64> {
        rate(self.covered_lines, self.total_lines)
    }
    /// Branch rate.
    pub fn branch_rate(&self) -> Option<f64> {
        rate(self.covered_branches, self.total_branches)
    }
    /// Function rate.
    pub fn function_rate(&self) -> Option<f64> {
        rate(self.covered_functions, self.total_functions)
    }
    /// Region rate.
    pub fn region_rate(&self) -> Option<f64> {
        rate(self.covered_regions, self.total_regions)
    }
}

/// A normalized coverage report before it is persisted as a snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoverageReport {
    /// Canonical parser format.
    pub format: String,
    /// Source artifact path.
    pub report_path: String,
    /// Normalized file rows.
    pub files: Vec<FileCoverage>,
    /// Normalized line rows.
    pub lines: Vec<LineCoverage>,
    /// Covered source lines attributed to named tests.
    pub test_coverage: Vec<TestCoverageLine>,
    /// Parser warnings.
    pub warnings: Vec<String>,
    /// Parser metadata.
    pub metadata: Value,
}

impl CoverageReport {
    /// Total lines.
    pub fn total_lines(&self) -> i64 {
        self.files.iter().map(|file| file.total_lines).sum()
    }
    /// Covered lines.
    pub fn covered_lines(&self) -> i64 {
        self.files.iter().map(|file| file.covered_lines).sum()
    }
    /// Total branches.
    pub fn total_branches(&self) -> i64 {
        self.files.iter().map(|file| file.total_branches).sum()
    }
    /// Covered branches.
    pub fn covered_branches(&self) -> i64 {
        self.files.iter().map(|file| file.covered_branches).sum()
    }
    /// Total functions.
    pub fn total_functions(&self) -> i64 {
        self.files.iter().map(|file| file.total_functions).sum()
    }
    /// Covered functions.
    pub fn covered_functions(&self) -> i64 {
        self.files.iter().map(|file| file.covered_functions).sum()
    }
    /// Total regions.
    pub fn total_regions(&self) -> i64 {
        self.files.iter().map(|file| file.total_regions).sum()
    }
    /// Covered regions.
    pub fn covered_regions(&self) -> i64 {
        self.files.iter().map(|file| file.covered_regions).sum()
    }
    /// Line rate.
    pub fn line_rate(&self) -> Option<f64> {
        rate(self.covered_lines(), self.total_lines())
    }
    /// Branch rate.
    pub fn branch_rate(&self) -> Option<f64> {
        rate(self.covered_branches(), self.total_branches())
    }
    /// Function rate.
    pub fn function_rate(&self) -> Option<f64> {
        rate(self.covered_functions(), self.total_functions())
    }
    /// Region rate.
    pub fn region_rate(&self) -> Option<f64> {
        rate(self.covered_regions(), self.total_regions())
    }
}

/// Accumulates format records into deterministic file and line rows.
#[derive(Debug)]
pub struct CoverageBuilder {
    repo_path: Option<String>,
    lines: BTreeMap<(String, i64), LineCoverage>,
    test_coverage: BTreeSet<(String, String, i64)>,
    file_metrics: BTreeMap<String, serde_json::Map<String, Value>>,
    normalized_paths: BTreeMap<String, String>,
}

impl CoverageBuilder {
    /// Creates a builder relative to an optional repository root.
    pub fn new(repo_path: Option<&str>) -> Self {
        Self {
            repo_path: repo_path.map(str::to_owned),
            lines: BTreeMap::new(),
            test_coverage: BTreeSet::new(),
            file_metrics: BTreeMap::new(),
            normalized_paths: BTreeMap::new(),
        }
    }

    /// Adds one line, branch, or function observation.
    #[allow(clippy::too_many_arguments)]
    pub fn add_line(
        &mut self,
        file_path: &str,
        line_number: i64,
        hits: i64,
        covered: Option<bool>,
        count_line: bool,
        total_branches: i64,
        covered_branches: i64,
        total_functions: i64,
        covered_functions: i64,
        details: Value,
    ) {
        if line_number <= 0 {
            return;
        }
        let normalized = self
            .normalized_paths
            .entry(file_path.to_owned())
            .or_insert_with(|| normalize_report_path(file_path, self.repo_path.as_deref()))
            .clone();
        let line_hits = if count_line { hits.max(0) } else { 0 };
        let is_covered = if count_line {
            covered.unwrap_or(line_hits > 0)
        } else {
            false
        };
        let row = LineCoverage {
            file_path: normalized.clone(),
            line_number,
            hits: line_hits,
            covered: is_covered,
            count_line,
            total_branches: total_branches.max(0),
            covered_branches: covered_branches.max(0),
            total_functions: total_functions.max(0),
            covered_functions: covered_functions.max(0),
            details,
        };
        if let Some(existing) = self.lines.get_mut(&(normalized, line_number)) {
            existing.merge(&row);
        } else {
            self.lines.insert((row.file_path.clone(), line_number), row);
        }
    }

    /// Adds one covered source line attributed to a named test.
    pub fn add_test_line(&mut self, test_name: &str, file_path: &str, line_number: i64) {
        let test_name = test_name.trim();
        if test_name.is_empty() || line_number <= 0 {
            return;
        }
        let normalized = self
            .normalized_paths
            .entry(file_path.to_owned())
            .or_insert_with(|| normalize_report_path(file_path, self.repo_path.as_deref()))
            .clone();
        self.test_coverage
            .insert((test_name.to_owned(), normalized, line_number));
    }

    /// Adds format-specific file metrics.
    pub fn add_file_metrics(&mut self, file_path: &str, metrics: serde_json::Map<String, Value>) {
        let normalized = self
            .normalized_paths
            .entry(file_path.to_owned())
            .or_insert_with(|| normalize_report_path(file_path, self.repo_path.as_deref()))
            .clone();
        self.file_metrics
            .entry(normalized)
            .or_default()
            .extend(metrics);
    }

    /// Finalizes deterministic file totals and a report.
    pub fn build(
        mut self,
        format: &str,
        report_path: &str,
        warnings: Vec<String>,
        metadata: Value,
    ) -> CoverageReport {
        let lines: Vec<LineCoverage> = self.lines.into_values().collect();
        let test_coverage = self
            .test_coverage
            .into_iter()
            .map(|(test_name, file_path, line_number)| TestCoverageLine {
                test_name,
                file_path,
                line_number,
            })
            .collect();
        let mut by_file: BTreeMap<String, Vec<LineCoverage>> = BTreeMap::new();
        for line in &lines {
            by_file
                .entry(line.file_path.clone())
                .or_default()
                .push(line.clone());
        }
        let mut files = Vec::new();
        let mut paths: std::collections::BTreeSet<String> = by_file.keys().cloned().collect();
        paths.extend(self.file_metrics.keys().cloned());
        for file_path in paths {
            let file_lines = by_file.get(&file_path).cloned().unwrap_or_default();
            let mut metrics = self.file_metrics.remove(&file_path).unwrap_or_default();
            let total_lines = metric_i64(
                &mut metrics,
                "total_lines",
                file_lines.iter().filter(|line| line.count_line).count() as i64,
            );
            let covered_lines = metric_i64(
                &mut metrics,
                "covered_lines",
                file_lines
                    .iter()
                    .filter(|line| line.count_line && line.covered)
                    .count() as i64,
            );
            let total_branches = metric_i64(
                &mut metrics,
                "total_branches",
                file_lines.iter().map(|line| line.total_branches).sum(),
            );
            let covered_branches = metric_i64(
                &mut metrics,
                "covered_branches",
                file_lines.iter().map(|line| line.covered_branches).sum(),
            );
            let total_functions = metric_i64(
                &mut metrics,
                "total_functions",
                file_lines.iter().map(|line| line.total_functions).sum(),
            );
            let covered_functions = metric_i64(
                &mut metrics,
                "covered_functions",
                file_lines.iter().map(|line| line.covered_functions).sum(),
            );
            let total_regions = metric_i64(&mut metrics, "total_regions", 0);
            let covered_regions = metric_i64(&mut metrics, "covered_regions", 0);
            files.push(FileCoverage {
                file_path,
                total_lines,
                covered_lines,
                total_branches,
                covered_branches,
                total_functions,
                covered_functions,
                total_regions,
                covered_regions,
                raw_metrics: Value::Object(metrics),
            });
        }
        CoverageReport {
            format: format.to_owned(),
            report_path: report_path.to_owned(),
            files,
            lines,
            test_coverage,
            warnings,
            metadata,
        }
    }
}

fn metric_i64(metrics: &mut serde_json::Map<String, Value>, key: &str, fallback: i64) -> i64 {
    metrics
        .remove(key)
        .and_then(|value| value.as_i64())
        .unwrap_or(fallback)
        .max(0)
}
