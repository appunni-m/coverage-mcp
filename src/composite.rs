//! Composite coverage contracts and deterministic aggregation.
//!
//! A composite snapshot is an immutable, run-scoped view over heterogeneous
//! evidence.  The inventory owns the denominator and explicit logical-source
//! identifiers own deduplication; filenames, package names, and matching
//! bytes are never used as implicit aliases.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};
use crate::models::normalize_report_path;
use crate::parser::parse_coverage_report;

/// Versioned authoritative production inventory format.
pub const INVENTORY_SCHEMA: &str = "coverage-mcp-inventory-v1";
/// Versioned canonical region artifact format.
pub const REGIONS_SCHEMA: &str = "coverage-mcp-regions-v1";
/// Versioned mapping used by every composite snapshot.
pub const MAPPING_VERSION: &str = "coverage-mcp-regions-v1";
/// Format name used by an inventory artifact descriptor.
pub const INVENTORY_FORMAT: &str = "coverage-mcp-inventory-v1";

/// Returns the stable spelling used by composite format comparisons.
pub fn normalize_composite_format(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "coverage.py" | "coveragepy" | "python" => "coveragepy".to_owned(),
        "istanbul" | "javascript" | "js" => "istanbul".to_owned(),
        "llvm" | "llvm-json" => "llvm".to_owned(),
        "coverage-mcp-regions-v1" => REGIONS_SCHEMA.to_owned(),
        "coverage-mcp-inventory-v1" => INVENTORY_FORMAT.to_owned(),
        other => other.to_owned(),
    }
}

/// One explicit mapping from an evidence path to an inventory source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LogicalSourceAlias {
    /// Evidence path as reported by the coverage producer.
    pub path: String,
    /// Authoritative logical source identifier from the inventory.
    pub logical_source_id: String,
}

/// Metadata attached to one composite artifact descriptor.
#[derive(Clone, Debug, Serialize)]
pub struct CompositeArtifactDescriptor {
    /// Component family, for example `rust`, `python`, or `javascript`.
    pub component_id: String,
    /// Package or backend variant, for example `cpu`, `simd`, or `gpu`.
    pub package_variant: String,
    /// Canonical language family.
    pub language: String,
    /// Coverage format declared by the artifact specification.
    pub format: String,
    /// Repository-relative inventory artifact path.
    pub inventory_artifact: String,
    /// Explicit path-to-logical-source aliases.
    pub logical_source_aliases: Vec<LogicalSourceAlias>,
    /// Inventory or coverage role.
    pub role: String,
    /// Toolchain versions used to produce the artifact.
    pub toolchain_versions: Value,
}

/// One expected canonical region from the authoritative inventory.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct InventoryRegion {
    /// One-based inclusive start line.
    pub start_line: i64,
    /// One-based inclusive end line.
    pub end_line: i64,
    /// Stable region discriminator within one logical source.
    pub discriminator: String,
}

impl InventoryRegion {
    fn key(&self, logical_source_id: &str) -> String {
        region_key(
            logical_source_id,
            self.start_line,
            self.end_line,
            &self.discriminator,
        )
    }
}

/// One production or catalogue entry in the inventory.
#[derive(Clone, Debug, Serialize)]
pub struct InventoryEntry {
    /// Explicit source identity used for deduplication.
    pub logical_source_id: String,
    /// Repository-relative source path.
    pub path: String,
    /// Owning package or component.
    pub package: String,
    /// Optional explicit component family; defaults to package.
    pub component_id: String,
    /// Language family.
    pub language: String,
    /// `production` entries contribute to the denominator.
    pub role: String,
    /// SHA-256 source hash recorded by the inventory producer.
    pub source_hash: String,
    /// Required package/backend variants.
    pub variants: Vec<String>,
    /// Coverage formats accepted for this source.
    pub expected_formats: Vec<String>,
    /// Canonical regions owned by the source.
    pub regions: Vec<InventoryRegion>,
}

/// Parsed authoritative inventory.
#[derive(Clone, Debug, Serialize)]
pub struct Inventory {
    /// Inventory schema identifier.
    pub schema: String,
    /// Region mapping version.
    pub mapping_version: String,
    /// Checkout revision used to generate the inventory.
    pub source_revision: String,
    /// Inventory entries, including non-production catalogue entries.
    pub entries: Vec<InventoryEntry>,
}

/// One normalized observed canonical region.
#[derive(Clone, Debug, Serialize)]
pub struct CanonicalRegion {
    /// Evidence path before alias resolution.
    pub path: String,
    /// Optional explicit logical source identity carried by the artifact.
    pub logical_source_id: Option<String>,
    /// One-based inclusive start line.
    pub start_line: i64,
    /// One-based inclusive end line.
    pub end_line: i64,
    /// Stable region discriminator.
    pub discriminator: String,
    /// Execution count.
    pub hits: i64,
    /// Whether this region was covered.
    pub covered: bool,
    /// Optional producer-recorded source hash.
    pub source_hash: Option<String>,
}

impl CanonicalRegion {
    fn key(&self, logical_source_id: &str) -> String {
        region_key(
            logical_source_id,
            self.start_line,
            self.end_line,
            &self.discriminator,
        )
    }
}

/// Coverage evidence supplied to the composite assembler.
#[derive(Clone, Debug)]
pub struct CompositeComponentInput {
    /// Validated descriptor metadata.
    pub descriptor: CompositeArtifactDescriptor,
    /// Absolute artifact path.
    pub artifact_path: PathBuf,
    /// Artifact content hash when available.
    pub artifact_hash: Option<String>,
    /// Child immutable snapshot ID when the standard parser ingested one.
    pub snapshot_id: Option<String>,
    /// Source revision declared by a versioned canonical region artifact.
    pub source_revision: Option<String>,
    /// Run-level artifact status (`ingested`, `missing`, `skipped_stale`, etc.).
    pub artifact_status: String,
    /// Snapshot ID whose evidence was reused when an incremental run did not
    /// emit a fresh artifact for this component.
    pub reused_from_snapshot_id: Option<String>,
    /// Normalized canonical region observations.
    pub regions: Vec<CanonicalRegion>,
}

/// Result of one composite assembly pass.
#[derive(Clone, Debug, Serialize)]
pub struct CompositeBuild {
    /// `complete` only when all required evidence and regions are valid.
    pub status: String,
    /// Exact covered canonical region count.
    pub covered_regions: i64,
    /// Exact inventory-owned canonical region denominator.
    pub total_regions: i64,
    /// Stable aggregate component summaries.
    pub components: Vec<Value>,
    /// Blocking and remediation reasons.
    pub reasons: Vec<String>,
    /// Counted remediation buckets.
    pub remediation: Value,
    /// Per-component evidence and provenance rows.
    pub component_evidence: Vec<Value>,
    /// Bounded-by-storage canonical region rows.
    pub regions: Vec<Value>,
    /// Inventory summary suitable for a public response.
    pub inventory: Value,
    /// Mapping and source provenance.
    pub provenance: Value,
}

/// Creates a durable incomplete result when an inventory cannot be parsed.
pub fn incomplete_build(
    reason: impl Into<String>,
    inventory: Value,
    provenance: Value,
) -> CompositeBuild {
    CompositeBuild {
        status: "incomplete".to_owned(),
        covered_regions: 0,
        total_regions: 0,
        components: ["rust", "python", "javascript"]
            .into_iter()
            .map(|component| {
                json!({
                    "component": component,
                    "covered_regions": 0,
                    "total_regions": 0,
                    "region_rate": Value::Null,
                    "status": "incomplete",
                })
            })
            .collect(),
        reasons: vec![reason.into()],
        remediation: json!({"missing_artifact": 1}),
        component_evidence: Vec::new(),
        regions: Vec::new(),
        inventory,
        provenance,
    }
}

#[derive(Clone, Debug)]
struct ExpectedRegion {
    logical_source_id: String,
    path: String,
    component_id: String,
    language: String,
    package: String,
    source_hash: String,
    variants: Vec<String>,
    expected_formats: Vec<String>,
    region: InventoryRegion,
}

/// Parses and validates one composite descriptor from a registered artifact.
pub fn composite_descriptor(spec: &Value) -> AppResult<Option<CompositeArtifactDescriptor>> {
    let Some(raw) = spec.get("composite") else {
        return Ok(None);
    };
    let object = raw.as_object().ok_or_else(|| {
        AppError::Validation("artifact composite metadata must be an object".to_owned())
    })?;
    let required = |name: &str| -> AppResult<String> {
        object
            .get(name)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| {
                AppError::Validation(format!(
                    "composite.{name} is required and must be a non-blank string"
                ))
            })
    };
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or("coverage")
        .trim()
        .to_owned();
    if !matches!(role.as_str(), "coverage" | "inventory") {
        return Err(AppError::Validation(
            "composite.role must be coverage or inventory".to_owned(),
        ));
    }
    let aliases = parse_aliases(object.get("logical_source_aliases"))?;
    if role == "coverage" && aliases.is_empty() {
        return Err(AppError::Validation(
            "composite.logical_source_aliases is required for coverage artifacts".to_owned(),
        ));
    }
    let toolchain_versions = object
        .get("toolchain_versions")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !toolchain_versions.is_object() {
        return Err(AppError::Validation(
            "composite.toolchain_versions must be an object".to_owned(),
        ));
    }
    Ok(Some(CompositeArtifactDescriptor {
        component_id: required("component_id")?,
        package_variant: required("package_variant")?,
        language: required("language")?,
        format: normalize_composite_format(
            object
                .get("format")
                .or_else(|| object.get("coverage_format"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(if role == "inventory" {
                    INVENTORY_FORMAT
                } else {
                    "auto"
                }),
        ),
        inventory_artifact: required("inventory_artifact")?,
        logical_source_aliases: aliases,
        role,
        toolchain_versions,
    }))
}

fn parse_aliases(value: Option<&Value>) -> AppResult<Vec<LogicalSourceAlias>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let values = value.as_array().ok_or_else(|| {
        AppError::Validation(
            "composite.logical_source_aliases must be an array of {path,logical_source_id} objects"
                .to_owned(),
        )
    })?;
    let mut aliases = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let object = value.as_object().ok_or_else(|| {
            AppError::Validation(
                "composite.logical_source_aliases entries must be objects".to_owned(),
            )
        })?;
        let path = required_object_string(object, "path", "logical source alias")?;
        let logical_source_id =
            required_object_string(object, "logical_source_id", "logical source alias")?;
        if !seen.insert((path.clone(), logical_source_id.clone())) {
            return Err(AppError::Validation(
                "composite.logical_source_aliases must not contain duplicates".to_owned(),
            ));
        }
        aliases.push(LogicalSourceAlias {
            path,
            logical_source_id,
        });
    }
    Ok(aliases)
}

/// Parses an authoritative inventory artifact.
pub fn parse_inventory(path: &Path) -> AppResult<Inventory> {
    let value: Value = serde_json::from_slice(&fs::read(path)?).map_err(|error| {
        AppError::Validation(format!("invalid composite inventory JSON: {error}"))
    })?;
    let object = value.as_object().ok_or_else(|| {
        AppError::Validation("composite inventory must be a JSON object".to_owned())
    })?;
    let schema = required_object_string(object, "schema", "composite inventory")?;
    if schema != INVENTORY_SCHEMA {
        return Err(AppError::Validation(format!(
            "unsupported composite inventory schema: {schema}"
        )));
    }
    let mapping_version = required_object_string(object, "mapping_version", "composite inventory")?;
    if mapping_version != MAPPING_VERSION {
        return Err(AppError::Validation(format!(
            "unsupported composite mapping version: {mapping_version}"
        )));
    }
    let source_revision = required_object_string(object, "source_revision", "composite inventory")?;
    let entries = object
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AppError::Validation("composite inventory.entries must be an array".to_owned())
        })?
        .iter()
        .map(parse_inventory_entry)
        .collect::<AppResult<Vec<_>>>()?;
    if entries.is_empty() {
        return Err(AppError::Validation(
            "composite inventory.entries must not be empty".to_owned(),
        ));
    }
    let mut ids = BTreeSet::new();
    for entry in &entries {
        if !ids.insert(entry.logical_source_id.clone()) {
            return Err(AppError::Validation(format!(
                "composite inventory logical_source_id is duplicated: {}",
                entry.logical_source_id
            )));
        }
    }
    Ok(Inventory {
        schema,
        mapping_version,
        source_revision,
        entries,
    })
}

fn parse_inventory_entry(value: &Value) -> AppResult<InventoryEntry> {
    let object = value.as_object().ok_or_else(|| {
        AppError::Validation("composite inventory entries must be objects".to_owned())
    })?;
    let logical_source_id = required_object_string(object, "logical_source_id", "inventory entry")?;
    let path = required_object_string(object, "path", "inventory entry")?;
    validate_inventory_path(&path)?;
    let package = required_object_string(object, "package", "inventory entry")?;
    let component_id = object
        .get("component_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&package)
        .trim()
        .to_owned();
    let language =
        required_object_string(object, "language", "inventory entry")?.to_ascii_lowercase();
    let role = required_object_string(object, "role", "inventory entry")?;
    if !matches!(role.as_str(), "production" | "catalogue") {
        return Err(AppError::Validation(format!(
            "inventory entry {logical_source_id} role must be production or catalogue"
        )));
    }
    let source_hash =
        required_object_string(object, "source_hash", "inventory entry")?.to_ascii_lowercase();
    validate_sha256(
        &source_hash,
        &format!("inventory entry {logical_source_id}"),
    )?;
    let variants = string_array(object.get("variants"), "inventory entry variants")?;
    let expected_formats = string_array(
        object.get("expected_formats"),
        "inventory entry expected_formats",
    )?;
    let mut variant_ids = BTreeSet::new();
    if variants
        .iter()
        .any(|variant| !variant_ids.insert(variant.clone()))
    {
        return Err(AppError::Validation(format!(
            "inventory entry {logical_source_id} variants must not contain duplicates"
        )));
    }
    let mut expected_formats = expected_formats
        .into_iter()
        .map(|format| normalize_composite_format(&format))
        .collect::<Vec<_>>();
    expected_formats.sort();
    expected_formats.dedup();
    let raw_regions = object
        .get("regions")
        .or_else(|| object.get("expected_regions"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AppError::Validation(format!(
                "inventory entry {logical_source_id} must contain regions"
            ))
        })?;
    let regions = raw_regions
        .iter()
        .map(parse_inventory_region)
        .collect::<AppResult<Vec<_>>>()?;
    let mut region_ids = BTreeSet::new();
    for region in &regions {
        if !region_ids.insert((
            region.start_line,
            region.end_line,
            region.discriminator.clone(),
        )) {
            return Err(AppError::Validation(format!(
                "inventory entry {logical_source_id} regions must not contain duplicates"
            )));
        }
    }
    if role == "production" && regions.is_empty() {
        return Err(AppError::Validation(format!(
            "production inventory entry {logical_source_id} must contain regions"
        )));
    }
    if variants.is_empty() && role == "production" {
        return Err(AppError::Validation(format!(
            "production inventory entry {logical_source_id} must declare variants"
        )));
    }
    if expected_formats.is_empty() && role == "production" {
        return Err(AppError::Validation(format!(
            "production inventory entry {logical_source_id} must declare expected_formats"
        )));
    }
    Ok(InventoryEntry {
        logical_source_id,
        path,
        package,
        component_id,
        language,
        role,
        source_hash,
        variants,
        expected_formats,
        regions,
    })
}

fn parse_inventory_region(value: &Value) -> AppResult<InventoryRegion> {
    let object = value
        .as_object()
        .ok_or_else(|| AppError::Validation("inventory regions must be objects".to_owned()))?;
    let start_line = positive_integer(object.get("start_line"), "inventory region start_line")?;
    let end_line = positive_integer(object.get("end_line"), "inventory region end_line")?;
    if end_line < start_line {
        return Err(AppError::Validation(
            "inventory region end_line must be >= start_line".to_owned(),
        ));
    }
    let discriminator = required_object_string(object, "discriminator", "inventory region")?;
    Ok(InventoryRegion {
        start_line,
        end_line,
        discriminator,
    })
}

/// Parses one canonical region artifact.
pub fn parse_region_artifact(path: &Path) -> AppResult<Vec<CanonicalRegion>> {
    parse_region_artifact_with_revision(path).map(|(_, regions)| regions)
}

/// Reads the source revision and regions from a canonical region artifact.
pub fn parse_region_artifact_with_revision(
    path: &Path,
) -> AppResult<(String, Vec<CanonicalRegion>)> {
    let value: Value = serde_json::from_slice(&fs::read(path)?)
        .map_err(|error| AppError::Validation(format!("invalid canonical region JSON: {error}")))?;
    let object = value.as_object().ok_or_else(|| {
        AppError::Validation("canonical region artifact must be a JSON object".to_owned())
    })?;
    let schema = required_object_string(object, "schema", "canonical region artifact")?;
    if schema != REGIONS_SCHEMA {
        return Err(AppError::Validation(format!(
            "unsupported canonical region schema: {schema}"
        )));
    }
    let mapping_version =
        required_object_string(object, "mapping_version", "canonical region artifact")?;
    if mapping_version != MAPPING_VERSION {
        return Err(AppError::Validation(format!(
            "unsupported canonical region mapping version: {mapping_version}"
        )));
    }
    let source_revision =
        required_object_string(object, "source_revision", "canonical region artifact")?;
    let regions = object
        .get("regions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AppError::Validation("canonical region artifact.regions must be an array".to_owned())
        })?
        .iter()
        .map(parse_canonical_region)
        .collect::<AppResult<Vec<_>>>()?;
    Ok((source_revision, regions))
}

fn parse_canonical_region(value: &Value) -> AppResult<CanonicalRegion> {
    let object = value.as_object().ok_or_else(|| {
        AppError::Validation("canonical region entries must be objects".to_owned())
    })?;
    let path = required_object_string(object, "path", "canonical region")?;
    let logical_source_id = object
        .get("logical_source_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let start_line = positive_integer(object.get("start_line"), "canonical region start_line")?;
    let end_line = positive_integer(object.get("end_line"), "canonical region end_line")?;
    if end_line < start_line {
        return Err(AppError::Validation(
            "canonical region end_line must be >= start_line".to_owned(),
        ));
    }
    let discriminator = required_object_string(object, "discriminator", "canonical region")?;
    let hits = object.get("hits").map_or(Ok(0), |value| {
        positive_or_zero(value, "canonical region hits")
    })?;
    let covered = object
        .get("covered")
        .and_then(Value::as_bool)
        .unwrap_or(hits > 0);
    let source_hash = Some(
        required_object_string(object, "source_hash", "canonical region")?.to_ascii_lowercase(),
    );
    Ok(CanonicalRegion {
        path,
        logical_source_id,
        start_line,
        end_line,
        discriminator,
        hits,
        covered,
        source_hash,
    })
}

/// Derives canonical regions from a standard report when it preserves enough
/// source identity for the requested mapping. The dedicated region artifact is
/// preferred for WGSL and for producers with richer span/discriminator data.
pub fn derive_standard_regions(
    path: &Path,
    format: &str,
    repo_path: Option<&str>,
) -> AppResult<Vec<CanonicalRegion>> {
    match format {
        "coveragepy" => derive_coveragepy_regions(path, repo_path),
        "istanbul" => derive_istanbul_regions(path, repo_path),
        "llvm" => derive_llvm_regions(path, repo_path),
        _ => {
            let report = parse_coverage_report(path, format, repo_path)?;
            Ok(report
                .lines
                .into_iter()
                .filter(|line| line.count_line)
                .map(|line| CanonicalRegion {
                    path: line.file_path,
                    logical_source_id: None,
                    start_line: line.line_number,
                    end_line: line.line_number,
                    discriminator: format!("{format}:line:{}", line.line_number),
                    hits: line.hits,
                    covered: line.covered,
                    source_hash: None,
                })
                .collect())
        }
    }
}

fn derive_coveragepy_regions(
    path: &Path,
    repo_path: Option<&str>,
) -> AppResult<Vec<CanonicalRegion>> {
    let value: Value = serde_json::from_slice(&fs::read(path)?)
        .map_err(|error| AppError::Validation(format!("invalid coverage.py JSON: {error}")))?;
    let files = value
        .get("files")
        .and_then(Value::as_object)
        .ok_or_else(|| AppError::Validation("coverage.py JSON must contain files".to_owned()))?;
    let mut regions = Vec::new();
    for (file_path, payload) in files {
        let payload = payload.as_object().ok_or_else(|| {
            AppError::Validation("coverage.py file payload must be an object".to_owned())
        })?;
        let normalized = normalize_report_path(file_path, repo_path);
        for (covered, field) in [(true, "executed_lines"), (false, "missing_lines")] {
            for line in integer_array(payload.get(field), &format!("coverage.py {field}"))? {
                regions.push(CanonicalRegion {
                    path: normalized.clone(),
                    logical_source_id: None,
                    start_line: line,
                    end_line: line,
                    discriminator: format!("python:line:{line}"),
                    hits: i64::from(covered),
                    covered,
                    source_hash: None,
                });
            }
        }
    }
    Ok(regions)
}

fn derive_istanbul_regions(
    path: &Path,
    repo_path: Option<&str>,
) -> AppResult<Vec<CanonicalRegion>> {
    let value: Value = serde_json::from_slice(&fs::read(path)?)
        .map_err(|error| AppError::Validation(format!("invalid Istanbul JSON: {error}")))?;
    let files = value.as_object().ok_or_else(|| {
        AppError::Validation("Istanbul coverage must be an object keyed by file".to_owned())
    })?;
    let mut regions = Vec::new();
    for (file_path, payload) in files {
        let payload = payload.as_object().ok_or_else(|| {
            AppError::Validation("Istanbul file payload must be an object".to_owned())
        })?;
        let statement_map = payload
            .get("statementMap")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                AppError::Validation("Istanbul file is missing statementMap".to_owned())
            })?;
        let counts = payload.get("s").and_then(Value::as_object).ok_or_else(|| {
            AppError::Validation("Istanbul file is missing statement counts".to_owned())
        })?;
        let normalized = normalize_report_path(file_path, repo_path);
        for (discriminator, location) in statement_map {
            let location = location.as_object().ok_or_else(|| {
                AppError::Validation("Istanbul statement location must be an object".to_owned())
            })?;
            let start = location_line(location.get("start"), "Istanbul statement start")?;
            let end = location_line(location.get("end"), "Istanbul statement end")?;
            let hits = counts
                .get(discriminator)
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    AppError::Validation(format!(
                        "Istanbul statement count is missing: {discriminator}"
                    ))
                })?;
            if hits < 0 {
                return Err(AppError::Validation(
                    "Istanbul statement count must be non-negative".to_owned(),
                ));
            }
            regions.push(CanonicalRegion {
                path: normalized.clone(),
                logical_source_id: None,
                start_line: start,
                end_line: end,
                discriminator: format!("istanbul:statement:{discriminator}"),
                hits,
                covered: hits > 0,
                source_hash: None,
            });
        }
    }
    Ok(regions)
}

fn derive_llvm_regions(path: &Path, repo_path: Option<&str>) -> AppResult<Vec<CanonicalRegion>> {
    let value: Value = serde_json::from_slice(&fs::read(path)?)
        .map_err(|error| AppError::Validation(format!("invalid LLVM JSON: {error}")))?;
    let units = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| AppError::Validation("LLVM JSON must contain data".to_owned()))?;
    let mut regions = Vec::new();
    for unit in units {
        let files = unit
            .get("files")
            .and_then(Value::as_array)
            .ok_or_else(|| AppError::Validation("LLVM data entry must contain files".to_owned()))?;
        for file in files {
            let object = file
                .as_object()
                .ok_or_else(|| AppError::Validation("LLVM file must be an object".to_owned()))?;
            let raw_path = required_object_string(object, "filename", "LLVM file")?;
            let normalized = normalize_report_path(&raw_path, repo_path);
            let segments = object
                .get("segments")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    AppError::Validation("LLVM file must contain segments".to_owned())
                })?;
            let entries = segments
                .iter()
                .filter_map(|segment| {
                    let values = segment.as_array()?;
                    (values.len() >= 5
                        && values.get(3).and_then(Value::as_bool) == Some(true)
                        && values.get(4).and_then(Value::as_bool) == Some(true))
                    .then_some(values)
                })
                .collect::<Vec<_>>();
            for (index, values) in entries.iter().enumerate() {
                let start_line = value_i64(values.first())?;
                let start_column = value_i64(values.get(1))?;
                let hits = value_i64(values.get(2))?;
                let (end_line, end_column) = entries
                    .get(index + 1)
                    .map(|next| (value_i64(next.first()), value_i64(next.get(1))))
                    .unwrap_or((Ok(start_line), Ok(start_column + 1)));
                let end_line = end_line?;
                let end_column = end_column?;
                regions.push(CanonicalRegion {
                    path: normalized.clone(),
                    logical_source_id: None,
                    start_line,
                    end_line,
                    discriminator: format!(
                        "llvm:{start_line}:{start_column}:{end_line}:{end_column}"
                    ),
                    hits,
                    covered: hits > 0,
                    source_hash: None,
                });
            }
        }
    }
    Ok(regions)
}

/// Aggregates one inventory and its declared component evidence.
pub fn assemble(
    inventory: &Inventory,
    inputs: &[CompositeComponentInput],
    repo_path: &Path,
    checkout_revision: Option<&str>,
    checkout_dirty: bool,
) -> CompositeBuild {
    let mut reasons = BTreeSet::new();
    let production = inventory
        .entries
        .iter()
        .filter(|entry| entry.role == "production")
        .collect::<Vec<_>>();
    if production.is_empty() {
        reasons.insert("inventory contains no production entries".to_owned());
    }
    if Some(inventory.source_revision.as_str()) != checkout_revision {
        reasons.insert("inventory source revision does not match the checkout".to_owned());
    }
    if checkout_dirty {
        reasons.insert(
            "checkout is dirty; production composite evidence requires a clean revision".to_owned(),
        );
    }
    let expected = expected_regions(&production);
    let mut observed: BTreeMap<(String, String), BTreeMap<String, (bool, i64)>> = BTreeMap::new();
    let mut descriptor_statuses = BTreeMap::<(String, String), String>::new();
    let mut source_mismatch_keys = BTreeSet::<(String, String, String)>::new();
    let mut descriptors = BTreeSet::new();
    let mut component_evidence = Vec::new();
    let mut artifact_hashes = Map::new();
    let mut toolchain_versions = Map::new();
    for input in inputs
        .iter()
        .filter(|input| input.descriptor.role == "coverage")
    {
        let descriptor = &input.descriptor;
        let descriptor_key = (
            descriptor.component_id.clone(),
            descriptor.package_variant.clone(),
        );
        descriptor_statuses.insert(descriptor_key.clone(), input.artifact_status.clone());
        if input
            .source_revision
            .as_deref()
            .is_some_and(|revision| revision != inventory.source_revision)
        {
            descriptor_statuses.insert(descriptor_key.clone(), "stale_evidence".to_owned());
            reasons.insert(format!(
                "{}:{} evidence source revision does not match the inventory",
                descriptor.component_id, descriptor.package_variant
            ));
        }
        if !descriptors.insert(descriptor_key.clone()) {
            reasons.insert(format!(
                "duplicate evidence descriptor for {}:{}",
                descriptor.component_id, descriptor.package_variant
            ));
        }
        artifact_hashes.insert(
            format!("{}:{}", descriptor.component_id, descriptor.package_variant),
            input
                .artifact_hash
                .clone()
                .map_or(Value::Null, Value::String),
        );
        if let Some(object) = descriptor.toolchain_versions.as_object() {
            toolchain_versions.insert(
                format!("{}:{}", descriptor.component_id, descriptor.package_variant),
                Value::Object(object.clone()),
            );
        }
        if input.artifact_status != "ingested" {
            reasons.insert(format!(
                "{}:{} evidence is {}",
                descriptor.component_id, descriptor.package_variant, input.artifact_status
            ));
        }
        let expected_for_component = expected
            .values()
            .filter(|entry| {
                entry.component_id == descriptor.component_id
                    && entry.variants.contains(&descriptor.package_variant)
            })
            .collect::<Vec<_>>();
        if expected_for_component.is_empty() {
            reasons.insert(format!(
                "evidence {}:{} is not owned by the production inventory",
                descriptor.component_id, descriptor.package_variant
            ));
        }
        let format_allowed = expected_for_component.iter().any(|entry| {
            entry
                .expected_formats
                .iter()
                .any(|format| normalize_composite_format(format) == descriptor.format)
        });
        if !format_allowed {
            descriptor_statuses.insert(descriptor_key, "format_mismatch".to_owned());
            reasons.insert(format!(
                "no declared evidence uses expected format {} for {}:{}",
                descriptor.format, descriptor.component_id, descriptor.package_variant
            ));
        }
        let aliases = descriptor
            .logical_source_aliases
            .iter()
            .map(|alias| {
                (
                    normalize_report_path(&alias.path, Some(&repo_path.to_string_lossy())),
                    alias.logical_source_id.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut observed_count = 0_i64;
        for region in &input.regions {
            if !format_allowed {
                continue;
            }
            let logical_source_id = region.logical_source_id.clone().or_else(|| {
                aliases
                    .get(&normalize_report_path(
                        &region.path,
                        Some(&repo_path.to_string_lossy()),
                    ))
                    .cloned()
            });
            let Some(logical_source_id) = logical_source_id else {
                reasons.insert(format!(
                    "{}:{} contains a region without an explicit logical-source alias",
                    descriptor.component_id, descriptor.package_variant
                ));
                continue;
            };
            let key = region.key(&logical_source_id);
            let Some(expected_region) = expected.get(&key) else {
                reasons.insert(format!("evidence contains unowned region {key}"));
                continue;
            };
            if let Some(source_hash) = region.source_hash.as_deref() {
                if source_hash != expected_region.source_hash {
                    reasons.insert(format!("source hash mismatch for {logical_source_id}"));
                    source_mismatch_keys.insert((
                        descriptor.component_id.clone(),
                        descriptor.package_variant.clone(),
                        key.clone(),
                    ));
                    continue;
                }
            }
            observed_count += 1;
            let entry = observed
                .entry((
                    descriptor.component_id.clone(),
                    descriptor.package_variant.clone(),
                ))
                .or_default()
                .entry(key)
                .or_insert((false, 0));
            entry.0 |= region.covered;
            entry.1 = entry.1.max(region.hits);
        }
        component_evidence.push(json!({
            "component_id": descriptor.component_id,
            "package_variant": descriptor.package_variant,
            "language": descriptor.language,
            "status": if input.artifact_status == "ingested" { "measured" } else { "missing" },
            "evidence_source": if input.reused_from_snapshot_id.is_some() { "baseline_snapshot" } else { "run_artifact" },
            "reused_from_snapshot_id": input.reused_from_snapshot_id,
            "format": descriptor_format(descriptor, input),
            "artifact_path": input.artifact_path,
            "artifact_hash": input.artifact_hash,
            "snapshot_id": input.snapshot_id,
            "observed_regions": observed_count,
            "toolchain_versions": descriptor.toolchain_versions,
        }));
    }
    let mut source_mismatch_sources = BTreeSet::new();
    for entry in &production {
        if !matches!(
            entry.language.as_str(),
            "rust" | "wgsl" | "wasm" | "python" | "javascript" | "js"
        ) {
            reasons.insert(format!(
                "unsupported production language for {}: {}",
                entry.logical_source_id, entry.language
            ));
        }
        let variants = if entry.variants.is_empty() {
            vec!["default".to_owned()]
        } else {
            entry.variants.clone()
        };
        for variant in variants {
            if !descriptors.contains(&(entry.component_id.clone(), variant.clone())) {
                reasons.insert(format!(
                    "missing required package/backend variant {}:{}",
                    entry.component_id, variant
                ));
            }
        }
        let source_path = repo_path.join(&entry.path);
        match source_hash(&source_path) {
            Ok(actual) if actual == entry.source_hash => {}
            Ok(_) => {
                source_mismatch_sources.insert(entry.logical_source_id.clone());
                reasons.insert(format!(
                    "source hash mismatch for {}",
                    entry.logical_source_id
                ));
            }
            Err(_) => {
                source_mismatch_sources.insert(entry.logical_source_id.clone());
                reasons.insert(format!("inventory source is missing: {}", entry.path));
            }
        }
        for format in &entry.expected_formats {
            let expected_format = normalize_composite_format(format);
            if !inputs.iter().any(|input| {
                input.descriptor.role == "coverage"
                    && input.descriptor.component_id == entry.component_id
                    && input.descriptor.language == entry.language
                    && descriptor_format(&input.descriptor, input) == expected_format
            }) {
                // The variant-specific check above provides the authoritative
                // missing-evidence error. This reason catches a wrong format.
                if descriptors
                    .iter()
                    .any(|(component, _)| component == &entry.component_id)
                {
                    reasons.insert(format!(
                        "no declared evidence uses expected format {expected_format} for {}",
                        entry.logical_source_id
                    ));
                }
            }
        }
    }
    let mut covered_keys = BTreeSet::new();
    let mut region_rows = Vec::new();
    let mut remediation_counts: BTreeMap<String, i64> = BTreeMap::new();
    for (key, entry) in &expected {
        let mut variant_states = Vec::new();
        let variants = entry.variants.clone();
        for variant in variants {
            let observed_region = observed
                .get(&(entry.component_id.clone(), variant.clone()))
                .and_then(|regions| regions.get(key));
            let descriptor_status =
                descriptor_statuses.get(&(entry.component_id.clone(), variant.clone()));
            let source_mismatch = source_mismatch_sources.contains(&entry.logical_source_id)
                || source_mismatch_keys.contains(&(
                    entry.component_id.clone(),
                    variant.clone(),
                    key.clone(),
                ));
            let (state, hits) = if source_mismatch {
                ("source_mismatch", 0)
            } else if checkout_dirty && descriptor_status.is_some_and(|status| status == "ingested")
            {
                ("stale_evidence", 0)
            } else {
                match descriptor_status {
                    None => ("missing_artifact", 0),
                    Some(status) if status == "missing" => ("missing_artifact", 0),
                    Some(status) if status == "skipped_stale" => ("stale_evidence", 0),
                    Some(status) if status != "ingested" => ("unmeasured", 0),
                    Some(_) => match observed_region {
                        Some((true, hits)) => {
                            covered_keys.insert(key.clone());
                            ("covered", *hits)
                        }
                        Some((false, hits)) => ("uncovered", *hits),
                        None => ("unmeasured", 0),
                    },
                }
            };
            /*
             * A shared logical source is covered when any required variant
             * covers it, but variant_states still expose every missing or
             * stale producer so the composite cannot be considered complete.
             */
            if state == "covered" {
                covered_keys.insert(key.clone());
            }
            *remediation_counts
                .entry(remediation_for_state(state).to_owned())
                .or_default() += 1;
            variant_states.push(json!({
                "package_variant": variant,
                "state": state,
                "hits": hits,
            }));
        }
        let state = [
            "source_mismatch",
            "missing_artifact",
            "stale_evidence",
            "unmeasured",
            "covered",
            "uncovered",
        ]
        .into_iter()
        .find(|candidate| {
            variant_states
                .iter()
                .any(|value| value["state"] == *candidate)
        })
        .unwrap_or("unmeasured");
        region_rows.push(json!({
            "region_key": key,
            "logical_source_id": entry.logical_source_id,
            "component_id": entry.component_id,
            "package": entry.package,
            "language": entry.language,
            "path": entry.path,
            "start_line": entry.region.start_line,
            "end_line": entry.region.end_line,
            "discriminator": entry.region.discriminator,
            "source_hash": entry.source_hash,
            "state": state,
            "variants": variant_states,
            "remediation_bucket": remediation_for_state(state),
        }));
    }
    let total_regions = expected.len() as i64;
    let covered_regions = covered_keys.len() as i64;
    let components = component_summaries(&expected, &covered_keys, &reasons);
    let status = if reasons.is_empty() && total_regions > 0 && covered_regions == total_regions {
        "complete"
    } else {
        "incomplete"
    };
    let inventory_value = json!({
        "schema": inventory.schema,
        "mapping_version": inventory.mapping_version,
        "source_revision": inventory.source_revision,
        "production_entries": production.len(),
        "catalogue_entries": inventory.entries.len().saturating_sub(production.len()),
        "production_regions": total_regions,
    });
    let provenance = json!({
        "mapping_version": inventory.mapping_version,
        "inventory_source_revision": inventory.source_revision,
        "checkout_revision": checkout_revision,
        "checkout_dirty": checkout_dirty,
        "artifact_hashes": artifact_hashes,
        "toolchain_versions": toolchain_versions,
    });
    CompositeBuild {
        status: status.to_owned(),
        covered_regions,
        total_regions,
        components,
        reasons: reasons.into_iter().collect(),
        remediation: Value::Object(
            remediation_counts
                .into_iter()
                .map(|(key, value)| (key, json!(value)))
                .collect(),
        ),
        component_evidence,
        regions: region_rows,
        inventory: inventory_value,
        provenance,
    }
}

fn expected_regions(entries: &[&InventoryEntry]) -> BTreeMap<String, ExpectedRegion> {
    let mut expected = BTreeMap::new();
    for entry in entries {
        let variants = if entry.variants.is_empty() {
            vec!["default".to_owned()]
        } else {
            entry.variants.clone()
        };
        for region in &entry.regions {
            expected.insert(
                region.key(&entry.logical_source_id),
                ExpectedRegion {
                    logical_source_id: entry.logical_source_id.clone(),
                    path: entry.path.clone(),
                    component_id: entry.component_id.clone(),
                    language: entry.language.clone(),
                    package: entry.package.clone(),
                    source_hash: entry.source_hash.clone(),
                    variants: variants.clone(),
                    expected_formats: entry.expected_formats.clone(),
                    region: region.clone(),
                },
            );
        }
    }
    expected
}

fn component_summaries(
    expected: &BTreeMap<String, ExpectedRegion>,
    covered: &BTreeSet<String>,
    reasons: &BTreeSet<String>,
) -> Vec<Value> {
    let mut groups: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (key, entry) in expected {
        let component = match entry.language.as_str() {
            "rust" | "wgsl" | "wasm" => "rust",
            "python" => "python",
            "javascript" | "js" => "javascript",
            _ => "other",
        };
        groups
            .entry(component.to_owned())
            .or_default()
            .insert(key.clone());
    }
    ["rust", "python", "javascript"]
        .into_iter()
        .map(|component| {
            let keys = groups.get(component).cloned().unwrap_or_default();
            let total = keys.len() as i64;
            let covered_count = keys.intersection(covered).count() as i64;
            json!({
                "component": component,
                "covered_regions": covered_count,
                "total_regions": total,
                "region_rate": (total > 0).then_some(covered_count as f64 / total as f64),
                "status": if total > 0 && covered_count == total && reasons.is_empty() { "complete" } else { "incomplete" },
            })
        })
        .collect()
}

fn descriptor_format(
    descriptor: &CompositeArtifactDescriptor,
    _input: &CompositeComponentInput,
) -> String {
    descriptor.format.clone()
}

fn remediation_for_state(state: &str) -> &'static str {
    match state {
        "uncovered" => "test_gap",
        "unmeasured" => "instrumentation_gap",
        "missing_artifact" => "generated_variant_gap",
        "stale_evidence" => "instrumentation_gap",
        "source_mismatch" => "source_mismatch",
        _ => "none",
    }
}

fn validate_inventory_path(path: &str) -> AppResult<()> {
    let value = Path::new(path);
    if value.is_absolute()
        || value.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(AppError::Validation(
            "inventory entry path must be repository-relative".to_owned(),
        ));
    }
    Ok(())
}

fn validate_sha256(value: &str, context: &str) -> AppResult<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(AppError::Validation(format!(
            "{context}.source_hash must be a 64-character SHA-256 hex string"
        )));
    }
    Ok(())
}

fn required_object_string(
    object: &Map<String, Value>,
    key: &str,
    context: &str,
) -> AppResult<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| AppError::Validation(format!("{context}.{key} must be a non-blank string")))
}

fn string_array(value: Option<&Value>, context: &str) -> AppResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    value
        .as_array()
        .ok_or_else(|| AppError::Validation(format!("{context} must be an array")))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| AppError::Validation(format!("{context} must contain strings")))
        })
        .collect()
}

fn positive_integer(value: Option<&Value>, context: &str) -> AppResult<i64> {
    let value = value
        .and_then(Value::as_i64)
        .ok_or_else(|| AppError::Validation(format!("{context} must be a positive integer")))?;
    if value < 1 {
        return Err(AppError::Validation(format!(
            "{context} must be a positive integer"
        )));
    }
    Ok(value)
}

fn positive_or_zero(value: &Value, context: &str) -> AppResult<i64> {
    let value = value
        .as_i64()
        .ok_or_else(|| AppError::Validation(format!("{context} must be an integer")))?;
    if value < 0 {
        return Err(AppError::Validation(format!(
            "{context} must be non-negative"
        )));
    }
    Ok(value)
}

fn integer_array(value: Option<&Value>, context: &str) -> AppResult<Vec<i64>> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| AppError::Validation(format!("{context} must be an array")))?
        .iter()
        .map(|value| positive_integer(Some(value), context))
        .collect()
}

fn location_line(value: Option<&Value>, context: &str) -> AppResult<i64> {
    let object = value
        .and_then(Value::as_object)
        .ok_or_else(|| AppError::Validation(format!("{context} must be an object")))?;
    positive_integer(object.get("line"), context)
}

fn value_i64(value: Option<&Value>) -> AppResult<i64> {
    value.and_then(Value::as_i64).ok_or_else(|| {
        AppError::Validation("canonical region number must be an integer".to_owned())
    })
}

fn region_key(
    logical_source_id: &str,
    start_line: i64,
    end_line: i64,
    discriminator: &str,
) -> String {
    format!("{logical_source_id}:{start_line}:{end_line}:{discriminator}")
}

fn source_hash(path: &Path) -> AppResult<String> {
    let mut digest = Sha256::new();
    digest.update(fs::read(path)?);
    Ok(hex_encode(&digest.finalize()))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push_str(&format!("{byte:02x}"));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn descriptor_requires_explicit_aliases_for_coverage() {
        let error = composite_descriptor(&json!({
            "composite": {
                "component_id":"rust",
                "package_variant":"cpu",
                "language":"rust",
                "inventory_artifact":"inventory.json"
            }
        }))
        .expect_err("coverage descriptor without aliases must fail");
        assert!(error.to_string().contains("logical_source_aliases"));
    }

    #[test]
    fn inventory_and_regions_aggregate_with_explicit_aliases() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("src.rs");
        fs::write(&source, "fn main() {}\n").unwrap();
        let source_hash = source_hash(&source).unwrap();
        let inventory_path = directory.path().join("inventory.json");
        let inventory = json!({
            "schema": INVENTORY_SCHEMA,
            "mapping_version": MAPPING_VERSION,
            "source_revision":"commit",
            "entries":[{
                "logical_source_id":"shared:src.rs",
                "path":"src.rs",
                "package":"demo",
                "component_id":"rust",
                "language":"rust",
                "role":"production",
                "source_hash":source_hash,
                "variants":["cpu","gpu"],
                "expected_formats":["coverage-mcp-regions-v1"],
                "regions":[{"start_line":1,"end_line":1,"discriminator":"r1"}]
            }]
        });
        let mut file = fs::File::create(&inventory_path).unwrap();
        file.write_all(serde_json::to_string(&inventory).unwrap().as_bytes())
            .unwrap();
        let inventory = parse_inventory(&inventory_path).unwrap();
        let region = CanonicalRegion {
            path: "src.rs".to_owned(),
            logical_source_id: None,
            start_line: 1,
            end_line: 1,
            discriminator: "r1".to_owned(),
            hits: 1,
            covered: true,
            source_hash: Some(source_hash.clone()),
        };
        let input = |variant: &str| CompositeComponentInput {
            descriptor: CompositeArtifactDescriptor {
                component_id: "rust".to_owned(),
                package_variant: variant.to_owned(),
                language: "rust".to_owned(),
                format: REGIONS_SCHEMA.to_owned(),
                inventory_artifact: "inventory.json".to_owned(),
                logical_source_aliases: vec![LogicalSourceAlias {
                    path: "src.rs".to_owned(),
                    logical_source_id: "shared:src.rs".to_owned(),
                }],
                role: "coverage".to_owned(),
                toolchain_versions: json!({"rust":"1.85.1"}),
            },
            artifact_path: directory.path().join(format!("{variant}.json")),
            artifact_hash: Some(format!("hash-{variant}")),
            snapshot_id: None,
            source_revision: None,
            artifact_status: "ingested".to_owned(),
            reused_from_snapshot_id: None,
            regions: vec![region.clone()],
        };
        let result = assemble(
            &inventory,
            &[input("cpu"), input("gpu")],
            directory.path(),
            Some("commit"),
            false,
        );
        assert_eq!(result.status, "complete");
        assert_eq!((result.covered_regions, result.total_regions), (1, 1));
        assert_eq!(result.components[0]["covered_regions"], 1);
        assert_eq!(result.components[0]["total_regions"], 1);
    }

    fn valid_descriptor_value() -> Value {
        json!({
            "composite": {
                "component_id": "rust",
                "package_variant": "default",
                "language": "rust",
                "inventory_artifact": "inventory.json",
                "logical_source_aliases": [{
                    "path": "src.rs",
                    "logical_source_id": "src:rs"
                }],
                "toolchain_versions": {"rust": "test"}
            }
        })
    }

    fn valid_inventory_entry() -> Value {
        json!({
            "logical_source_id": "src:rs",
            "path": "src.rs",
            "package": "demo",
            "component_id": "rust",
            "language": "rust",
            "role": "production",
            "source_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "variants": ["default"],
            "expected_formats": [REGIONS_SCHEMA],
            "regions": [{"start_line": 1, "end_line": 1, "discriminator": "r1"}]
        })
    }

    fn write_value(directory: &tempfile::TempDir, name: &str, value: &Value) -> std::path::PathBuf {
        let path = directory.path().join(name);
        fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        path
    }

    #[test]
    fn descriptor_validation_covers_inventory_and_error_shapes() {
        assert!(composite_descriptor(&json!({})).unwrap().is_none());
        assert!(composite_descriptor(&json!({"composite": 1})).is_err());
        let mut invalid_role = valid_descriptor_value();
        invalid_role["composite"]["role"] = json!("other");
        assert!(composite_descriptor(&invalid_role).is_err());
        let mut invalid_aliases = valid_descriptor_value();
        invalid_aliases["composite"]["logical_source_aliases"] = json!(1);
        assert!(composite_descriptor(&invalid_aliases).is_err());
        let mut invalid_alias_entry = valid_descriptor_value();
        invalid_alias_entry["composite"]["logical_source_aliases"] = json!([1]);
        assert!(composite_descriptor(&invalid_alias_entry).is_err());
        let mut missing_alias_path = valid_descriptor_value();
        missing_alias_path["composite"]["logical_source_aliases"] =
            json!([{"logical_source_id":"src:rs"}]);
        assert!(composite_descriptor(&missing_alias_path).is_err());
        let mut missing_alias_id = valid_descriptor_value();
        missing_alias_id["composite"]["logical_source_aliases"] = json!([{"path":"src.rs"}]);
        assert!(composite_descriptor(&missing_alias_id).is_err());
        let mut duplicate_alias = valid_descriptor_value();
        duplicate_alias["composite"]["logical_source_aliases"] = json!([
            {"path":"src.rs","logical_source_id":"src:rs"},
            {"path":"src.rs","logical_source_id":"src:rs"}
        ]);
        assert!(composite_descriptor(&duplicate_alias).is_err());
        let mut invalid_toolchain = valid_descriptor_value();
        invalid_toolchain["composite"]["toolchain_versions"] = json!("rust");
        assert!(composite_descriptor(&invalid_toolchain).is_err());
        for field in [
            "component_id",
            "package_variant",
            "language",
            "inventory_artifact",
        ] {
            let mut missing = valid_descriptor_value();
            missing["composite"].as_object_mut().unwrap().remove(field);
            assert!(composite_descriptor(&missing).is_err());
        }
        let mut inventory = valid_descriptor_value();
        inventory["composite"]
            .as_object_mut()
            .unwrap()
            .remove("logical_source_aliases");
        inventory["composite"]["role"] = json!("inventory");
        inventory["composite"]
            .as_object_mut()
            .unwrap()
            .remove("toolchain_versions");
        let inventory_descriptor = composite_descriptor(&inventory).unwrap().unwrap();
        assert_eq!(inventory_descriptor.format, INVENTORY_FORMAT);
        let coverage_descriptor = composite_descriptor(&valid_descriptor_value())
            .unwrap()
            .unwrap();
        assert_eq!(coverage_descriptor.format, "auto");
        let mut blank_format = valid_descriptor_value();
        blank_format["composite"]["format"] = json!(" ");
        assert_eq!(
            composite_descriptor(&blank_format).unwrap().unwrap().format,
            "auto"
        );
    }

    #[test]
    fn inventory_validation_covers_schema_entries_and_regions() {
        let directory = tempfile::tempdir().unwrap();
        let valid_inventory = json!({
            "schema": INVENTORY_SCHEMA,
            "mapping_version": MAPPING_VERSION,
            "source_revision": "commit",
            "entries": [valid_inventory_entry(), {
                "logical_source_id": "catalogue:rs",
                "path": "catalogue.rs",
                "package": "demo",
                "language": "rust",
                "role": "catalogue",
                "source_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "variants": [],
                "expected_formats": [],
                "regions": []
            }]
        });
        let parsed = parse_inventory(&write_value(
            &directory,
            "valid-inventory.json",
            &valid_inventory,
        ))
        .unwrap();
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[1].component_id, "demo");
        assert!(parse_inventory(&directory.path().join("missing-inventory.json")).is_err());

        let mut bad_json = fs::File::create(directory.path().join("bad.json")).unwrap();
        bad_json.write_all(b"{").unwrap();
        assert!(parse_inventory(&directory.path().join("bad.json")).is_err());
        for bad in [
            json!(1),
            json!({"schema": "wrong", "mapping_version": MAPPING_VERSION, "source_revision":"c", "entries": []}),
            json!({"schema": INVENTORY_SCHEMA, "mapping_version": "wrong", "source_revision":"c", "entries": []}),
            json!({"schema": INVENTORY_SCHEMA, "mapping_version": MAPPING_VERSION, "source_revision":"c", "entries": 1}),
            json!({"schema": INVENTORY_SCHEMA, "mapping_version": MAPPING_VERSION, "source_revision":"c", "entries": []}),
        ] {
            assert!(
                parse_inventory(&write_value(&directory, "invalid-inventory.json", &bad)).is_err()
            );
        }
        let mut missing_source = valid_inventory.clone();
        missing_source
            .as_object_mut()
            .unwrap()
            .remove("source_revision");
        assert!(
            parse_inventory(&write_value(
                &directory,
                "missing-source.json",
                &missing_source
            ))
            .is_err()
        );
        let mut missing_schema = valid_inventory.clone();
        missing_schema.as_object_mut().unwrap().remove("schema");
        assert!(
            parse_inventory(&write_value(
                &directory,
                "missing-schema.json",
                &missing_schema
            ))
            .is_err()
        );
        let mut missing_mapping = valid_inventory.clone();
        missing_mapping
            .as_object_mut()
            .unwrap()
            .remove("mapping_version");
        assert!(
            parse_inventory(&write_value(
                &directory,
                "missing-mapping.json",
                &missing_mapping
            ))
            .is_err()
        );
        let mut invalid_entry = valid_inventory.clone();
        invalid_entry["entries"] = json!([1]);
        assert!(
            parse_inventory(&write_value(
                &directory,
                "invalid-entry.json",
                &invalid_entry
            ))
            .is_err()
        );
        let mut duplicate = valid_inventory.clone();
        duplicate["entries"][1]["logical_source_id"] = json!("src:rs");
        assert!(parse_inventory(&write_value(&directory, "duplicate.json", &duplicate)).is_err());

        for field in [
            "logical_source_id",
            "path",
            "package",
            "language",
            "role",
            "source_hash",
        ] {
            let mut missing = valid_inventory_entry();
            missing.as_object_mut().unwrap().remove(field);
            assert!(parse_inventory_entry(&missing).is_err());
        }
        assert!(parse_inventory_entry(&json!(1)).is_err());
        let mut invalid_path = valid_inventory_entry();
        invalid_path["path"] = json!("../src.rs");
        assert!(parse_inventory_entry(&invalid_path).is_err());
        invalid_path["path"] = json!("/src.rs");
        assert!(parse_inventory_entry(&invalid_path).is_err());
        let mut invalid_role = valid_inventory_entry();
        invalid_role["role"] = json!("other");
        assert!(parse_inventory_entry(&invalid_role).is_err());
        let mut invalid_hash = valid_inventory_entry();
        invalid_hash["source_hash"] = json!("bad");
        assert!(parse_inventory_entry(&invalid_hash).is_err());
        let mut invalid_variants = valid_inventory_entry();
        invalid_variants["variants"] = json!(1);
        assert!(parse_inventory_entry(&invalid_variants).is_err());
        invalid_variants["variants"] = json!(["default", "default"]);
        assert!(parse_inventory_entry(&invalid_variants).is_err());
        let mut invalid_formats = valid_inventory_entry();
        invalid_formats["expected_formats"] = json!([1]);
        assert!(parse_inventory_entry(&invalid_formats).is_err());
        let mut missing_regions = valid_inventory_entry();
        missing_regions.as_object_mut().unwrap().remove("regions");
        assert!(parse_inventory_entry(&missing_regions).is_err());
        let mut duplicate_regions = valid_inventory_entry();
        duplicate_regions["regions"] = json!([
            {"start_line":1,"end_line":1,"discriminator":"r1"},
            {"start_line":1,"end_line":1,"discriminator":"r1"}
        ]);
        assert!(parse_inventory_entry(&duplicate_regions).is_err());
        let mut empty_production = valid_inventory_entry();
        empty_production["regions"] = json!([]);
        assert!(parse_inventory_entry(&empty_production).is_err());
        empty_production["regions"] = json!([{"start_line":1,"end_line":1,"discriminator":"r1"}]);
        empty_production["variants"] = json!([]);
        assert!(parse_inventory_entry(&empty_production).is_err());
        empty_production["variants"] = json!(["default"]);
        empty_production["expected_formats"] = json!([]);
        assert!(parse_inventory_entry(&empty_production).is_err());
        let mut invalid_region = valid_inventory_entry();
        invalid_region["regions"] = json!([1]);
        assert!(parse_inventory_entry(&invalid_region).is_err());
        invalid_region["regions"] = json!([{"end_line":1,"discriminator":"r1"}]);
        assert!(parse_inventory_entry(&invalid_region).is_err());
        invalid_region["regions"] = json!([{"start_line":1,"discriminator":"r1"}]);
        assert!(parse_inventory_entry(&invalid_region).is_err());
        invalid_region["regions"] = json!([{"start_line":2,"end_line":1,"discriminator":"r1"}]);
        assert!(parse_inventory_entry(&invalid_region).is_err());
        invalid_region["regions"] = json!([{"start_line":1,"end_line":1}]);
        assert!(parse_inventory_entry(&invalid_region).is_err());
    }

    #[test]
    fn canonical_and_standard_region_formats_cover_valid_and_invalid_inputs() {
        let directory = tempfile::tempdir().unwrap();
        let canonical = json!({
            "schema": REGIONS_SCHEMA,
            "mapping_version": MAPPING_VERSION,
            "source_revision": "commit",
            "regions": [{
                "path": "src.rs",
                "logical_source_id": "src:rs",
                "start_line": 1,
                "end_line": 1,
                "discriminator": "r1",
                "hits": 0,
                "source_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            }]
        });
        let canonical_path = write_value(&directory, "regions.json", &canonical);
        let (revision, regions) = parse_region_artifact_with_revision(&canonical_path).unwrap();
        assert_eq!(revision, "commit");
        assert!(!regions[0].covered);
        assert_eq!(parse_region_artifact(&canonical_path).unwrap().len(), 1);
        assert!(
            parse_region_artifact_with_revision(&directory.path().join("missing-regions.json"))
                .is_err()
        );
        fs::write(directory.path().join("invalid-regions.json"), b"{").unwrap();
        assert!(
            parse_region_artifact_with_revision(&directory.path().join("invalid-regions.json"))
                .is_err()
        );
        for bad in [
            json!(1),
            json!({"schema":"wrong","mapping_version":MAPPING_VERSION,"source_revision":"c","regions":[]}),
            json!({"schema":REGIONS_SCHEMA,"mapping_version":"wrong","source_revision":"c","regions":[]}),
            json!({"schema":REGIONS_SCHEMA,"mapping_version":MAPPING_VERSION,"source_revision":"c","regions":1}),
        ] {
            assert!(
                parse_region_artifact_with_revision(&write_value(
                    &directory,
                    "bad-regions.json",
                    &bad
                ))
                .is_err()
            );
        }
        let mut missing_revision = canonical.clone();
        missing_revision
            .as_object_mut()
            .unwrap()
            .remove("source_revision");
        assert!(
            parse_region_artifact_with_revision(&write_value(
                &directory,
                "missing-revision.json",
                &missing_revision
            ))
            .is_err()
        );
        let mut missing_schema = canonical.clone();
        missing_schema.as_object_mut().unwrap().remove("schema");
        assert!(
            parse_region_artifact_with_revision(&write_value(
                &directory,
                "missing-region-schema.json",
                &missing_schema
            ))
            .is_err()
        );
        let mut missing_mapping = canonical.clone();
        missing_mapping
            .as_object_mut()
            .unwrap()
            .remove("mapping_version");
        assert!(
            parse_region_artifact_with_revision(&write_value(
                &directory,
                "missing-region-mapping.json",
                &missing_mapping
            ))
            .is_err()
        );
        for field in [
            "path",
            "start_line",
            "end_line",
            "discriminator",
            "source_hash",
        ] {
            let mut missing = canonical.clone();
            missing["regions"][0].as_object_mut().unwrap().remove(field);
            assert!(
                parse_region_artifact_with_revision(&write_value(
                    &directory,
                    "missing-region-field.json",
                    &missing
                ))
                .is_err()
            );
        }
        let mut bad_region = canonical.clone();
        bad_region["regions"] = json!([1]);
        assert!(
            parse_region_artifact_with_revision(&write_value(
                &directory,
                "bad-region.json",
                &bad_region
            ))
            .is_err()
        );
        bad_region["regions"] = json!([{"path":"src.rs","start_line":2,"end_line":1,"discriminator":"r1","source_hash":"a"}]);
        assert!(
            parse_region_artifact_with_revision(&write_value(
                &directory,
                "backwards-region.json",
                &bad_region
            ))
            .is_err()
        );
        bad_region["regions"] =
            json!([{"path":"src.rs","start_line":1,"discriminator":"r1","source_hash":"a"}]);
        assert!(
            parse_region_artifact_with_revision(&write_value(
                &directory,
                "missing-region-end.json",
                &bad_region
            ))
            .is_err()
        );
        bad_region["regions"] = json!([{"path":"src.rs","start_line":1,"end_line":1,"discriminator":"r1","hits":-1,"source_hash":"a"}]);
        assert!(
            parse_region_artifact_with_revision(&write_value(
                &directory,
                "negative-hits.json",
                &bad_region
            ))
            .is_err()
        );
        bad_region["regions"] = json!([{"path":"src.rs","start_line":1,"end_line":1,"discriminator":"r1","hits":"1","source_hash":"a"}]);
        assert!(
            parse_region_artifact_with_revision(&write_value(
                &directory,
                "string-hits.json",
                &bad_region
            ))
            .is_err()
        );

        let coveragepy = json!({"files":{"src/a.py":{"executed_lines":[1],"missing_lines":[2]}}});
        let coveragepy_path = write_value(&directory, "coveragepy.json", &coveragepy);
        assert_eq!(
            derive_standard_regions(&coveragepy_path, "coveragepy", None)
                .unwrap()
                .len(),
            2
        );
        assert!(
            derive_standard_regions(
                &directory.path().join("missing-coveragepy.json"),
                "coveragepy",
                None
            )
            .is_err()
        );
        assert!(derive_standard_regions(&coveragepy_path, "coverage.py", None).is_ok());
        let mut bad_coveragepy = json!({});
        fs::write(directory.path().join("invalid-coveragepy.json"), b"{").unwrap();
        assert!(
            derive_standard_regions(
                &directory.path().join("invalid-coveragepy.json"),
                "coveragepy",
                None
            )
            .is_err()
        );
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-coveragepy.json", &bad_coveragepy),
                "coveragepy",
                None
            )
            .is_err()
        );
        bad_coveragepy = json!({"files":{"src/a.py":1}});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-coveragepy-file.json", &bad_coveragepy),
                "coveragepy",
                None
            )
            .is_err()
        );
        bad_coveragepy = json!({"files":{"src/a.py":{"executed_lines":1,"missing_lines":[]}}});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-coveragepy-lines.json", &bad_coveragepy),
                "coveragepy",
                None
            )
            .is_err()
        );

        let istanbul = json!({"src/app.js":{"statementMap":{"0":{"start":{"line":1},"end":{"line":2}}},"s":{"0":1}}});
        let istanbul_path = write_value(&directory, "istanbul.json", &istanbul);
        assert_eq!(
            derive_standard_regions(&istanbul_path, "istanbul", None)
                .unwrap()
                .len(),
            1
        );
        assert!(
            derive_standard_regions(
                &directory.path().join("missing-istanbul.json"),
                "istanbul",
                None
            )
            .is_err()
        );
        fs::write(directory.path().join("invalid-istanbul.json"), b"{").unwrap();
        assert!(
            derive_standard_regions(
                &directory.path().join("invalid-istanbul.json"),
                "istanbul",
                None
            )
            .is_err()
        );
        let mut bad_istanbul = json!(1);
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-istanbul-root.json", &bad_istanbul),
                "istanbul",
                None
            )
            .is_err()
        );
        bad_istanbul = json!({"src/app.js":1});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-istanbul-file.json", &bad_istanbul),
                "istanbul",
                None
            )
            .is_err()
        );
        bad_istanbul = json!({"src/app.js":{}});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-istanbul-map.json", &bad_istanbul),
                "istanbul",
                None
            )
            .is_err()
        );
        bad_istanbul = json!({"src/app.js":{"statementMap":{},"s":{}}});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-istanbul-counts.json", &bad_istanbul),
                "istanbul",
                None
            )
            .is_ok()
        );
        bad_istanbul = json!({"src/app.js":{"statementMap":{},"s":1}});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-istanbul-counts-object.json", &bad_istanbul),
                "istanbul",
                None
            )
            .is_err()
        );
        bad_istanbul = json!({"src/app.js":{"statementMap":{"0":1},"s":{"0":1}}});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-istanbul-location.json", &bad_istanbul),
                "istanbul",
                None
            )
            .is_err()
        );
        bad_istanbul =
            json!({"src/app.js":{"statementMap":{"0":{"start":{},"end":{"line":1}}},"s":{"0":1}}});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-istanbul-start.json", &bad_istanbul),
                "istanbul",
                None
            )
            .is_err()
        );
        bad_istanbul =
            json!({"src/app.js":{"statementMap":{"0":{"start":{"line":1},"end":{}}},"s":{"0":1}}});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-istanbul-end.json", &bad_istanbul),
                "istanbul",
                None
            )
            .is_err()
        );
        bad_istanbul = json!({"src/app.js":{"statementMap":{"0":{"start":{"line":1},"end":{"line":1}}},"s":{"0":-1}}});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-istanbul-negative.json", &bad_istanbul),
                "istanbul",
                None
            )
            .is_err()
        );
        bad_istanbul = json!({"src/app.js":{"statementMap":{"0":{"start":{"line":1},"end":{"line":1}}},"s":{}}});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "bad-istanbul-count.json", &bad_istanbul),
                "istanbul",
                None
            )
            .is_err()
        );
    }

    #[test]
    fn llvm_and_fallback_region_formats_cover_shape_errors() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("invalid-llvm.json"), b"{").unwrap();
        assert!(
            derive_standard_regions(&directory.path().join("invalid-llvm.json"), "llvm", None)
                .is_err()
        );
        assert!(
            derive_standard_regions(&directory.path().join("missing-llvm.json"), "llvm", None)
                .is_err()
        );
        let llvm = json!({"data":[{"files":[{"filename":"src.rs","segments":[[1,1,0,true,true],[2,1,2,true,true],[3,1,0,false,false]]}]}]});
        let llvm_path = write_value(&directory, "llvm.json", &llvm);
        let regions = derive_standard_regions(&llvm_path, "llvm", None).unwrap();
        assert_eq!(regions.len(), 2);
        assert!(regions[0].end_line >= regions[0].start_line);
        for bad in [
            json!({}),
            json!({"data":[{}]}),
            json!({"data":[{"files":[1]}]}),
            json!({"data":[{"files":[{}]}]}),
            json!({"data":[{"files":[{"filename":"src.rs"}]}]}),
            json!({"data":[{"files":[{"filename":"src.rs","segments":[[1,1,"bad",true,true]]}]}]}),
            json!({"data":[{"files":[{"filename":"src.rs","segments":[[null,1,0,true,true]]}]}]}),
            json!({"data":[{"files":[{"filename":"src.rs","segments":[[1,null,0,true,true],[2,1,0,true,true]]}]}]}),
            json!({"data":[{"files":[{"filename":"src.rs","segments":[[1,1,0,true,true],[null,1,0,true,true]]}]}]}),
            json!({"data":[{"files":[{"filename":"src.rs","segments":[[1,1,0,true,true],[2,null,0,true,true]]}]}]}),
        ] {
            assert!(
                derive_standard_regions(
                    &write_value(&directory, "bad-llvm.json", &bad),
                    "llvm",
                    None
                )
                .is_err()
            );
        }
        let skipped_segment = json!({"data":[{"files":[{"filename":"src.rs","segments":[1]}]}]});
        assert!(
            derive_standard_regions(
                &write_value(&directory, "skipped-llvm-segment.json", &skipped_segment),
                "llvm",
                None
            )
            .is_ok()
        );
        let lcov = "TN:\nSF:src.rs\nDA:1,1\nDA:2,0\nend_of_record\n";
        let lcov_path = directory.path().join("fallback.lcov");
        fs::write(&lcov_path, lcov).unwrap();
        assert_eq!(
            derive_standard_regions(&lcov_path, "lcov", None)
                .unwrap()
                .len(),
            2
        );
        assert!(derive_standard_regions(&lcov_path, "unknown", None).is_err());
        for format in [
            "coverage.py",
            "coveragepy",
            "python",
            "istanbul",
            "javascript",
            "js",
            "llvm",
            "llvm-json",
            REGIONS_SCHEMA,
            INVENTORY_FORMAT,
            "other",
        ] {
            assert!(!normalize_composite_format(format).is_empty());
        }
        assert!(validate_inventory_path("src/a.rs").is_ok());
        assert!(validate_inventory_path("../a.rs").is_err());
        assert!(validate_inventory_path("/a.rs").is_err());
        assert!(validate_sha256(&"a".repeat(64), "test").is_ok());
        assert!(validate_sha256("bad", "test").is_err());
        assert!(string_array(None, "values").unwrap().is_empty());
        assert!(string_array(Some(&json!(1)), "values").is_err());
        assert!(string_array(Some(&json!([1])), "values").is_err());
        assert!(positive_integer(Some(&json!(1)), "line").is_ok());
        assert!(positive_integer(Some(&json!(0)), "line").is_err());
        assert!(positive_integer(Some(&json!("1")), "line").is_err());
        assert!(positive_or_zero(&json!(0), "hits").is_ok());
        assert!(positive_or_zero(&json!(-1), "hits").is_err());
        assert!(positive_or_zero(&json!("1"), "hits").is_err());
        assert!(integer_array(Some(&json!([1, 2])), "lines").is_ok());
        assert!(integer_array(Some(&json!([])), "lines").is_ok());
        assert!(integer_array(Some(&json!(1)), "lines").is_err());
        assert!(location_line(Some(&json!({"line": 1})), "location").is_ok());
        assert!(location_line(Some(&json!({})), "location").is_err());
        assert!(location_line(Some(&json!(1)), "location").is_err());
        assert!(value_i64(Some(&json!(1))).is_ok());
        assert!(value_i64(Some(&json!("1"))).is_err());
    }

    #[test]
    fn assembly_state_matrix_covers_denominator_and_provenance_rules() {
        let directory = tempfile::tempdir().unwrap();
        let incomplete = incomplete_build("missing".to_owned(), json!({}), json!({}));
        assert_eq!(incomplete.components.len(), 3);
        let source_path = directory.path().join("src.rs");
        fs::write(&source_path, "fn main() {}\n").unwrap();
        let hash = source_hash(&source_path).unwrap();
        let inventory_value = json!({
            "schema": INVENTORY_SCHEMA,
            "mapping_version": MAPPING_VERSION,
            "source_revision": "commit",
            "entries": [{
                "logical_source_id":"src:rs",
                "path":"src.rs",
                "package":"demo",
                "component_id":"rust",
                "language":"rust",
                "role":"production",
                "source_hash":hash,
                "variants":["default","gpu"],
                "expected_formats":[REGIONS_SCHEMA],
                "regions":[{"start_line":1,"end_line":1,"discriminator":"r1"}]
            }]
        });
        let inventory_path = write_value(&directory, "assembly-inventory.json", &inventory_value);
        let inventory = parse_inventory(&inventory_path).unwrap();
        let descriptor = |variant: &str, format: &str, aliases: Vec<LogicalSourceAlias>| {
            CompositeArtifactDescriptor {
                component_id: "rust".to_owned(),
                package_variant: variant.to_owned(),
                language: "rust".to_owned(),
                format: format.to_owned(),
                inventory_artifact: "assembly-inventory.json".to_owned(),
                logical_source_aliases: aliases,
                role: "coverage".to_owned(),
                toolchain_versions: json!({}),
            }
        };
        let region = |covered: bool, logical_source_id: Option<&str>, source_hash: Option<&str>| {
            CanonicalRegion {
                path: "src.rs".to_owned(),
                logical_source_id: logical_source_id.map(str::to_owned),
                start_line: 1,
                end_line: 1,
                discriminator: "r1".to_owned(),
                hits: i64::from(covered),
                covered,
                source_hash: source_hash.map(str::to_owned),
            }
        };
        let input =
            |variant: &str, status: &str, regions: Vec<CanonicalRegion>| CompositeComponentInput {
                descriptor: descriptor(
                    variant,
                    REGIONS_SCHEMA,
                    vec![LogicalSourceAlias {
                        path: "src.rs".to_owned(),
                        logical_source_id: "src:rs".to_owned(),
                    }],
                ),
                artifact_path: directory.path().join(format!("{variant}.json")),
                artifact_hash: None,
                snapshot_id: None,
                source_revision: None,
                artifact_status: status.to_owned(),
                reused_from_snapshot_id: None,
                regions,
            };
        for status in ["missing", "skipped_stale", "unmeasured"] {
            let result = assemble(
                &inventory,
                &[input("default", status, Vec::new())],
                directory.path(),
                Some("commit"),
                false,
            );
            assert_eq!(result.status, "incomplete");
        }
        let no_descriptor = assemble(&inventory, &[], directory.path(), Some("commit"), false);
        assert!(
            no_descriptor
                .reasons
                .iter()
                .any(|reason| reason.contains("missing required"))
        );
        let empty_inventory = Inventory {
            entries: Vec::new(),
            ..inventory.clone()
        };
        let no_production = assemble(
            &empty_inventory,
            &[],
            directory.path(),
            Some("other"),
            false,
        );
        assert!(
            no_production
                .reasons
                .iter()
                .any(|reason| reason.contains("no production"))
        );
        let unmeasured = assemble(
            &inventory,
            &[input("default", "ingested", Vec::new())],
            directory.path(),
            Some("commit"),
            false,
        );
        assert_eq!(unmeasured.regions[0]["variants"][0]["state"], "unmeasured");
        let uncovered = assemble(
            &inventory,
            &[input(
                "default",
                "ingested",
                vec![region(false, None, None)],
            )],
            directory.path(),
            Some("commit"),
            false,
        );
        assert_eq!(uncovered.regions[0]["variants"][0]["state"], "uncovered");
        let mut single_variant_inventory = inventory.clone();
        single_variant_inventory.entries[0].variants = vec!["default".to_owned()];
        let uncovered_top_level = assemble(
            &single_variant_inventory,
            &[input(
                "default",
                "ingested",
                vec![region(false, None, None)],
            )],
            directory.path(),
            Some("commit"),
            false,
        );
        assert_eq!(uncovered_top_level.regions[0]["state"], "uncovered");
        let covered = assemble(
            &inventory,
            &[
                input("default", "ingested", vec![region(true, None, Some(&hash))]),
                input("gpu", "skipped_stale", Vec::new()),
            ],
            directory.path(),
            Some("commit"),
            false,
        );
        assert_eq!(covered.regions[0]["variants"][0]["state"], "covered");
        assert_eq!(covered.status, "incomplete");
        let mut no_toolchain = input(
            "default",
            "ingested",
            vec![region(true, Some("src:rs"), None)],
        );
        no_toolchain.descriptor.toolchain_versions = json!(1);
        no_toolchain.reused_from_snapshot_id = Some("baseline".to_owned());
        let no_toolchain_build = assemble(
            &single_variant_inventory,
            &[no_toolchain],
            directory.path(),
            Some("commit"),
            false,
        );
        assert_eq!(
            no_toolchain_build.component_evidence[0]["evidence_source"],
            "baseline_snapshot"
        );
        let dirty = assemble(
            &inventory,
            &[input(
                "default",
                "ingested",
                vec![region(true, Some("src:rs"), None)],
            )],
            directory.path(),
            Some("commit"),
            true,
        );
        assert!(dirty.reasons.iter().any(|reason| reason.contains("dirty")));
        let revision_mismatch = CompositeComponentInput {
            source_revision: Some("other".to_owned()),
            ..input(
                "default",
                "ingested",
                vec![region(true, Some("src:rs"), None)],
            )
        };
        assert!(
            assemble(
                &inventory,
                &[revision_mismatch],
                directory.path(),
                Some("commit"),
                false,
            )
            .reasons
            .iter()
            .any(|reason| reason.contains("source revision"))
        );
        assert!(
            assemble(&inventory, &[], directory.path(), Some("other"), false,)
                .reasons
                .iter()
                .any(|reason| reason.contains("does not match"))
        );
        let duplicate = input(
            "default",
            "ingested",
            vec![region(true, Some("src:rs"), None)],
        );
        assert!(
            assemble(
                &inventory,
                &[duplicate.clone(), duplicate],
                directory.path(),
                Some("commit"),
                false,
            )
            .reasons
            .iter()
            .any(|reason| reason.contains("duplicate evidence"))
        );
        let mut no_alias = input("default", "ingested", vec![region(true, None, None)]);
        no_alias.descriptor.logical_source_aliases.clear();
        assert!(
            assemble(
                &inventory,
                &[no_alias],
                directory.path(),
                Some("commit"),
                false,
            )
            .reasons
            .iter()
            .any(|reason| reason.contains("without an explicit"))
        );
        let unowned = input(
            "default",
            "ingested",
            vec![CanonicalRegion {
                discriminator: "other".to_owned(),
                ..region(true, Some("src:rs"), None)
            }],
        );
        assert!(
            assemble(
                &inventory,
                &[unowned],
                directory.path(),
                Some("commit"),
                false
            )
            .reasons
            .iter()
            .any(|reason| reason.contains("unowned"))
        );
        let source_mismatch = input(
            "default",
            "ingested",
            vec![region(true, Some("src:rs"), Some(&"b".repeat(64)))],
        );
        assert!(
            assemble(
                &inventory,
                &[source_mismatch],
                directory.path(),
                Some("commit"),
                false,
            )
            .reasons
            .iter()
            .any(|reason| reason.contains("source hash mismatch"))
        );
        let wrong_format = CompositeComponentInput {
            descriptor: descriptor("default", "llvm", Vec::new()),
            ..input("default", "llvm", vec![region(true, Some("src:rs"), None)])
        };
        assert!(
            assemble(
                &inventory,
                &[wrong_format],
                directory.path(),
                Some("commit"),
                false
            )
            .reasons
            .iter()
            .any(|reason| reason.contains("expected format"))
        );
        let not_owned = CompositeComponentInput {
            descriptor: CompositeArtifactDescriptor {
                component_id: "python".to_owned(),
                ..descriptor("default", REGIONS_SCHEMA, Vec::new())
            },
            ..input(
                "default",
                "ingested",
                vec![region(true, Some("src:rs"), None)],
            )
        };
        assert!(
            assemble(
                &inventory,
                &[not_owned],
                directory.path(),
                Some("commit"),
                false,
            )
            .reasons
            .iter()
            .any(|reason| reason.contains("not owned"))
        );

        let mut empty_variant_inventory = inventory.clone();
        empty_variant_inventory.entries[0].variants.clear();
        let empty_variant = assemble(
            &empty_variant_inventory,
            &[input(
                "default",
                "ingested",
                vec![region(true, Some("src:rs"), None)],
            )],
            directory.path(),
            Some("commit"),
            false,
        );
        assert_eq!(empty_variant.regions[0]["variants"][0]["state"], "covered");
        let mut actual_hash_mismatch = empty_variant_inventory.clone();
        actual_hash_mismatch.entries[0].source_hash = "e".repeat(64);
        assert!(
            assemble(
                &actual_hash_mismatch,
                &[input(
                    "default",
                    "ingested",
                    vec![region(true, Some("src:rs"), None)],
                )],
                directory.path(),
                Some("commit"),
                false,
            )
            .reasons
            .iter()
            .any(|reason| reason.contains("source hash mismatch"))
        );
        let unmeasured_top_level = assemble(
            &single_variant_inventory,
            &[input("default", "ingested", Vec::new())],
            directory.path(),
            Some("commit"),
            false,
        );
        assert_eq!(unmeasured_top_level.regions[0]["state"], "unmeasured");

        let missing_source_inventory = Inventory {
            entries: vec![InventoryEntry {
                logical_source_id: "missing".to_owned(),
                path: "does-not-exist.rs".to_owned(),
                package: "demo".to_owned(),
                component_id: "other".to_owned(),
                language: "go".to_owned(),
                role: "production".to_owned(),
                source_hash: "c".repeat(64),
                variants: vec!["default".to_owned()],
                expected_formats: vec![REGIONS_SCHEMA.to_owned()],
                regions: vec![InventoryRegion {
                    start_line: 1,
                    end_line: 1,
                    discriminator: "g1".to_owned(),
                }],
            }],
            ..inventory.clone()
        };
        let unsupported = input("default", "ingested", Vec::new());
        let unsupported = CompositeComponentInput {
            descriptor: CompositeArtifactDescriptor {
                component_id: "other".to_owned(),
                package_variant: "default".to_owned(),
                language: "go".to_owned(),
                ..unsupported.descriptor
            },
            ..unsupported
        };
        let unsupported_result = assemble(
            &missing_source_inventory,
            &[unsupported],
            directory.path(),
            Some("commit"),
            false,
        );
        assert!(
            unsupported_result
                .reasons
                .iter()
                .any(|reason| reason.contains("unsupported"))
        );
        assert!(
            unsupported_result
                .reasons
                .iter()
                .any(|reason| reason.contains("missing"))
        );
        assert_eq!(remediation_for_state("unknown"), "none");
        let catalogue = InventoryEntry {
            logical_source_id: "catalogue".to_owned(),
            path: "catalogue.rs".to_owned(),
            package: "demo".to_owned(),
            component_id: "rust".to_owned(),
            language: "rust".to_owned(),
            role: "catalogue".to_owned(),
            source_hash: "d".repeat(64),
            variants: Vec::new(),
            expected_formats: Vec::new(),
            regions: Vec::new(),
        };
        assert!(expected_regions(&[&catalogue]).is_empty());
        let mut language_expected = BTreeMap::new();
        for (key, language) in [("py", "python"), ("js", "javascript")] {
            language_expected.insert(
                key.to_owned(),
                ExpectedRegion {
                    logical_source_id: key.to_owned(),
                    path: format!("{key}.src"),
                    component_id: key.to_owned(),
                    language: language.to_owned(),
                    package: "demo".to_owned(),
                    source_hash: "a".repeat(64),
                    variants: vec!["default".to_owned()],
                    expected_formats: vec![REGIONS_SCHEMA.to_owned()],
                    region: InventoryRegion {
                        start_line: 1,
                        end_line: 1,
                        discriminator: key.to_owned(),
                    },
                },
            );
        }
        let summary = component_summaries(&language_expected, &BTreeSet::new(), &BTreeSet::new());
        assert_eq!(summary.len(), 3);
    }
}
