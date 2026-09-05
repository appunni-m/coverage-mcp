//! Marginal coverage against a fixed, complete observation inventory.
use super::*;

// LLVM summaries combine generic instantiations at the same source span.
// Function names remain display labels, not separate coverage denominators.
fn inventory(
    measurement: &Measurement,
    root: &str,
    metric: &str,
) -> AppResult<BTreeMap<String, Observation>> {
    let mut rows: BTreeMap<String, Observation> = BTreeMap::new();
    for row in measurement_observations(measurement, root)?
        .into_values()
        .filter(|row| row.kind == metric)
    {
        let key = row
            .identity
            .clone()
            .unwrap_or_else(|| format!("{}:{:?}:{}", row.path, row.span, row.arm));
        rows.entry(key)
            .and_modify(|existing| existing.hits = existing.hits.max(row.hits))
            .or_insert(row);
    }
    Ok(rows)
}

fn totals(rows: &BTreeMap<String, Observation>) -> Value {
    let covered = rows.values().filter(|row| row.hits > 0).count();
    json!({"covered":covered,"total":rows.len(),"missing":rows.len()-covered})
}

fn detail_matches(
    measurement: &Measurement,
    rows: &BTreeMap<String, Observation>,
    metric: &str,
) -> bool {
    let observed = totals(rows);
    let reported = counts(&measurement.report, metric);
    !rows.is_empty()
        && rows.values().all(|row| row.hits >= 0)
        && observed["total"] == reported["total"]
        && observed["covered"] == reported["covered"]
}

fn conflict(base: &Measurement, batch: &Measurement) -> bool {
    base.source_state == "changed_or_missing"
        || batch.source_state == "changed_or_missing"
        || (base.context["build_id"].is_string()
            && batch.context["build_id"].is_string()
            && base.context["build_id"] != batch.context["build_id"])
        || batch.report.files.iter().any(|file| {
            let a = &base.context["source_hashes"][&file.file_path];
            let b = &batch.context["source_hashes"][&file.file_path];
            a.is_string() && b.is_string() && a != b
        })
}

fn verified(base: &Measurement, batch: &Measurement) -> bool {
    base.source_state == "matches_receipt"
        && batch.source_state == "matches_receipt"
        && base.context["build_id"] == batch.context["build_id"]
        && base.context["test_status"] == "passed"
        && batch.context["test_status"] == "passed"
}

pub(super) fn compare_incremental(
    reader: &Reader<'_>,
    request: &CompareRequest,
    current: &Measurement,
    baseline: &Measurement,
    metric: &str,
) -> AppResult<Value> {
    let root = &reader.context.checkout_path;
    let mut union = inventory(baseline, root, metric)?;
    let mut result = json!({"status":"incomparable","scope":"incremental","measurement":summary(current,metric),"baseline":summary(baseline,metric),"reason":"","incremental":null,"changes":null,"gained":[],"gained_group_count":0,"remaining":[],"remaining_group_count":0,"regression_checked":false});
    if baseline.context["test_status"] == "failed" {
        result["status"] = json!("tests_failed");
        result["reason"] = json!(
            "The baseline tests failed. Choose a passing baseline; no combined coverage was calculated."
        );
    } else if !detail_matches(baseline, &union, metric) {
        result["reason"] = json!(
            "Baseline detail is incomplete or differs from its reported totals. Export exact coverage for the selected metric before combining reports."
        );
    } else {
        return combine(
            reader, request, current, baseline, metric, &mut union, result,
        );
    }
    Ok(reader.envelope(result, None, None))
}

fn combine(
    reader: &Reader<'_>,
    request: &CompareRequest,
    current: &Measurement,
    baseline: &Measurement,
    metric: &str,
    union: &mut BTreeMap<String, Observation>,
    mut result: Value,
) -> AppResult<Value> {
    let mut trusted = true;
    let mut known_build = baseline.context["build_id"].as_str().map(str::to_owned);
    // Read prior reports one at a time; memory follows the baseline inventory,
    // not the number of test executions or accumulated report artifacts.
    for reference in &request.previous {
        let batch = load(reader, Some(reference), None)?;
        let rows = inventory(&batch, &reader.context.checkout_path, metric)?;
        if let Some((status, reason)) = reject(
            baseline,
            &batch,
            union,
            &rows,
            metric,
            known_build.as_deref(),
        ) {
            result["status"] = json!(status);
            result["reason"] = json!(format!("Earlier batch {reference}: {reason}"));
            return Ok(reader.envelope(result, None, None));
        }
        if known_build.is_none() {
            known_build = batch.context["build_id"].as_str().map(str::to_owned);
        }
        trusted &= verified(baseline, &batch);
        for (key, row) in rows {
            if row.hits > 0 {
                union.insert(key, row);
            }
        }
    }
    let rows = inventory(current, &reader.context.checkout_path, metric)?;
    if let Some((status, reason)) = reject(
        baseline,
        current,
        union,
        &rows,
        metric,
        known_build.as_deref(),
    ) {
        result["status"] = json!(status);
        result["reason"] = json!(reason);
        return Ok(reader.envelope(result, None, None));
    }
    trusted &= verified(baseline, current);
    let accepted = totals(union);
    let gained: Vec<_> = rows
        .iter()
        .filter(|(key, row)| row.hits > 0 && union[*key].hits == 0)
        .map(|(_, row)| row)
        .collect();
    let count = gained.len();
    let batch_covered = rows.values().filter(|row| row.hits > 0).count();
    let (gained_groups, gained_count) = groups(
        gained
            .into_iter()
            .filter(|row| row.matches(request.query.as_deref(), None)),
        request.limit,
        0,
    );
    for (key, row) in rows {
        if row.hits > 0 {
            union.insert(key, row);
        }
    }
    let (remaining, remaining_count) = groups(
        union
            .values()
            .filter(|row| row.hits == 0 && row.matches(request.query.as_deref(), None)),
        request.limit,
        0,
    );
    let missing = accepted["missing"].as_u64().unwrap_or(0);
    result["status"] = json!(if !trusted {
        "limited"
    } else if count == 0 {
        "no_gain"
    } else {
        "improved"
    });
    result["reason"] = json!(if trusted {
        "Baseline and batches have matching source/build receipts and passing tests. Combined coverage is their union; the full suite was not rerun and regression status is unknown."
    } else {
        "Unverified incremental estimate: source/build identity or passing test evidence is missing. Combined counts are coordinate evidence only; full-suite regression status is unknown."
    });
    result["incremental"] = json!({"metric":metric,"evidence":if trusted{"verified"}else{"unverified"},"baseline_scope":baseline.context["scope"].as_str().unwrap_or("unknown"),"previous_batches":request.previous.len(),"accepted":accepted,"batch_covered":batch_covered,"newly_covered":count,"already_covered":batch_covered-count,"combined":totals(union),"gain_percentage_points":100.0*count as f64/union.len() as f64,"remaining_gap_reduction_percent":if missing==0{0.0}else{100.0*count as f64/missing as f64}});
    result["changes"] =
        json!({"newly_observed":count,"not_observed":null,"added":0,"removed":0,"regressed":null});
    result["gained"] = json!(gained_groups);
    result["gained_group_count"] = json!(gained_count);
    result["remaining"] = json!(remaining);
    result["remaining_group_count"] = json!(remaining_count);
    Ok(reader.envelope(result, None, None))
}

fn reject(
    baseline: &Measurement,
    batch: &Measurement,
    union: &BTreeMap<String, Observation>,
    rows: &BTreeMap<String, Observation>,
    metric: &str,
    known_build: Option<&str>,
) -> Option<(&'static str, &'static str)> {
    if batch.context["test_status"] == "failed" {
        Some((
            "tests_failed",
            "Batch tests failed; no combined coverage was calculated.",
        ))
    } else if baseline.report.format != batch.report.format
        || conflict(baseline, batch)
        || known_build
            .zip(batch.context["build_id"].as_str())
            .is_some_and(|(known, batch)| known != batch)
    {
        Some((
            "incomparable",
            "Source, build, or report format differs from the baseline. Rebuild a compatible baseline; no combined coverage was calculated.",
        ))
    } else if !detail_matches(batch, rows, metric) {
        Some((
            "incomparable",
            "Batch detail is incomplete or differs from its reported totals. Export exact coverage for the selected metric.",
        ))
    } else if rows.keys().any(|key| !union.contains_key(key)) {
        Some((
            "incomparable",
            "Batch includes observations outside the baseline inventory. Keep instrumentation and measured source fixed, or establish a new baseline.",
        ))
    } else {
        None
    }
}
