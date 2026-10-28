"""Render a self-contained HTML view from a sanitized synthetic run report."""

from __future__ import annotations

import argparse
import html
import json
from datetime import UTC, datetime
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("data/demo"), help="pipeline root whose current generation should be reported")
    parser.add_argument("--input", type=Path, help="explicit saved run report; defaults to the current published run under --root")
    parser.add_argument("--output", type=Path, default=Path("reports/synthetic-dashboard.html"))
    args = parser.parse_args()
    if args.input is None:
        pointer = json.loads((args.root / "tables" / "gold" / "current_generation.json").read_text(encoding="utf-8"))
        input_path = args.root / "evidence" / "runs" / f"{pointer['run_id']}.json"
    else:
        input_path = args.input
    report = json.loads(input_path.read_text(encoding="utf-8"))
    if report.get("synthetic") is not True:
        raise ValueError("The portfolio report renderer accepts only explicitly synthetic evidence")
    daily = report.get("daily_delivered", [])
    exceptions = report.get("invoice_exceptions", [])
    net_amounts = report.get("invoice_net_amounts", [])
    freshness = report.get("source_freshness", [])
    missing = [row for row in freshness if row.get("freshness_status") == "MISSING_EXPECTED_FILE"]
    stale = [row for row in freshness if str(row.get("freshness_status", "")).startswith("STALE_")]
    arrived = [row for row in freshness if int(row.get("files_received_this_run", 0)) > 0]
    changes = report.get("changes_from_previous_generation")
    totals = [float(row["accepted_net_quantity"]) for row in daily]
    total_quantity = sum(totals)
    daily_rows = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(row.get(key, '')))}</td>" for key in ("project_id", "material_id", "business_date", "accepted_net_quantity")) + "</tr>"
        for row in daily
    ) or '<tr><td colspan="4">No accepted deliveries in this generation</td></tr>'
    exception_rows = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(row.get(key, '')))}</td>" for key in ("invoice_id", "invoice_line_id", "exception_reason")) + "</tr>"
        for row in exceptions
    ) or '<tr><td colspan="3">No invoice exceptions</td></tr>'
    net_rows = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(row.get(key, '')))}</td>" for key in ("company_id", "supplier_id", "currency", "net_invoice_amount")) + "</tr>"
        for row in net_amounts
    ) or '<tr><td colspan="4">No invoice amounts</td></tr>'
    sources = "".join(
        f"<li>{html.escape(feed.replace('_', ' '))}: {int(count)} row(s)</li>"
        for feed, count in sorted(report.get("source_rows_by_feed", {}).items())
    )
    generated = datetime.now(UTC).isoformat()
    change_summary = "No prior successful generation is available for comparison."
    if changes:
        change_summary = (
            f"Compared with {html.escape(str(changes.get('previous_generation', 'unknown')))}: "
            f"accepted quantity {html.escape(str(changes['accepted_net_quantity']['previous']))} → "
            f"{html.escape(str(changes['accepted_net_quantity']['current']))}; invoice exceptions "
            f"{int(changes['invoice_exception_count']['previous'])} → {int(changes['invoice_exception_count']['current'])}; "
            f"quarantined rows {int(changes['quarantine_rows']['previous'])} → {int(changes['quarantine_rows']['current'])}."
        )
    run_status = str(report.get("status", "UNKNOWN"))
    published = report.get("published") is True
    publication_text = (f"Consumers should resolve all products through {report.get('publication_pointer', 'the current-generation pointer')}; "
                        f"it names generation {report.get('generation', 'unknown')}." if published else
                        "This run did not publish. Consumers must continue using the last successful generation.")
    source_summary = f"{len(arrived)} of {len(freshness)} expected feeds arrived; {len(missing)} missing and {len(stale)} stale."
    content = f"""<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Synthetic data pipeline operations report</title>
<style>
body{{font:16px/1.5 system-ui,sans-serif;background:#f4f6f8;color:#18212b;margin:0}}main{{max-width:1050px;margin:2rem auto;padding:0 1rem}}header,.card{{background:white;border:1px solid #dce2e8;border-radius:12px;padding:1rem 1.25rem;margin-bottom:1rem}}.banner{{color:#7b3300;background:#fff0df;border-color:#edc59f;font-weight:700}}.metrics{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:1rem}}.metric{{font-size:1.7rem;font-weight:700}}table{{border-collapse:collapse;width:100%}}th,td{{padding:.55rem;text-align:left;border-bottom:1px solid #e5e9ed}}th{{background:#eef2f5}}small,.muted{{color:#52606d}}h1,h2{{line-height:1.2}}
</style><main>
<header class="banner">SYNTHETIC DEMONSTRATION DATA — fictional project and source records</header>
<header><h1>Data pipeline operations report</h1><p><strong>Run outcome: {html.escape(run_status)}</strong> · Published: {'yes' if published else 'no'}</p><p>{html.escape(publication_text)}</p><p class="muted">Run {html.escape(str(report.get('run_id', 'unknown')))} started {html.escape(str(report.get('started_at_utc', 'unknown')))} and finished {html.escape(str(report.get('finished_at_utc', 'unknown')))} · Report exported {html.escape(generated)}</p></header>
<section class="card"><h2>Source arrival and freshness</h2><p>{html.escape(source_summary)}</p><p>Arrived: {', '.join(html.escape(str(row['source_feed']).replace('_', ' ')) for row in arrived) or 'none'}.</p><p>Missing: {', '.join(html.escape(str(row['source_feed']).replace('_', ' ')) for row in missing) or 'none'}.</p><p>Stale: {', '.join(html.escape(str(row['source_feed']).replace('_', ' ')) for row in stale) or 'none'}.</p></section>
<section class="card"><h2>What changed</h2><p>{change_summary}</p></section>
<section class="metrics"><article class="card"><div class="metric">{total_quantity:.3f} t</div>Accepted net quantity</article><article class="card"><div class="metric">{len(exceptions)}</div>Invoice exceptions</article><article class="card"><div class="metric">{int(report.get('quarantine_rows', 0))}</div>Quarantined source rows</article><article class="card"><div class="metric">{int(report.get('duplicate_business_envelopes_suppressed', 0))}</div>Replay duplicates suppressed</article></section>
<section class="card"><h2>Daily accepted deliveries</h2><table><thead><tr><th>Project</th><th>Material</th><th>Receipt date</th><th>Accepted metric tonnes</th></tr></thead><tbody>{daily_rows}</tbody></table></section>
<section class="card"><h2>Invoice exceptions</h2><table><thead><tr><th>Invoice</th><th>Line</th><th>Reason</th></tr></thead><tbody>{exception_rows}</tbody></table><p class="muted">This report identifies exceptions only. It never initiates payments.</p></section>
<section class="card"><h2>Net invoice amounts</h2><table><thead><tr><th>Company</th><th>Supplier</th><th>Currency</th><th>Net amount</th></tr></thead><tbody>{net_rows}</tbody></table></section>
<section class="card"><h2>Source presence and update age</h2><table><thead><tr><th>Feed</th><th>Status</th><th>Files in run</th><th>Rows in run</th><th>Latest source update</th></tr></thead><tbody>{''.join('<tr>' + ''.join(f'<td>{html.escape(str(row.get(key, "")))}</td>' for key in ('source_feed', 'freshness_status', 'files_received_this_run', 'rows_received_this_run', 'latest_source_updated_at_utc')) + '</tr>' for row in freshness) or '<tr><td colspan="5">No source freshness evidence in this report</td></tr>'}</tbody></table><p class="muted">The 72-hour threshold is a synthetic demo rule, not a source SLA.</p></section>
<section class="card"><h2>Input row counts</h2><ul>{sources}</ul><p class="muted">Counts come from the selected run report. They do not establish source completeness or live freshness.</p></section>
</main></html>"""
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content, encoding="utf-8")
    print(f"Wrote synthetic report: {args.output}")


if __name__ == "__main__":
    main()
