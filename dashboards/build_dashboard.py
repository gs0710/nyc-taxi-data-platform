"""Build a static HTML dashboard from the marts schema.

Pure Python and inline SVG: no JavaScript libraries and no internet needed to view it.
Run it inside the loader container so it can reach Postgres (see `make dashboard`).
Exit codes: 0 = written, 1 = database or file error.
"""
import html
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

OUT = Path(os.getenv("DASHBOARD_OUT", "/app/dashboards/index.html"))
DATA = Path(os.getenv("DATA_DIR", "/app/data"))
log = logging.getLogger("dashboard")
BLUE, GREEN, ORANGE = "#2E5597", "#2F855A", "#C05621"

QUERIES = {
    "kpi": """
        SELECT count(*), coalesce(sum(total_amount), 0), avg(fare_amount),
               avg(trip_distance), avg(trip_duration_minutes)
        FROM marts.fact_trips""",
    "card_tip_rate": """
        SELECT sum(tip_amount) / nullif(sum(fare_amount), 0)
        FROM marts.fact_trips WHERE payment_type_key = 1 AND fare_amount > 0""",
    "date_range": """
        SELECT min(d.full_date), max(d.full_date)
        FROM marts.fact_trips f JOIN marts.dim_date d ON d.date_key = f.pickup_date_key""",
    "boroughs": """
        SELECT z.borough, count(*) AS trips
        FROM marts.fact_trips f JOIN marts.dim_zone z ON z.zone_key = f.pickup_zone_key
        GROUP BY 1 ORDER BY 2 DESC""",
    "top_zones": """
        SELECT z.zone_name, count(*) AS trips
        FROM marts.fact_trips f JOIN marts.dim_zone z ON z.zone_key = f.pickup_zone_key
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
    "hours": """
        SELECT pickup_hour, count(*) FROM marts.fact_trips GROUP BY 1 ORDER BY 1""",
    "weekdays": """
        SELECT d.day_of_week, d.day_name, count(*)::float / count(DISTINCT d.full_date)
        FROM marts.fact_trips f JOIN marts.dim_date d ON d.date_key = f.pickup_date_key
        GROUP BY 1, 2 ORDER BY 1""",
    "payments": """
        SELECT p.payment_type_name, count(*) AS trips, avg(f.total_amount), avg(f.tip_amount)
        FROM marts.fact_trips f
        JOIN marts.dim_payment_type p ON p.payment_type_key = f.payment_type_key
        GROUP BY 1 ORDER BY 2 DESC""",
    "loads": """
        SELECT load_id, source_month, rows_in_parquet, rows_loaded, status,
               to_char(finished_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI')
        FROM audit.load_history ORDER BY load_id DESC LIMIT 5""",
}

CSS = """
:root{--bg:#f6f8fb;--card:#fff;--ink:#1a202c;--muted:#64748b;--line:#e2e8f0;--accent:#2E5597}
@media (prefers-color-scheme:dark){:root{--bg:#0f172a;--card:#1e293b;--ink:#e2e8f0;
--muted:#94a3b8;--line:#334155;--accent:#7aa2f7}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font-family:system-ui,Arial,sans-serif;line-height:1.45}
main{max-width:1200px;margin:0 auto;padding:24px 16px 48px}
h1{margin:0 0 4px;font-size:1.7rem}
h2{margin:0 0 10px;font-size:1.05rem}
.muted{color:var(--muted);font-size:.9rem}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:18px 0}
.kpi,.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.kpi b{display:block;font-size:1.45rem;color:var(--accent)}
.kpi span{color:var(--muted);font-size:.82rem}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:14px;margin-bottom:14px}
.card svg{width:100%;height:auto;display:block}
.lbl{font-size:12px;fill:var(--ink)}
.val{font-size:11px;fill:var(--muted)}
.axis{font-size:10px;fill:var(--muted)}
.grid{stroke:var(--line);stroke-width:1}
table{width:100%;border-collapse:collapse;font-size:.9rem}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}
th{color:var(--muted);font-weight:600}
td.n,th.n{text-align:right}
.ok{color:#2F855A;font-weight:600}.bad{color:#C53030;font-weight:600}
footer{margin-top:24px}
@media (max-width:520px){.grid2{grid-template-columns:1fr}}
"""


def esc(value):
    return html.escape(str(value), quote=True)


def fmt_int(n):
    return f"{int(n):,}"


def short(n):
    n = float(n)
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(n) >= div:
            return f"{n / div:.1f}".rstrip("0").rstrip(".") + suffix
    return f"{n:.0f}"


def trunc(text, limit=26):
    text = str(text)
    return text if len(text) <= limit else text[: limit - 1] + "\u2026"


def hbar(items, color=BLUE, width=640, label_w=200, row_h=26):
    """Horizontal bar chart as inline SVG. items = [(label, value)]."""
    if not items:
        return "<p class='muted'>No data</p>"
    vmax = max(v for _, v in items) or 1
    bar_w = width - label_w - 80
    parts = []
    for i, (label, value) in enumerate(items):
        y = i * row_h + 4
        w = max(1.0, value / vmax * bar_w)
        parts.append(
            f'<text x="{label_w - 8}" y="{y + 15}" text-anchor="end" class="lbl">{esc(trunc(label))}</text>'
            f'<rect x="{label_w}" y="{y + 3}" width="{w:.1f}" height="{row_h - 8}" rx="3" fill="{color}">'
            f"<title>{esc(label)}: {fmt_int(value)}</title></rect>"
            f'<text x="{label_w + w + 6:.1f}" y="{y + 15}" class="val">{short(value)}</text>'
        )
    height = row_h * len(items) + 8
    return f'<svg viewBox="0 0 {width} {height}" role="img">{"".join(parts)}</svg>'


def columns(items, color=BLUE, width=700, height=250, label_every=1):
    """Vertical bar chart as inline SVG. items = [(label, value)]."""
    if not items:
        return "<p class='muted'>No data</p>"
    left, right, top, bottom = 52, 12, 14, 30
    plot_w, plot_h = width - left - right, height - top - bottom
    vmax = max(v for _, v in items) or 1
    step = plot_w / len(items)
    bar_w = step * 0.7
    parts = []
    for k in range(5):
        frac = k / 4
        y = top + plot_h * (1 - frac)
        parts.append(f'<line x1="{left}" x2="{width - right}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{left - 6}" y="{y + 4:.1f}" text-anchor="end" class="axis">{short(vmax * frac)}</text>')
    for i, (label, value) in enumerate(items):
        h = value / vmax * plot_h
        x = left + i * step + (step - bar_w) / 2
        parts.append(
            f'<rect x="{x:.1f}" y="{top + plot_h - h:.1f}" width="{bar_w:.1f}" height="{h:.1f}" rx="2" fill="{color}">'
            f"<title>{esc(label)}: {short(value)}</title></rect>"
        )
        if i % label_every == 0:
            parts.append(
                f'<text x="{x + bar_w / 2:.1f}" y="{height - 10}" text-anchor="middle" class="axis">{esc(label)}</text>'
            )
    return f'<svg viewBox="0 0 {width} {height}" role="img">{"".join(parts)}</svg>'


def table(headers, rows, numeric=()):
    head = "".join(f'<th class="{"n" if i in numeric else ""}">{esc(h)}</th>' for i, h in enumerate(headers))
    body = "".join(
        "<tr>" + "".join(f'<td class="{"n" if i in numeric else ""}">{cell}</td>' for i, cell in enumerate(row)) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def card(title, inner, note=""):
    extra = f'<p class="muted">{note}</p>' if note else ""
    return f'<section class="card"><h2>{esc(title)}</h2>{inner}{extra}</section>'


def kpi(label, value):
    return f'<div class="kpi"><b>{esc(value)}</b><span>{esc(label)}</span></div>'


def load_quality_files(data_dir):
    """Read the Spark run summaries and quality-gate reports written by the pipeline."""
    runs = {}
    for path in sorted((data_dir / "processed" / "_runs").glob("clean_*.json")):
        month = path.stem.removeprefix("clean_")
        entry = {"run": json.loads(path.read_text()), "gate": None}
        gate = data_dir / "processed" / "_quality" / f"quality_{month}.json"
        if gate.exists():
            entry["gate"] = json.loads(gate.read_text())
        runs[month] = entry
    return runs


def render(data):
    """Turn the query results (a dict) into one self-contained HTML string."""
    trips, revenue, avg_fare, avg_miles, avg_minutes = data["kpi"]
    first, last = data["date_range"]
    tip_rate = data["card_tip_rate"]
    total = trips or 1

    kpis = "".join([
        kpi("Trips", fmt_int(trips)),
        kpi("Revenue (total_amount)", "$" + short(revenue)),
        kpi("Average fare", f"${float(avg_fare or 0):.2f}"),
        kpi("Average distance", f"{float(avg_miles or 0):.2f} mi"),
        kpi("Average duration", f"{float(avg_minutes or 0):.1f} min"),
        kpi("Card tip rate (tip / fare)", f"{float(tip_rate or 0) * 100:.1f}%"),
    ])

    hours = dict(data["hours"])
    hour_items = [(str(h), hours.get(h, 0)) for h in range(24)]
    weekday_items = [(name[:3], float(avg)) for _, name, avg in data["weekdays"]]

    pay_rows = [
        [esc(name), fmt_int(n), f"{n / total * 100:.1f}%", f"${float(t or 0):.2f}", f"${float(tip or 0):.2f}"]
        for name, n, t, tip in data["payments"]
    ]

    quality_html = "<p class='muted'>No Spark run summaries found yet.</p>"
    if data["quality"]:
        rows, latest = [], None
        for month, entry in data["quality"].items():
            run, gate = entry["run"], entry["gate"]
            status = gate["status"] if gate else "n/a"
            css = "ok" if status == "passed" else "bad"
            rows.append([
                esc(month), fmt_int(run["rows_in"]), fmt_int(run["rows_clean"]),
                fmt_int(run["rows_rejected_or_quarantined"]), f'<span class="{css}">{esc(status)}</span>',
            ])
            latest = (month, run)
        reasons = [(k.replace("_", " "), v) for k, v in latest[1]["counts_by_reason"].items() if k != "clean"]
        reasons.sort(key=lambda kv: -kv[1])
        quality_html = (
            table(["Month", "Raw rows", "Clean rows", "Rejected / quarantined", "Quality gate"], rows, numeric=(1, 2, 3))
            + f"<h2 style='margin-top:16px'>Why rows were set aside ({esc(latest[0])})</h2>"
            + hbar(reasons, color=ORANGE)
        )

    load_rows = [
        [str(i), esc(m), fmt_int(a or 0), fmt_int(b or 0), esc(s), esc(t)]
        for i, m, a, b, s, t in data["loads"]
    ]
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    parts = [
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        f"<title>NYC Taxi Dashboard</title><style>{CSS}</style></head><body><main>",
        "<h1>NYC Yellow Taxi Dashboard</h1>",
        f"<p class='muted'>Trips from {esc(first)} to {esc(last)} &middot; star schema in PostgreSQL &middot; generated {esc(generated)}</p>",
        f"<div class='kpis'>{kpis}</div>",
        "<div class='grid2'>",
        card("Trips by pickup borough", hbar(data["boroughs"], BLUE),
             "Unknown and N/A are placeholder zones in the TLC lookup table."),
        card("Top 10 pickup zones", hbar(data["top_zones"], GREEN)),
        card("Trips by hour of day", columns(hour_items, BLUE), "Hour of the pickup, local New York time."),
        card("Average trips per day, by weekday", columns(weekday_items, GREEN),
             "Total trips for a weekday divided by the number of such days in the data."),
        "</div>",
        card("Payment type", table(["Payment type", "Trips", "Share", "Avg total", "Avg tip"], pay_rows, numeric=(1, 2, 3, 4)),
             "The TLC data dictionary says cash tips are not recorded, so cash shows a tip near $0. Please verify this in the dictionary."),
        "<div style='height:14px'></div>",
        card("Data quality (from the Spark job and the quality gate)", quality_html,
             "A month appears here once Spark has processed it, even if it is not loaded into Postgres yet."),
        "<div style='height:14px'></div>",
        card("Recent loads into Postgres",
             table(["ID", "Month", "Rows in Parquet", "Rows loaded", "Status", "Finished (UTC)"], load_rows, numeric=(2, 3))),
        "<footer class='muted'>Built by dashboards/build_dashboard.py from the marts and audit schemas. ",
        "Data: NYC TLC Trip Record Data.</footer></main></body></html>",
    ]
    return "".join(parts)


def fetch(conn):
    data = {name: conn.execute(sql).fetchall() for name, sql in QUERIES.items()}
    data["kpi"] = data["kpi"][0]
    data["card_tip_rate"] = data["card_tip_rate"][0][0]
    data["date_range"] = data["date_range"][0]
    data["quality"] = load_quality_files(DATA)
    return data


def main():
    import psycopg  # imported here so the render functions can be tested without a database driver

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        with psycopg.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ.get("POSTGRES_PORT", "5432"),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ) as conn:
            data = fetch(conn)
        if not data["kpi"][0]:
            log.error("fact_trips is empty, nothing to show")
            return 1
        page = render(data)
        OUT.parent.mkdir(parents=True, exist_ok=True)
        tmp = OUT.with_name(OUT.name + ".tmp")
        tmp.write_text(page, encoding="utf-8")
        tmp.replace(OUT)
    except (psycopg.Error, OSError, ValueError, KeyError) as exc:
        log.error("Could not build the dashboard: %s", exc)
        return 1
    log.info("Wrote %s (%.0f KB)", OUT, OUT.stat().st_size / 1024)
    return 0


if __name__ == "__main__":
    sys.exit(main())
