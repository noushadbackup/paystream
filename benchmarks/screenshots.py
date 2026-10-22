"""Screenshots of the running stack for the docs: Grafana, the Flink job graph and checkpoints, a Trino
query on silver.fraud_alerts, and the Fraud Ops Console. Needs `pip install playwright` and a
Chromium (`playwright install chromium`, or pass --chrome to an existing chrome binary).

  python benchmarks/screenshots.py --out docs/img
"""

from __future__ import annotations

import argparse
import html
import json
import time
import urllib.request
from pathlib import Path

ALERTS_SQL = """SELECT alert_id, rule, account_id, trigger_txn_id, score, action, detected_at
FROM lake.silver.fraud_alerts ORDER BY detected_at DESC LIMIT 15"""


def trino_table(host: str) -> str:
    import trino

    cur = trino.dbapi.connect(host=host, port=8080, user="screenshots", catalog="lake").cursor()
    cur.execute(ALERTS_SQL)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    cur.execute("SELECT count(*) FROM lake.silver.fraud_alerts")
    total = cur.fetchall()[0][0]
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in r) + "</tr>" for r in rows)
    return f"""<!doctype html><meta charset="utf-8"><style>
body{{font:13px ui-monospace,Consolas,monospace;background:#0f1115;color:#e6e6e6;margin:24px}}
pre{{color:#9ecbff}} table{{border-collapse:collapse}} th,td{{border:1px solid #333;padding:4px 8px;text-align:left}}
th{{background:#1b1f27}} .meta{{color:#999}}</style>
<div class="meta">trino&gt; (Trino 463 on the live lakehouse, {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())})</div>
<pre>{html.escape(ALERTS_SQL)};</pre><table><tr>{head}</tr>{body}</table>
<p class="meta">{len(rows)} of {total:,} rows in lake.silver.fraud_alerts</p>"""


def payments_job_id(flink: str) -> str:
    with urllib.request.urlopen(f"{flink}/jobs/overview", timeout=10) as r:
        return next(j["jid"] for j in json.load(r)["jobs"] if j["name"] == "paystream-payments" and j["state"] == "RUNNING")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="docs/img")
    p.add_argument("--chrome", help="path to a chrome/chromium binary if Playwright's own is not installed")
    p.add_argument("--flink", default="http://localhost:8082")
    p.add_argument("--grafana", default="http://localhost:3000")
    p.add_argument("--console", default="http://localhost:8501")
    p.add_argument("--trino", default="localhost")
    a = p.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    from playwright.sync_api import sync_playwright

    jid = payments_job_id(a.flink)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(executable_path=a.chrome) if a.chrome else pw.chromium.launch()
        page = browser.new_page(viewport={"width": 1600, "height": 1000}, color_scheme="dark")

        page.goto(f"{a.grafana}/d/paystream?orgId=1&from=now-30m&to=now&refresh=10s&kiosk")
        page.wait_for_timeout(8000)
        page.screenshot(path=out / "grafana-live.png", full_page=True)

        page.goto(f"{a.flink}/#/job/running/{jid}/overview")
        page.wait_for_timeout(6000)
        page.screenshot(path=out / "flink-job-graph.png")
        page.goto(f"{a.flink}/#/job/running/{jid}/checkpoints")
        page.wait_for_timeout(5000)
        page.screenshot(path=out / "flink-checkpoints.png", full_page=True)

        page.set_content(trino_table(a.trino))
        page.screenshot(path=out / "trino-fraud-alerts.png", full_page=True)

        page.goto(a.console)
        page.wait_for_timeout(15000)  # Streamlit renders after its queries return
        page.screenshot(path=out / "ops-console-live.png", full_page=True)
        browser.close()
    print("wrote", ", ".join(sorted(f.name for f in out.glob("*-live.png")) + ["flink-*.png", "trino-fraud-alerts.png"]))


if __name__ == "__main__":
    main()
