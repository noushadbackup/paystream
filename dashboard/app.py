"""PayStream Fraud Ops Console: what a fraud analyst on shift would keep open.

  streamlit run dashboard/app.py                                    # live stack (Trino on localhost:8080)
  PAYSTREAM_DUCKDB=dbt/ci/ci.duckdb streamlit run dashboard/app.py  # demo, no stack needed
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

import cases as cs
import queries as q

BUILT_IN = ["VELOCITY", "MULE_FAN_IN", "NEW_DEVICE_CASH_OUT", "AGENT_FLOAT_SPIKE"]
PALETTE = ["#4C78A8", "#E45756", "#F58518", "#72B7B2", "#54A24B", "#B279A2", "#9D755D", "#EECA3B", "#FF9DA6"]

def taka(bdt: float) -> str:
    """Bangladeshi convention: lakh (1e5) and crore (1e7)."""
    if bdt >= 1e7:
        return f"৳{bdt / 1e7:,.2f} crore"
    if bdt >= 1e5:
        return f"৳{bdt / 1e5:,.2f} lakh"
    return f"৳{bdt:,.0f}"


st.set_page_config(page_title="PayStream Fraud Ops", page_icon="🛡️", layout="wide")


@st.cache_resource
def backend() -> q.Backend:
    return q.backend_from_env()


b = backend()


@st.cache_resource
def case_backend():
    return cs.cases_from_env(b, q.anchor(b))


RULES = BUILT_IN + [r for r in q.rule_names(b) if r not in BUILT_IN]
RULE_COLORS = alt.Scale(domain=RULES, range=PALETTE[: len(RULES)])

with st.sidebar:
    st.header("PayStream")
    st.caption(f"Source: {b.name}")
    if isinstance(b, q.DuckDBBackend):
        st.warning("Demo data: payments come from the generator, and alerts are placed where the Flink "
                   "rules would fire. Latencies here are illustrative, not measured.")
    window = st.select_slider("Window", options=[5, 15, 30, 60, 180], value=30, format_func=lambda m: f"{m} min")
    rules = st.multiselect("Rules", RULES, default=RULES)
    refresh = st.toggle("Auto-refresh every 5 s", value=not isinstance(b, q.DuckDBBackend))
    st.divider()
    lookup = st.text_input("Look up an account", placeholder="U0001234, A00042, M00007")


@st.fragment(run_every="5s" if refresh else None)
def live() -> None:
    now = q.anchor(b)
    h = q.headline(b, now, window)
    st.caption(f"As of {now:%Y-%m-%d %H:%M:%S} UTC · last {window} minutes")

    c = st.columns(5)
    c[0].metric("Payments", f"{h['payments']:,}")
    c[1].metric("Money moved", taka(h["success_bdt"]))
    c[2].metric("Failure rate", f"{h['failure_rate']:.2%}")
    c[3].metric("Fraud alerts", f"{h['alerts']:,}")
    c[4].metric("Alert latency p95", f"{h['alert_p95_ms']:,.0f} ms" if h["alert_p95_ms"] is not None else "–")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Alerts per minute")
        apm = q.alerts_per_minute(b, now, window)
        apm = apm[apm["rule"].isin(rules)] if rules else apm
        if apm.empty:
            st.info("No alerts in this window.")
        else:
            st.altair_chart(
                alt.Chart(apm).mark_bar().encode(
                    x=alt.X("minute:T", title=None, scale=alt.Scale(type="utc"), axis=alt.Axis(format="%H:%M")),
                    y=alt.Y("alerts:Q", title="alerts"),
                    color=alt.Color("rule:N", scale=RULE_COLORS, legend=alt.Legend(orient="bottom", title=None)),
                    tooltip=["minute:T", "rule:N", "alerts:Q"],
                ).properties(height=260),
                width="stretch",
            )
        st.subheader("Payments per minute")
        ppm = q.payments_per_minute(b, now, window)
        st.altair_chart(
            alt.Chart(ppm).mark_line().encode(
                x=alt.X("minute:T", title=None, scale=alt.Scale(type="utc"), axis=alt.Axis(format="%H:%M")),
                y=alt.Y("payments:Q", title="payments"),
                color=alt.Color("txn_type:N", legend=alt.Legend(orient="bottom", title=None)),
                tooltip=["minute:T", "txn_type:N", "payments:Q"],
            ).properties(height=220),
            width="stretch",
        )
    with right:
        st.subheader("Live alert feed")
        feed = q.alert_feed(b, now, rules)
        st.dataframe(
            feed,
            hide_index=True,
            height=540,
            column_config={
                "detected_at": st.column_config.DatetimeColumn("Detected", format="HH:mm:ss"),
                "rule": "Rule",
                "account_id": "Account",
                "score": st.column_config.ProgressColumn("Score", min_value=1.0, max_value=3.0, format="%.2f"),
                "action": "Action",
                "details": "Why",
                "trigger_txn_id": "Trigger txn",
            },
        )


live_tab, cases_tab, rules_tab = st.tabs(["Live", "Cases", "Rules"])
with live_tab:
    live()
    st.divider()
    recon_col, agent_col = st.columns(2)
    with recon_col:
        st.subheader("Stream vs ledger reconciliation")
        st.caption("Every successful gateway payment must be posted in the ledger once, with the same amount.")
        recon = q.reconciliation(b)
        st.dataframe(
            recon.style.map(lambda s: "color: #2e7d32; font-weight: 600" if s == "MATCHED"
                            else "color: #c62828; font-weight: 600", subset=["status"]),
            hide_index=True,
            column_config={"stream_bdt": st.column_config.NumberColumn("Stream ৳", format="%,.2f"),
                           "ledger_bdt": st.column_config.NumberColumn("Ledger ৳", format="%,.2f")},
        )
    with agent_col:
        st.subheader("Agents running low on float")
        st.caption("Net e-money change today: cash-in spends float, cash-out earns it back.")
        st.dataframe(q.agent_float(b), hide_index=True)

with cases_tab:
    cases = case_backend()
    if cases is None:
        st.info("Case management needs the case service (`make up`) or, for the demo, `pip install -e case-service`.")
    else:
        now_c = q.anchor(b) if isinstance(cases, cs.DemoCases) else pd.Timestamp.now(tz="UTC").to_pydatetime()
        s = cases.stats()
        st.caption(f"Source: {cases.name}. Alerts on the same account or device join one case; priority sets the "
                   "SLA (P1 15 min, P2 2 h, P3 24 h). A verdict labels every alert on the case and acts on the "
                   "account within the same request.")
        m = st.columns(6)
        m[0].metric("Open cases", f"{s['open']:,}")
        m[1].metric("P1 · 15 min", s["open_by_priority"].get(1, s["open_by_priority"].get("1", 0)))
        m[2].metric("P2 · 2 h", s["open_by_priority"].get(2, s["open_by_priority"].get("2", 0)))
        m[3].metric("SLA breached", s["sla_breached"])
        m[4].metric("Suppressed", s["alerts_suppressed"], help="Alerts not re-queued because an analyst cleared "
                    "the same rule on the same subject in the last 72 h")
        m[5].metric("Effects pending", s["effects_pending"])

        work, queue_col = st.columns([2, 3])
        with work:
            st.subheader("Work a case")
            analyst = st.text_input("Analyst", value="ayesha")
            if st.button("Claim next case", type="primary"):
                claimed = cases.claim(analyst)
                st.session_state["case_id"] = claimed["case_id"] if claimed else None
                if not claimed:
                    st.success("The queue is empty.")
            if case_id := st.session_state.get("case_id"):
                c = cases.case(case_id)
                st.markdown(f"**{c['case_id']}** · P{c['priority']} · subject `{c['subject_id']}` · {c['status']}")
                st.dataframe(pd.DataFrame(c["alerts"])[["rule", "trigger_txn_id", "score", "action", "details"]],
                             hide_index=True)
                if c["status"] != "CLOSED":
                    with st.form("close"):
                        verdict = st.radio("Verdict", cs.VERDICTS, horizontal=True)
                        note = st.text_input("Note")
                        if st.form_submit_button("Close case"):
                            r = cases.close(analyst, case_id, verdict, note)
                            st.success(f"{r['verdict']}: {r['labels']} label(s) sent"
                                       + (", account frozen" if r["frozen"] else "")
                                       + (", flags lifted and rule allowed for 72 h" if r["cleared"] else ""))
                with st.expander("History"):
                    st.dataframe(pd.DataFrame(c["history"])[["at", "actor", "kind", "detail"]], hide_index=True)
        with queue_col:
            st.subheader("Queue")
            st.dataframe(cs.queue_frame(cases.queue(100), now_c), hide_index=True, height=480,
                         column_config={"max_score": st.column_config.NumberColumn("Score", format="%.2f"),
                                        "sla_left_min": st.column_config.NumberColumn("SLA left (min)"),
                                        "subject_id": "Subject"})

with rules_tab:
    st.subheader("Rule performance")
    st.caption("Every rule over all data in the lake. Precision is the share of a rule's alerts that land on accounts "
               "with a fraud label. Shadow rules run on live traffic without acting, so their numbers are known before "
               "anyone lets them decline a payment.")
    perf = q.rule_performance(b)
    st.dataframe(
        perf,
        hide_index=True,
        column_config={
            "shadow": st.column_config.CheckboxColumn("Shadow"),
            "precision_vs_labels": st.column_config.ProgressColumn("Precision", min_value=0.0, max_value=1.0,
                                                                   format="%.2f"),
            "worst_day_p95_ms": st.column_config.NumberColumn("Worst day p95 (ms)", format="%.0f"),
        },
    )
    st.subheader("Shadow alerts")
    shadow = q.shadow_alerts(b, q.anchor(b))
    if shadow.empty:
        st.info("No shadow rule has fired in this data. A rule in shadow alerts here instead of in the feed.")
    else:
        st.dataframe(shadow, hide_index=True)
    st.caption("Rules are written, backtested and approved in the rules service (maker-checker, audit log): "
               "`POST /rules`, `/backtest`, `/submit`, `/approve`. Approved versions reach Flink in seconds.")

if lookup:
    st.divider()
    st.subheader(f"Account {lookup}")
    payments, alerts = q.account(b, lookup.strip(), q.anchor(b))
    if not alerts.empty:
        st.error(f"{len(alerts)} fraud alert(s) on this account")
        st.dataframe(alerts, hide_index=True)
    st.dataframe(pd.DataFrame(payments), hide_index=True)
