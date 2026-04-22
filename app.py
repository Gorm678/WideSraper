"""
app.py — Cittros D365FO Discovery Tool
Run with: streamlit run app.py
"""

import os
import json
import glob
import csv as csv_mod
from datetime import datetime

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Cittros · ERP Discovery",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
h1, h2, h3 { font-family: 'DM Serif Display', serif; letter-spacing: -0.02em; }
section[data-testid="stSidebar"] { background: #0f1117; border-right: 1px solid #1e2130; }
section[data-testid="stSidebar"] * { color: #e8eaf0 !important; }
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stMultiSelect label,
section[data-testid="stSidebar"] .stTextInput label {
    color: #8892a4 !important; font-size: 0.75rem;
    text-transform: uppercase; letter-spacing: 0.08em;
}
.header-bar {
    background: linear-gradient(135deg, #0f1117 0%, #1a1f2e 100%);
    border-bottom: 2px solid #2d6ef7;
    padding: 1.5rem 2rem; margin: -1rem -1rem 2rem -1rem;
    display: flex; align-items: center; gap: 1rem;
}
.header-title { font-family: 'DM Serif Display', serif; font-size: 1.8rem; color: white; margin: 0; }
.header-sub { color: #6b7a99; font-size: 0.85rem; margin: 0; font-weight: 300; }
.header-dot { width: 10px; height: 10px; background: #2d6ef7; border-radius: 50%; box-shadow: 0 0 12px #2d6ef7; }
.metric-row { display: grid; grid-template-columns: repeat(5, 1fr); gap: 1rem; margin-bottom: 2rem; }
.metric-card { background: #f8f9fc; border: 1px solid #e8eaf0; border-radius: 10px; padding: 1.2rem 1.5rem; border-left: 3px solid #2d6ef7; }
.metric-card.green  { border-left-color: #10b981; }
.metric-card.orange { border-left-color: #f59e0b; }
.metric-card.purple { border-left-color: #8b5cf6; }
.metric-card.red    { border-left-color: #ef4444; }
.metric-number { font-size: 2rem; font-weight: 700; color: #0f1117; line-height: 1; }
.metric-label { font-size: 0.75rem; color: #6b7a99; text-transform: uppercase; letter-spacing: 0.06em; margin-top: 0.3rem; }
.section-header {
    font-family: 'DM Serif Display', serif; font-size: 1.3rem; color: #0f1117;
    margin: 2rem 0 1rem 0; padding-bottom: 0.5rem; border-bottom: 1px solid #e8eaf0;
}
.log-box {
    background: #0f1117; border-radius: 8px; padding: 1rem;
    font-family: monospace; font-size: 0.8rem; color: #8892a4;
    max-height: 300px; overflow-y: auto; line-height: 1.6;
}
.log-line-ok   { color: #10b981; }
.log-line-warn { color: #f59e0b; }
.log-line-info { color: #6b7a99; }
div.stButton > button[kind="primary"] {
    background: #2d6ef7; color: white; border: none; border-radius: 8px;
    padding: 0.6rem 2rem; font-weight: 600; font-size: 0.9rem; transition: background 0.2s;
}
div.stButton > button[kind="primary"]:hover { background: #1d5ce0; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="header-bar">
    <div class="header-dot"></div>
    <div>
        <p class="header-title">Cittros &middot; ERP Discovery</p>
        <p class="header-sub">Find Nordic companies using D365FO or Dynamics AX / Axapta</p>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### API Keys")
    fc_key = st.text_input("Firecrawl API Key", type="password",
                           value=os.environ.get("FIRECRAWL_API_KEY",""))
    ai_provider = st.selectbox("AI Provider", ["OpenAI (GPT-4o)", "Anthropic (Claude)"])
    if "OpenAI" in ai_provider:
        ai_key = st.text_input("OpenAI API Key", type="password",
                               value=os.environ.get("OPENAI_API_KEY",""))
    else:
        ai_key = st.text_input("Anthropic API Key", type="password",
                               value=os.environ.get("ANTHROPIC_API_KEY",""))
    st.markdown("---")
    st.markdown("### Countries")
    countries = st.multiselect("Run discovery for",
                               ["Denmark","Sweden","Norway"],
                               default=["Denmark","Sweden","Norway"])
    st.markdown("---")
    st.markdown("### Settings")
    use_ai       = st.toggle("Enable AI enrichment", value=True)
    result_limit = st.slider("Results per query", 5, 20, 10)

# ── Load engine ───────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    from engine import DiscoveryEngine
    return DiscoveryEngine(config_path="config.json", feedback_path="feedback.csv")

try:
    engine = get_engine()
except Exception as e:
    st.error(f"Failed to load engine: {e}")
    st.stop()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_leads, tab_run, tab_cases, tab_history, tab_config = st.tabs([
    "🎯 Leads",
    "🚀 Run Discovery",
    "🔍 Case Studies",
    "📂 Past Runs",
    "⚙️ Config",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — LEADS (rep view with action buttons)
# ══════════════════════════════════════════════════════════════════════════════
with tab_leads:
    from masterlist import MasterList
    master = MasterList()
    stats  = master.stats()

    st.markdown(f"""
    <div class="metric-row">
        <div class="metric-card">
            <div class="metric-number">{stats['total']}</div>
            <div class="metric-label">Total Companies</div>
        </div>
        <div class="metric-card green">
            <div class="metric-number">{stats['new']}</div>
            <div class="metric-label">New this run</div>
        </div>
        <div class="metric-card orange">
            <div class="metric-number">{stats['seen']}</div>
            <div class="metric-label">Seen before</div>
        </div>
        <div class="metric-card purple">
            <div class="metric-number">{stats['crm']}</div>
            <div class="metric-label">In to CRM</div>
        </div>
        <div class="metric-card red">
            <div class="metric-number">{stats['dismissed']}</div>
            <div class="metric-label">Dismissed</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Filters
    fc1, fc2, fc3, fc4 = st.columns(4)
    with fc1:
        f_status = st.multiselect("Status",
                                  ["new","seen","actioned_crm","actioned_dismissed"],
                                  default=["new","seen"])
    with fc2:
        all_countries = sorted({r["country"] for r in master.get_all()}) or ["Denmark","Sweden","Norway"]
        f_country = st.multiselect("Country", all_countries, default=all_countries)
    with fc3:
        all_erps = sorted({r["erp_detected"] for r in master.get_all()}) or ["D365FO"]
        f_erp = st.multiselect("ERP", all_erps, default=all_erps)
    with fc4:
        f_search = st.text_input("Search", placeholder="Company name...")

    # Export
    _, ex1, ex2 = st.columns([2,1,1])
    with ex1:
        st.download_button("Export new leads",
                           data=master.to_csv_bytes(status_filter=["new"]),
                           file_name=f"new_leads_{datetime.now().strftime('%Y%m%d')}.csv",
                           mime="text/csv", use_container_width=True)
    with ex2:
        st.download_button("Export all",
                           data=master.to_csv_bytes(),
                           file_name=f"all_companies_{datetime.now().strftime('%Y%m%d')}.csv",
                           mime="text/csv", use_container_width=True)

    st.markdown('<p class="section-header">Companies</p>', unsafe_allow_html=True)

    all_records = master.get_all()
    filtered = [
        r for r in all_records
        if r.get("status") in f_status
        and r.get("country","") in f_country
        and r.get("erp_detected","") in f_erp
        and (not f_search or f_search.lower() in r.get("company_name","").lower())
    ]

    if not filtered:
        if stats["total"] == 0:
            st.info("No companies yet. Run a discovery or scrape case studies first.")
        else:
            st.info("No companies match the current filters.")
    else:
        st.caption(f"Showing {len(filtered)} of {stats['total']} companies")

    for rec in filtered:
        status = rec.get("status","new")
        badge_styles = {
            "new":                "background:#d1fae5;color:#065f46",
            "seen":               "background:#fef3c7;color:#92400e",
            "actioned_crm":       "background:#dbeafe;color:#1d4ed8",
            "actioned_dismissed": "background:#f3f4f6;color:#6b7280",
        }
        badge_labels = {
            "new":"NEW", "seen":"SEEN BEFORE",
            "actioned_crm":"IN CRM", "actioned_dismissed":"DISMISSED",
        }
        bstyle = badge_styles.get(status, "background:#f3f4f6;color:#6b7280")
        blabel = badge_labels.get(status, status.upper())
        badge  = f"<span style='{bstyle};padding:2px 10px;border-radius:20px;font-size:0.72rem;font-weight:600'>{blabel}</span>"

        times      = rec.get("times_seen","1")
        first_seen = rec.get("first_seen","")
        last_seen  = rec.get("last_seen","")
        seen_txt   = f"First seen {first_seen}" if str(times)=="1" else f"Seen {times}x · last {last_seen}"

        with st.expander(
            f"{rec.get('company_name','—')}  |  {rec.get('country','')}  |  {rec.get('erp_detected','')}",
            expanded=(status=="new"),
        ):
            hcol1, hcol2 = st.columns([3,1])
            with hcol1:
                st.markdown(f"{badge} &nbsp; <small style='color:#8892a4'>{seen_txt}</small>",
                            unsafe_allow_html=True)
            with hcol2:
                url = rec.get("best_evidence_url","")
                if url:
                    st.markdown(f"[View source]({url})")

            ic1, ic2, ic3 = st.columns(3)
            with ic1:
                st.markdown(f"**ERP:** {rec.get('erp_detected','—')}")
                st.markdown(f"**Country:** {rec.get('country','—')}")
            with ic2:
                st.markdown(f"**Evidence:** {rec.get('best_evidence_type','—')}")
                st.markdown(f"**Found via:** {rec.get('source_owner','—')}")
            with ic3:
                title = rec.get("best_evidence_title","")
                if title:
                    st.markdown(f"**Page:** {title[:70]}")

            snippet = rec.get("best_snippet","")
            if snippet:
                st.markdown(f"> {snippet[:350]}")

            if rec.get("status_note"):
                st.caption(f"Note: {rec['status_note']}")

            st.markdown("---")
            key = rec.get("company_key","")

            if status not in ("actioned_crm","actioned_dismissed"):
                b1, b2, _ = st.columns([1,1,2])
                with b1:
                    if st.button("✅ Exist In CRM/Added to CRM", key=f"crm_{key}"):
                        master.action(key, "actioned_crm")
                        st.success("Added to CRM list")
                        st.rerun()
                with b2:
                    if st.button("❌ Not relevant", key=f"dis_{key}"):
                        st.session_state[f"show_note_{key}"] = True

                if st.session_state.get(f"show_note_{key}"):
                    note = st.selectbox("Reason", [
                        "Already a customer", "Too small", "Wrong industry",
                        "Not a real company", "Duplicate", "Other",
                    ], key=f"note_{key}")
                    if st.button("Confirm dismiss", key=f"confirm_{key}"):
                        master.action(key, "actioned_dismissed", note)
                        st.session_state[f"show_note_{key}"] = False
                        st.rerun()
            else:
                if st.button("↩ Undo", key=f"undo_{key}"):
                    master.reset_to_new(key)
                    st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — RUN DISCOVERY
# ══════════════════════════════════════════════════════════════════════════════
with tab_run:
    col1, col2 = st.columns([2,1])
    with col1:
        st.markdown('<p class="section-header">Start a Discovery Run</p>', unsafe_allow_html=True)
        total_q = sum(len(engine.config["queries"].get(c,[])) for c in countries)
        st.markdown(f"""
        Searches **{len(countries)} countries** · **{total_q} queries** · **{result_limit} results each**.
        AI enrichment **{"on" if use_ai else "off"}**.
        New companies appear in the **Leads** tab automatically.
        """)
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        run_clicked = st.button("Run Discovery", type="primary", use_container_width=True)

    if run_clicked:
        if not fc_key:
            st.error("Firecrawl API key required.")
            st.stop()
        os.environ["FIRECRAWL_API_KEY"] = fc_key
        if use_ai and ai_key:
            if "OpenAI" in ai_provider:
                os.environ["OPENAI_API_KEY"] = ai_key
                os.environ["AI_PROVIDER"]    = "openai"
            else:
                os.environ["ANTHROPIC_API_KEY"] = ai_key
                os.environ["AI_PROVIDER"]       = "anthropic"

        engine.config["result_limit"]      = result_limit
        engine.config["use_ai_enrichment"] = use_ai
        get_engine.clear()
        engine = get_engine()

        log_lines    = []
        log_ph       = st.empty()
        progress_bar = st.progress(0)
        status_text  = st.empty()

        def progress_cb(msg):
            log_lines.append(msg)
            css = "log-line-ok" if "✓" in msg else \
                  "log-line-warn" if ("⚠" in msg or "✗" in msg) else "log-line-info"
            lines_html = "".join(
                '<div class="' + (css if i==len(log_lines)-1 else "log-line-info") + '">' + l + '</div>'
                for i, l in enumerate(log_lines[-20:])
            )
            log_ph.markdown(f'<div class="log-box">{lines_html}</div>', unsafe_allow_html=True)

        with st.spinner("Running..."):
            try:
                rows = engine.run(countries=countries, progress_cb=progress_cb)
                raw_path, target_path, targets, counts = engine.save_results(rows)
                progress_bar.progress(100)
                status_text.success(
                    f"Done — {counts['new']} new companies · "
                    f"{counts['updated']} already known · "
                    f"{len(targets)} total this run"
                )
                st.balloons()
                st.info("Switch to the Leads tab to review results.")
            except Exception as e:
                st.error(f"Run failed: {e}")
                import traceback; st.code(traceback.format_exc())

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — CASE STUDIES (saves to master list)
# ══════════════════════════════════════════════════════════════════════════════
with tab_cases:
    st.markdown('<p class="section-header">Scrape Partner Case Studies</p>', unsafe_allow_html=True)
    partner_sites = engine.config.get("partner_sites", [])

    st.markdown(f"""
    Scrapes **{len(partner_sites)} Nordic partner sites** for named customer references.
    New companies go straight into the **Leads** tab.
    """)

    with st.expander(f"{len(partner_sites)} partner sites configured"):
        for p in partner_sites:
            st.markdown(f"- **{p['name']}** ({p['country']}) — {p['index_url']}")

    csc1, csc2 = st.columns([2,1])
    with csc1:
        cs_countries = st.multiselect("Countries", ["Denmark","Sweden","Norway"],
                                      default=["Denmark","Sweden","Norway"], key="cs_c")
    with csc2:
        st.markdown("<br>", unsafe_allow_html=True)
        scrape_clicked = st.button("Scrape Case Studies", type="primary", use_container_width=True)

    fp  = [p for p in partner_sites if p["country"] in cs_countries]
    est = sum(p.get("max_cases",30) for p in fp)
    st.caption(f"~{est} pages · {len(fp)} partners · ~${round(est*0.015,2)} cost")

    if scrape_clicked:
        if not fc_key:
            st.error("Firecrawl API key required.")
            st.stop()
        os.environ["FIRECRAWL_API_KEY"] = fc_key

        from firecrawl import FirecrawlApp as FC
        from scraper import CaseStudyScraper
        from masterlist import MasterList

        fc_app   = FC(api_key=fc_key)
        cs_lines = []
        cs_log   = st.empty()
        cs_prog  = st.progress(0)

        def cs_cb(msg):
            cs_lines.append(msg)
            css = "log-line-ok" if "✓" in msg else \
                  "log-line-warn" if ("✗" in msg or "–" in msg) else "log-line-info"
            html = "".join(
                '<div class="' + (css if i==len(cs_lines)-1 else "log-line-info") + '">' + l + '</div>'
                for i, l in enumerate(cs_lines[-25:])
            )
            cs_log.markdown(f'<div class="log-box">{html}</div>', unsafe_allow_html=True)

        with st.spinner("Scraping..."):
            try:
                scraper  = CaseStudyScraper(fc_app, fp, cs_cb)
                cs_rows  = scraper.scrape_all()

                # Save to master list
                ml     = MasterList()
                counts = ml.merge_run(cs_rows)

                # Save timestamped CSV
                os.makedirs("outputs", exist_ok=True)
                ts   = datetime.now().strftime("%Y%m%d_%H%M")
                path = f"outputs/casestudy_{ts}.csv"
                fields = ["target_company","target_domain","country","erp_detected",
                          "classification","evidence_type","evidence_url","evidence_title",
                          "evidence_snippet","source_owner","ai_reason","ai_confidence"]
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = csv_mod.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                    w.writeheader(); w.writerows(cs_rows)

                cs_prog.progress(100)
                st.success(
                    f"**{counts['new']} new companies** added to master list · "
                    f"{counts['updated']} already known"
                )
                st.info("Switch to the **Leads** tab to review and action results.")

                if cs_rows:
                    df_cs = pd.DataFrame(cs_rows)
                    avail = [c for c in ["target_company","country","erp_detected",
                                         "source_owner","evidence_url"] if c in df_cs.columns]
                    st.dataframe(df_cs[avail], use_container_width=True, height=350)

            except Exception as e:
                st.error(f"Scraping failed: {e}")
                import traceback; st.code(traceback.format_exc())

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — PAST RUNS
# ══════════════════════════════════════════════════════════════════════════════
with tab_history:
    st.markdown('<p class="section-header">Past Runs</p>', unsafe_allow_html=True)
    all_files = sorted(
        glob.glob("outputs/targets_*.csv") + glob.glob("outputs/casestudy_*.csv"),
        reverse=True,
    )
    if not all_files:
        st.info("No past runs yet.")
    for tf in all_files:
        label = tf.replace("outputs/","").replace(".csv","")
        df_h  = pd.read_csv(tf)
        with st.expander(f"{label} — {len(df_h)} rows"):
            if "country" in df_h.columns:
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Rows", len(df_h))
                c2.metric("DK", len(df_h[df_h["country"]=="Denmark"]))
                c3.metric("SE", len(df_h[df_h["country"]=="Sweden"]))
                c4.metric("NO", len(df_h[df_h["country"]=="Norway"]))
            st.download_button(f"Download {label}.csv",
                               df_h.to_csv(index=False).encode(),
                               file_name=f"{label}.csv", key=f"dl_{label}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — CONFIG
# ══════════════════════════════════════════════════════════════════════════════
with tab_config:
    st.markdown('<p class="section-header">Configuration</p>', unsafe_allow_html=True)
    cfg = engine.config

    with st.expander("Partner domains blocklist"):
        pt = st.text_area("One domain per line",
                          "\n".join(sorted(cfg.get("partner_domains",[]))), height=200)
        if st.button("Save partner domains"):
            cfg["partner_domains"] = [d.strip() for d in pt.splitlines() if d.strip()]
            with open("config.json","w",encoding="utf-8") as f: json.dump(cfg,f,indent=2,ensure_ascii=False)
            get_engine.clear(); st.success("Saved.")

    with st.expander("Bad target names"):
        bt = st.text_area("One name per line",
                          "\n".join(sorted(cfg.get("bad_target_names",[]))), height=200)
        if st.button("Save bad names"):
            cfg["bad_target_names"] = [n.strip() for n in bt.splitlines() if n.strip()]
            with open("config.json","w",encoding="utf-8") as f: json.dump(cfg,f,indent=2,ensure_ascii=False)
            get_engine.clear(); st.success("Saved.")

    with st.expander("Query library"):
        for country in ["Denmark","Sweden","Norway"]:
            qs = cfg.get("queries",{}).get(country,[])
            qt = st.text_area(f"{country}", "\n".join(qs), height=150, key=f"q_{country}")
            if st.button(f"Save {country}", key=f"sq_{country}"):
                cfg["queries"][country] = [q.strip() for q in qt.splitlines() if q.strip()]
                with open("config.json","w",encoding="utf-8") as f: json.dump(cfg,f,indent=2,ensure_ascii=False)
                get_engine.clear(); st.success("Saved.")

    with st.expander("Partner sites (case study scraping)"):
        ps = st.text_area("JSON", json.dumps(cfg.get("partner_sites",[]),indent=2,ensure_ascii=False), height=300)
        if st.button("Save partner sites"):
            try:
                cfg["partner_sites"] = json.loads(ps)
                with open("config.json","w",encoding="utf-8") as f: json.dump(cfg,f,indent=2,ensure_ascii=False)
                get_engine.clear(); st.success(f"Saved {len(cfg['partner_sites'])} sites.")
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON: {e}")

    with st.expander("Master list management"):
        from masterlist import MasterList
        ml = MasterList()
        st.markdown(f"**{len(ml.records)} companies** tracked across all runs.")
        st.download_button("Download master_list.csv", ml.to_csv_bytes(), file_name="master_list.csv")
        st.markdown("---")
        if st.button("Reset master list (delete all history)"):
            if os.path.exists("master_list.csv"): os.remove("master_list.csv")
            st.warning("Master list cleared.")

    with st.expander("Raw config.json"):
        st.json(cfg)
