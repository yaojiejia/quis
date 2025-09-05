# dashboard.py
import streamlit as st
import duckdb
import re
import json
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path
import subprocess
from alerter import find_bad_urls, summarize_quality, company_nulls_summary, send_slack

PARQUET_DIR = "parquet"
URL_THRESHOLD_PCT = 5.0
COMPANY_NULLS_THRESHOLD_PCT = 2.0
COMPLETENESS_THRESHOLD_PCT = 5.0
COMPLETENESS_COLUMNS = ["name","position","city","country_code","current_company_name"]

st.set_page_config(page_title="Profiles Dashboard", layout="wide")
st.title("Profiles Dashboard")

@st.cache_resource
def get_conn():
    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE VIEW people AS
        SELECT * FROM read_parquet('{Path(PARQUET_DIR).as_posix()}/**/*.parquet')
    """)
    con.execute("""
        CREATE OR REPLACE VIEW people_aug AS
        SELECT
            *,
            lower(coalesce(name,'')) AS name_lc,
            lower(coalesce(city,'')) AS city_lc,
            regexp_replace(lower(coalesce(city,'')), '[^\\p{L}\\p{N}\\s]', '') AS city_folded
        FROM people
    """)
    return con

def extract_linkedin_id(u: str) -> str:
    if not isinstance(u, str):
        return ""
    m = re.search(r"/in/([^/?#]+)/?", u)
    return m.group(1) if m else ""

def run_dq_full(con):
    dq_df = con.execute("SELECT * FROM people_aug").df()
    bad = find_bad_urls(dq_df)
    should_alert, msg, _ = summarize_quality(len(dq_df), len(bad), URL_THRESHOLD_PCT)
    if should_alert:
        send_slack(f"WARNING {msg}")
    comp_sql_parts = []
    for c in COMPLETENESS_COLUMNS:
        comp_sql_parts.append(f"SUM(CASE WHEN {c} IS NULL OR length(trim({c}))=0 THEN 1 ELSE 0 END) AS {c}_nulls")
    comp_sql = f"SELECT COUNT(*) AS total, {', '.join(comp_sql_parts)} FROM people_aug"
    comp_stats = con.execute(comp_sql).df().iloc[0].to_dict()
    total = int(comp_stats["total"])
    if total > 0:
        for c in COMPLETENESS_COLUMNS:
            nulls = int(comp_stats[f"{c}_nulls"])
            pct = (nulls/total)*100.0
            if pct >= COMPLETENESS_THRESHOLD_PCT:
                send_slack(f"WARNING completeness: {c} null/blank {nulls}/{total} rows ({pct:.2f}%) ≥ {COMPLETENESS_THRESHOLD_PCT:.2f}%")

con = get_conn()

with st.sidebar:
    name_query = st.text_input("Name contains:")
    city_query = st.text_input("City query (partial ok):")
    limit = st.slider("Max rows", 100, 10000, 1000, step=100)
    st.divider()
    network_id = st.text_input("linkedin_id for network")
    see_network = st.button("See network")
    st.divider()
    start_pipeline = st.button("Start Pipeline")

if start_pipeline:
    try:
        result = subprocess.run(
            ["python", "pipeline.py"],
            capture_output=True,
            text=True,
            check=True
        )
        st.success("Pipeline executed successfully")
        st.text(result.stdout)
        run_dq_full(con)
    except subprocess.CalledProcessError as e:
        st.error("Pipeline failed")
        st.text(e.stderr)

where = []
params = []
if name_query:
    where.append("name_lc LIKE ?")
    params.append(f"%{name_query.lower()}%")
if city_query:
    tokens = [t for t in re.split(r"\\W+", city_query.lower()) if t]
    for t in tokens:
        where.append("city_folded LIKE ?")
        params.append(f"%{t}%")
where_clause = ("WHERE " + " AND ".join(where)) if where else ""

df = con.execute(f"""
SELECT *
FROM people_aug
{where_clause}
LIMIT {limit}
""", params).df()

st.subheader("Rows")
st.dataframe(df, use_container_width=True, hide_index=True)

agg_cities = con.execute(f"""
SELECT city, COUNT(*) AS cnt
FROM people_aug
{where_clause}
GROUP BY city
ORDER BY cnt DESC
LIMIT 20
""", params).df()
st.subheader("Top cities (filtered)")
if not agg_cities.empty:
    st.bar_chart(agg_cities.set_index("city"))
else:
    st.write("No data")

agg_companies = con.execute(f"""
SELECT current_company_name, COUNT(*) AS cnt
FROM people_aug
{where_clause}
GROUP BY current_company_name
ORDER BY cnt DESC
LIMIT 20
""", params).df()
st.subheader("Top companies (filtered)")
if not agg_companies.empty:
    st.bar_chart(agg_companies.set_index("current_company_name"))
else:
    st.write("No data")

edu_where = []
if where_clause:
    edu_where.append(where_clause[6:])
edu_where.append("educations_details IS NOT NULL")
edu_where.append("length(trim(educations_details)) > 0")
edu_where_clause = "WHERE " + " AND ".join(edu_where)

agg_edu = con.execute(f"""
SELECT
    CASE
        WHEN educations_details ILIKE '%new york university%' THEN 'New York University'
        ELSE educations_details
    END AS school,
    COUNT(*) AS cnt
FROM people_aug
{edu_where_clause}
GROUP BY school
ORDER BY cnt DESC
LIMIT 4
""", params).df()

st.subheader("Top schools (filtered)")
if not agg_edu.empty:
    fig, ax = plt.subplots()
    ax.pie(agg_edu["cnt"], labels=agg_edu["school"], autopct="%1.1f%%", startangle=90)
    ax.axis("equal")
    st.pyplot(fig)
else:
    st.write("No data")


    
st.divider()
st.header("Data Quality")

dq_url_df = con.execute(f"SELECT * FROM people_aug {where_clause}", params).df()
bad = find_bad_urls(dq_url_df)
should_alert, msg, _ = summarize_quality(len(dq_url_df), len(bad), URL_THRESHOLD_PCT)
if should_alert:
    st.error(msg)
else:
    st.success(msg)
if not bad.empty:
    st.dataframe(bad[["name","url"]].head(200), use_container_width=True, hide_index=True)

dq_company = con.execute(f"""
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN current_company_name IS NULL OR length(trim(current_company_name))=0 THEN 1 ELSE 0 END) AS nulls
FROM people_aug
{where_clause}
""", params).df()
if dq_company.iloc[0]["total"] > 0:
    total_cc = int(dq_company.iloc[0]["total"])
    nulls_cc = int(dq_company.iloc[0]["nulls"])
    should_alert_cc, msg_cc, rate_cc = company_nulls_summary(total_cc, nulls_cc, COMPANY_NULLS_THRESHOLD_PCT)
    if should_alert_cc:
        st.error(msg_cc)
    else:
        st.success(msg_cc)

comp_parts = []
for c in COMPLETENESS_COLUMNS:
    comp_parts.append(f"SUM(CASE WHEN {c} IS NULL OR length(trim({c}))=0 THEN 1 ELSE 0 END) AS {c}_nulls")
comp_sql = f"SELECT COUNT(*) AS total, {', '.join(comp_parts)} FROM people_aug {where_clause}"
comp_stats = con.execute(comp_sql, params).df().iloc[0].to_dict()
total = int(comp_stats["total"])
rows = []
if total > 0:
    for c in COMPLETENESS_COLUMNS:
        nulls = int(comp_stats[f"{c}_nulls"])
        pct = (nulls/total)*100.0
        rows.append({"column": c, "nulls": nulls, "total": total, "pct": round(pct,2), "status": "ALERT" if pct >= COMPLETENESS_THRESHOLD_PCT else "OK"})
comp_df = pd.DataFrame(rows)
st.subheader("Completeness (filtered)")
if not comp_df.empty:
    st.dataframe(comp_df, use_container_width=True, hide_index=True)
else:
    st.write("No data")

st.divider()
st.subheader("Similar Profiles Network")

if see_network and network_id:
    row = con.execute("SELECT name, position, url, similar_profiles FROM people WHERE linkedin_id = ?", [network_id]).df()
    if row.empty:
        st.error("No record found for that linkedin_id")
    else:
        center_name = row.iloc[0].get("name") or network_id
        center_title = row.iloc[0].get("position") or ""
        center_url = row.iloc[0].get("url") or ""
        sp_raw = row.iloc[0].get("similar_profiles")
        try:
            sp = json.loads(sp_raw) if isinstance(sp_raw, str) else (sp_raw if isinstance(sp_raw, list) else [])
        except:
            sp = []
        sim_df = pd.DataFrame(sp)
        if not sim_df.empty and "url" not in sim_df.columns and "url_text" in sim_df.columns:
            sim_df = sim_df.rename(columns={"url_text":"url"})
        if sim_df.empty:
            st.warning("No similar profiles available")
        else:
            G = nx.Graph()
            G.add_node(center_name, title=center_title, url=center_url, kind="center")
            for _, r in sim_df.iterrows():
                n = r.get("name") or ""
                t = r.get("title") or ""
                u = r.get("url") or ""
                if n:
                    G.add_node(n, title=t, url=u, kind="similar")
                    G.add_edge(center_name, n)
            fig, ax = plt.subplots(figsize=(5, 4))
            pos = nx.spring_layout(G, seed=42, k=0.7)
            center_nodes = [n for n, d in G.nodes(data=True) if d.get("kind") == "center"]
            similar_nodes = [n for n, d in G.nodes(data=True) if d.get("kind") == "similar"]
            nx.draw_networkx_nodes(G, pos, nodelist=center_nodes, node_size=500)
            nx.draw_networkx_nodes(G, pos, nodelist=similar_nodes, node_size=300)
            nx.draw_networkx_edges(G, pos, width=0.8, alpha=0.6)
            nx.draw_networkx_labels(G, pos, font_size=7)
            ax.axis("off")
            st.pyplot(fig)
            out = sim_df[["name","title","url"]].copy()
            out.loc[-1] = [center_name, center_title, center_url]
            out.index = out.index + 1
            out = out.sort_index()
            out["linkedin_id"] = out["url"].apply(extract_linkedin_id)
            st.dataframe(out[["name","title","url","linkedin_id"]], use_container_width=True, hide_index=True)
