import streamlit as st
import pandas as pd
import re
from ui.services.dq import latest_dq_for_job, dq_badge_html
from ui.links import go_to_link


def md_link(val) -> str:
    s = "" if val is None else str(val)
    if s.startswith("s3://") or s.startswith("http://") or s.startswith("https://"):
        return f"[{s}]({s})"
    return s


def show_table(df: pd.DataFrame, title: str | None = None, height: int | str | None = None):
    if title:
        st.markdown(f"### {title}")
    if df is None or df.empty:
        st.info("No rows yet.")
        return
    kwargs = dict(use_container_width=True, hide_index=True)
    if height is not None:
        kwargs["height"] = height
    st.dataframe(df, **kwargs)


def html_table(df: pd.DataFrame, title: str | None = None):
    """Render a small HTML table with clickable links without requiring pandas.tabulate."""
    if title:
        st.markdown(f"### {title}")
    if df is None or df.empty:
        st.info("No rows yet.")
        return

    def esc(s: str) -> str:
        return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace('"', "&quot;").replace("'", "&#39;"))

    def linkify(v):
        if v is None:
            return ""
        s = str(v)
        if s.strip().startswith("<span"):
            return s
        m = re.match(r"^\[(.*?)\]\((.*?)\)$", s.strip())
        if m:
            txt, url = m.group(1), m.group(2)
            return f"<a href=\"{esc(url)}\">{esc(txt)}</a>"
        if s.startswith("http://") or s.startswith("https://") or s.startswith("s3://") or s.startswith("?page="):
            return f"<a href=\"{esc(s)}\">{esc(s)}</a>"
        return esc(s)

    cols = list(df.columns)
    thead = "".join(
        [f"<th style='text-align:left;padding:6px;border-bottom:1px solid #555;'>{esc(c)}</th>" for c in cols])
    rows_html = ""
    for _, r in df.iterrows():
        tds = "".join([f"<td style='padding:6px;border-bottom:1px solid #333;'>{linkify(r[c])}</td>" for c in cols])
        rows_html += f"<tr>{tds}</tr>"
    html = f"<table style='width:100%;border-collapse:collapse;'><thead><tr>{thead}</tr></thead><tbody>{rows_html}</tbody></table>"
    st.markdown(html, unsafe_allow_html=True)


def search_box(label: str, key: str) -> str:
    return st.text_input(label, value="", key=key, placeholder="Type to filter...").strip().lower()


def jobs_table_with_open(df: pd.DataFrame, title: str, search_key: str):
    st.markdown(f"### {title}")
    q = search_box("Search jobs", key=search_key)
    view = df.copy()
    if q:
        cols = ["Job Id", "Job Type", "Orchestrator", "Status", "Created At", "Submitted By", "Source", "Destination",
                "Result Location"]
        mask = False
        for c in cols:
            if c in view.columns:
                mask = mask | view[c].astype(str).str.lower().str.contains(q)
        view = view[mask]
    if view.empty:
        st.info("No matching jobs.")
        return

    dq_badges = []
    for jid in view["Job Id"].astype(str).tolist():
        status, _, _, _ = latest_dq_for_job(jid)
        dq_badges.append(dq_badge_html(status))
    view = view.copy()
    view.insert(3, "DQ", dq_badges)

    out = pd.DataFrame({
        "Job Id": view["Job Id"],
        "Job Type": view.get("Job Type", ""),
        "Status": view.get("Status", ""),
        "DQ": view["DQ"],
        "Visibility": view.get("Visibility", ""),
        "Source": view.get("Source", "").apply(md_link),
        "Destination": view.get("Destination", "").apply(md_link),
        "Result Location": view.get("Result Location", "").apply(md_link),
        "Created At": view.get("Created At", ""),
        "Open": view.get("Open Job Link", "").apply(lambda x: f"[Open]({x})" if str(x).strip() else ""),
    })
    html_table(out, title=None)


def assets_table_with_open(df: pd.DataFrame, title: str, search_key: str):
    st.markdown(f"### {title}")
    q = search_box("Search assets", key=search_key)

    view = df.copy() if df is not None else pd.DataFrame()
    if q and not view.empty:
        cols = ["Asset Id", "Type", "Name", "Visibility", "Owner", "Published At", "Source Job Id"]
        mask = False
        for c in cols:
            if c in view.columns:
                mask = mask | view[c].astype(str).str.lower().str.contains(q)
        view = view[mask]

    if view is None or view.empty:
        st.info("No matching assets.")
        return

    h = st.columns([2.0, 2.5, 1.0, 1.4, 2.0, 1.0])
    h[0].markdown("**Asset Id**")
    h[1].markdown("**Name**")
    h[2].markdown("**Visibility**")
    h[3].markdown("**Published**")
    h[4].markdown("**Result Location**")
    h[5].markdown("**Open**")

    for _, r in view.sort_values("Published At", ascending=False).iterrows():
        link = str(r.get("Open Asset Link", ""))
        res = str(r.get("Result Location", ""))
        row = st.columns([2.0, 2.5, 1.0, 1.4, 2.0, 1.0])
        row[0].write(str(r.get("Asset Id", "")))
        row[1].write(str(r.get("Name", "")))
        row[2].write(str(r.get("Visibility", "")))
        row[3].write(str(r.get("Published At", "")))
        row[4].markdown(md_link(res), unsafe_allow_html=True)
        if link:
            if row[5].button("Open", key=f"open_asset_{r.get('Asset Id', '')}"):
                go_to_link(link)
        else:
            row[5].write("—")
