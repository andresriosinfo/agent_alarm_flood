import streamlit as st
import pandas as pd

from app.services import load_results, get_latest_event, get_summary_metrics
from app.components import load_css, section_title, summary_card, info_card

st.set_page_config(
    page_title="Alarm Intelligence Agent",
    page_icon="🚨",
    layout="wide"
)

load_css("app/styles.css")

df = load_results()
summary = get_summary_metrics(df)
latest = get_latest_event(df)

section_title(
    "Industrial Alarm Intelligence Agent",
    "Flood detection, event classification and operational prioritization"
)

c1, c2, c3, c4 = st.columns(4)

with c1:
    summary_card("Flood events detected", str(summary["total_events"]), "Classified event blocks")

with c2:
    summary_card("Latest severity", str(summary["latest_severity"]), "Based on baseline alarm rate")

with c3:
    summary_card("Latest event type", str(summary["latest_type"]), "Rule-based interpretation")

with c4:
    summary_card("Recommended action", str(summary["latest_action"]), "Operational response")

st.markdown("###")

if latest is None:
    info_card(
        "No results available",
        "Run the pipeline first so the app can load artifacts/flood_events_classified.csv"
    )
    st.stop()

left, right = st.columns([1.2, 1])

with left:
    info_card(
        "Latest detected event",
        f"""
        <b>Start:</b> {latest.get('start', 'N/A')}<br>
        <b>End:</b> {latest.get('end', 'N/A')}<br>
        <b>Duration:</b> {latest.get('duration_min', 'N/A')} min<br>
        <b>Total alarms:</b> {latest.get('n', 'N/A')}<br>
        <b>Unique tags:</b> {latest.get('unique_tags', 'N/A')}<br>
        <b>Max alarms/min:</b> {latest.get('max_rate', 'N/A')}
        """
    )

with right:
    info_card(
        "Agent interpretation",
        f"""
        <b>Flood candidate:</b> {latest.get('flood_candidate_v11', 'N/A')}<br>
        <b>Event type:</b> {latest.get('flood_type_v11', 'N/A')}<br>
        <b>Severity:</b> {latest.get('severity_v11', 'N/A')}<br>
        <b>Recommended action:</b> {latest.get('recommended_action', 'N/A')}
        """
    )

st.markdown("###")
section_title("Event history", "Detected and classified event blocks")

show_cols = [
    "block_id", "start", "end", "duration_min", "n", "max_rate",
    "unique_tags", "dominant_tag", "dominant_msg",
    "severity_v11", "flood_type_v11", "recommended_action"
]

available_cols = [c for c in show_cols if c in df.columns]
st.dataframe(df[available_cols], use_container_width=True, hide_index=True)