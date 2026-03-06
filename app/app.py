import streamlit as st
import pandas as pd

from services import (
    load_results,
    get_relevant_events,
    get_event_label,
    build_operator_message,
    get_status_text,
)

st.set_page_config(
    page_title="Alarm Intelligence Agent",
    layout="wide",
    initial_sidebar_state="collapsed",
)

with open("app/styles.css", "r", encoding="utf-8") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

df = load_results()

st.markdown('<div class="main-title">Industrial Alarm Intelligence</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="main-subtitle">Detection, classification and operational prioritization of alarm floods</div>',
    unsafe_allow_html=True
)

if df.empty:
    st.error("No results found in artifacts/flood_events_classified.csv")
    st.stop()

df_relevant = get_relevant_events(df)

if df_relevant.empty:
    st.markdown("""
    <div class="panel">
        <div class="panel-title">Current system status</div>
        <div class="panel-body">
            <span class="status-normal">NORMAL</span><br><br>
            No relevant flood events were found in the latest classified results.
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

event_labels = [get_event_label(row) for _, row in df_relevant.iterrows()]
selected_label = st.selectbox(
    "Select event to review",
    event_labels,
    index=0
)

selected_idx = event_labels.index(selected_label)
selected_event = df_relevant.iloc[selected_idx]

status_text = get_status_text(df_relevant)
event_type = str(selected_event.get("flood_type_v11", "N/A"))
severity = str(selected_event.get("severity_v11", "N/A"))
action = str(selected_event.get("recommended_action", "N/A"))

severity_badge = "badge-red" if severity == "severe" else "badge-amber"
status_class = "status-alert" if status_text == "FLOOD DETECTED" else "status-normal"

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Current status</div>
        <div class="kpi-value {status_class}">{status_text}</div>
        <div class="kpi-help">Current interpretation of classified events</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Event type</div>
        <div class="kpi-value">{event_type}</div>
        <div class="kpi-help">Operational classification of the selected event</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Severity</div>
        <div class="kpi-value">{severity.upper()}</div>
        <div class="kpi-help">Estimated from alarm rate versus baseline</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Recommended action</div>
        <div class="kpi-value">{action}</div>
        <div class="kpi-help">Action proposed by the rule-based agent</div>
    </div>
    """, unsafe_allow_html=True)

left, right = st.columns([1.15, 1])

with left:
    st.markdown(f"""
    <div class="panel">
        <div class="panel-title">Event summary</div>
        <div class="panel-body">
            <div><b>Start:</b> {selected_event.get("start", "N/A")}</div>
            <div><b>End:</b> {selected_event.get("end", "N/A")}</div>
            <div><b>Duration:</b> {round(float(selected_event.get("duration_min", 0)), 2)} min</div>
            <div><b>Total alarms:</b> {int(selected_event.get("n", 0))}</div>
            <div><b>Affected tags:</b> {int(selected_event.get("unique_tags", 0))}</div>
            <div><b>Peak alarm rate:</b> {round(float(selected_event.get("max_rate", 0)), 2)} alarms/min</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

with right:
    operator_message = build_operator_message(selected_event)
    st.markdown(f"""
    <div class="panel">
        <div class="panel-title">Operational interpretation</div>
        <div style="margin-bottom:10px;">
            <span class="badge {severity_badge}">{severity.upper()}</span>
            <span class="badge badge-blue">{event_type}</span>
        </div>
        <div class="panel-body">
            {operator_message}
        </div>
    </div>
    """, unsafe_allow_html=True)

with st.expander("Show classification signals"):
    s1, s2, s3 = st.columns(3)
    with s1:
        st.write("Dominant tag:", selected_event.get("dominant_tag", "N/A"))
        st.write("Dominant tag share:", selected_event.get("dominant_tag_share", "N/A"))
    with s2:
        st.write("Dominant message:", selected_event.get("dominant_msg", "N/A"))
        st.write("Dominant message share:", selected_event.get("dominant_msg_share", "N/A"))
    with s3:
        st.write("Priority 1 share:", selected_event.get("prio1_share", "N/A"))
        st.write("Flood candidate:", selected_event.get("flood_candidate_v11", "N/A"))

st.markdown("## Detected relevant events")

display_cols = [
    "start",
    "end",
    "duration_min",
    "n",
    "unique_tags",
    "severity_v11",
    "flood_type_v11",
    "recommended_action",
]

existing_cols = [c for c in display_cols if c in df_relevant.columns]

st.dataframe(
    df_relevant[existing_cols].copy(),
    use_container_width=True,
    hide_index=True
)