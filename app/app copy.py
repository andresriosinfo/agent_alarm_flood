import sys
from pathlib import Path
from datetime import datetime, time

import altair as alt
import pandas as pd
import streamlit as st

# Asegura que se pueda importar src/
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import DBConfig, FloodConfig
from src.db import get_connection
from src.baseline_cache import get_or_create_baseline
from src.operational_agent import assess_current_state


st.set_page_config(
    page_title="Alarm Intelligence",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
.stApp {
    background-color: #0B1120;
    color: #E5E7EB;
}
header[data-testid="stHeader"] {
    background: transparent;
}
section[data-testid="stSidebar"] {
    display: none;
}
.block-container {
    max-width: 1400px;
    padding-top: 1rem;
    padding-bottom: 1.2rem;
}
.main-title {
    font-size: 2rem;
    font-weight: 700;
    color: #F3F4F6;
    margin-bottom: 0.2rem;
}
.main-subtitle {
    color: #9CA3AF;
    font-size: 0.98rem;
    margin-bottom: 1.2rem;
}
.card {
    background: #111827;
    border: 1px solid #243041;
    border-radius: 16px;
    padding: 16px 18px;
    min-height: 110px;
}
.card-label {
    color: #9CA3AF;
    font-size: 0.82rem;
    text-transform: uppercase;
    letter-spacing: .05em;
    margin-bottom: 8px;
}
.card-value {
    color: #F3F4F6;
    font-size: 1.35rem;
    font-weight: 700;
    line-height: 1.15;
}
.card-help {
    color: #94A3B8;
    font-size: 0.84rem;
    margin-top: 6px;
}
.panel {
    background: #111827;
    border: 1px solid #243041;
    border-radius: 16px;
    padding: 18px;
    margin-bottom: 14px;
}
.panel-title {
    color: #9CA3AF;
    font-size: 0.84rem;
    text-transform: uppercase;
    letter-spacing: .05em;
    margin-bottom: 12px;
}
.panel-body {
    color: #E5E7EB;
    font-size: 1rem;
    line-height: 1.6;
}
.badge {
    display: inline-block;
    padding: 0.28rem 0.68rem;
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 600;
    border: 1px solid #243041;
    margin-right: 6px;
    margin-bottom: 8px;
}
.badge-blue {
    background: rgba(37, 99, 235, 0.15);
    color: #93C5FD;
}
.badge-amber {
    background: rgba(245, 158, 11, 0.14);
    color: #FCD34D;
}
.badge-red {
    background: rgba(239, 68, 68, 0.14);
    color: #FCA5A5;
}
.badge-green {
    background: rgba(16, 185, 129, 0.14);
    color: #86EFAC;
}
.status-normal { color: #34D399; font-weight: 700; }
.status-elevated { color: #FCD34D; font-weight: 700; }
.status-high { color: #FB923C; font-weight: 700; }
.status-flood { color: #F87171; font-weight: 700; }

div[data-baseweb="select"] > div,
div[data-testid="stDateInput"] input,
div[data-testid="stTimeInput"] input {
    background-color: #111827 !important;
    color: #E5E7EB !important;
    border-color: #243041 !important;
}
.stButton > button {
    background: #2563EB;
    color: white;
    border: none;
    border-radius: 12px;
    padding: 0.6rem 1rem;
    font-weight: 600;
}
.stButton > button:hover {
    background: #1D4ED8;
}
div[data-testid="stDataFrame"] {
    border: 1px solid #243041;
    border-radius: 16px;
    overflow: hidden;
}
div[data-testid="stVegaLiteChart"] {
    background: #111827;
    border: 1px solid #243041;
    border-radius: 16px;
    padding: 10px;
}
</style>
""", unsafe_allow_html=True)


@st.cache_resource(show_spinner=False)
def get_baseline_cached():
    conn = get_connection(DBConfig())
    try:
        baseline = get_or_create_baseline(
            conn=conn,
            db_config=DBConfig(),
            flood_config=FloodConfig(),
            force_recompute=False,
        )
        return baseline
    finally:
        conn.close()


def get_status_class(state: str) -> str:
    if state == "FLOOD DETECTED":
        return "status-flood"
    if state == "HIGH RISK OF FLOOD":
        return "status-high"
    if state == "ELEVATED RISK":
        return "status-elevated"
    return "status-normal"


def state_to_level(state: str) -> int:
    mapping = {
        "NORMAL": 0,
        "ELEVATED RISK": 1,
        "HIGH RISK OF FLOOD": 2,
        "FLOOD DETECTED": 3,
    }
    return mapping.get(state, 0)


def get_severity_badge(severity: str) -> str:
    sev = (severity or "").lower()
    if sev == "severe":
        return "badge-red"
    if sev == "medium":
        return "badge-amber"
    return "badge-green"


def operator_message(result: dict) -> str:
    state = result.get("current_state", "NORMAL")
    posture = result.get("operational_posture", "Continue normal monitoring.")
    event = result.get("current_event")
    features = result.get("recent_features", {})

    if state == "NORMAL":
        return (
            "Recent alarm activity remains within normal operating behavior. "
            f"The recent 1-minute alarm rate is {features.get('rate_1m', 0):.1f} alarms/min "
            "and no flood pattern is currently active. "
            f"Recommended posture: {posture}"
        )

    if state == "ELEVATED RISK":
        return (
            "The agent detected an early deviation from the recent normal pattern. "
            "This does not confirm a flood yet, but it suggests increasing attention "
            "to the affected area. "
            f"Recommended posture: {posture}"
        )

    if state == "HIGH RISK OF FLOOD":
        return (
            "Alarm behavior is showing a strong abnormal escalation relative to baseline. "
            "A flood may be developing if this pattern continues. "
            f"Recommended posture: {posture}"
        )

    if state == "FLOOD DETECTED" and event is not None:
        ev_type = event.get("flood_type_v11", "UNKNOWN")
        action = event.get("recommended_action", "no_action")
        return (
            f"A flood pattern is currently active and was classified as {ev_type}. "
            f"The recommended action is {action}. "
            f"Recommended posture: {posture}"
        )

    return posture


@st.cache_data(show_spinner=False)
def run_replay_cached(anchor_time_str: str, baseline_key: str) -> dict:
    """
    Cachea el resultado del replay por anchor_time.
    baseline_key se usa para invalidar el cache si cambia el baseline.
    """
    conn = get_connection(DBConfig())
    try:
        baseline = get_baseline_cached()
        result = assess_current_state(
            conn=conn,
            db_config=DBConfig(),
            flood_config=FloodConfig(),
            anchor_time=anchor_time_str,
            baseline=baseline,
        )
        return result
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def build_risk_timeline(anchor_time_str: str, lookback_minutes: int, baseline_key: str) -> pd.DataFrame:
    """
    Construye una línea de tiempo minuto a minuto terminando en anchor_time_str.
    Para cada minuto calcula el estado del agente.
    """
    anchor_dt = datetime.strptime(anchor_time_str, "%Y-%m-%d %H:%M:%S")

    rows = []
    for delta in range(lookback_minutes, -1, -1):
        ts = anchor_dt - pd.Timedelta(minutes=delta)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

        result = run_replay_cached(ts_str, baseline_key)
        if result is None:
            continue

        rows.append(
            {
                "timestamp": ts,
                "risk_score": result.get("risk_score", 0),
                "current_state": result.get("current_state", "NORMAL"),
                "flood_detected": result.get("flood_detected", False),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["timestamp", "risk_score", "current_state", "flood_detected"])

    return pd.DataFrame(rows)


def make_risk_score_chart(timeline_df: pd.DataFrame) -> alt.Chart:
    df = timeline_df.copy()

    chart = (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=3)
        .encode(
            x=alt.X(
                "timestamp:T",
                title="Time",
                axis=alt.Axis(
                    labelColor="#9CA3AF",
                    titleColor="#E5E7EB",
                    gridColor="#243041",
                    domainColor="#243041",
                    tickColor="#243041",
                    format="%H:%M",
                ),
            ),
            y=alt.Y(
                "risk_score:Q",
                title="Risk score",
                axis=alt.Axis(
                    labelColor="#9CA3AF",
                    titleColor="#E5E7EB",
                    gridColor="#243041",
                    domainColor="#243041",
                    tickColor="#243041",
                ),
                scale=alt.Scale(domain=[0, 100]),
            ),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Timestamp"),
                alt.Tooltip("risk_score:Q", title="Risk score", format=".1f"),
                alt.Tooltip("current_state:N", title="State"),
            ],
        )
        .properties(height=280)
        .configure_view(stroke=None)
        .configure(background="transparent")
    )

    return chart


def make_state_timeline_chart(timeline_df: pd.DataFrame) -> alt.Chart:
    df = timeline_df.copy()

    chart = (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=3)
        .encode(
            x=alt.X(
                "timestamp:T",
                title="Time",
                axis=alt.Axis(
                    labelColor="#9CA3AF",
                    titleColor="#E5E7EB",
                    gridColor="#243041",
                    domainColor="#243041",
                    tickColor="#243041",
                    format="%H:%M",
                ),
            ),
            y=alt.Y(
                "state_level:Q",
                title="Operational state",
                axis=alt.Axis(
                    values=[0, 1, 2, 3],
                    labelExpr="datum.value == 0 ? 'NORMAL' : datum.value == 1 ? 'ELEVATED' : datum.value == 2 ? 'HIGH RISK' : 'FLOOD'",
                    labelColor="#9CA3AF",
                    titleColor="#E5E7EB",
                    gridColor="#243041",
                    domainColor="#243041",
                    tickColor="#243041",
                ),
                scale=alt.Scale(domain=[0, 3]),
            ),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Timestamp"),
                alt.Tooltip("current_state:N", title="State"),
                alt.Tooltip("risk_score:Q", title="Risk score", format=".1f"),
            ],
        )
        .properties(height=280)
        .configure_view(stroke=None)
        .configure(background="transparent")
    )

    return chart


st.markdown('<div class="main-title">Industrial Alarm Intelligence</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="main-subtitle">Historical replay of operational alarm assessment</div>',
    unsafe_allow_html=True
)

with st.container():
    c1, c2, c3 = st.columns([1, 1, 0.8])

    with c1:
        selected_date = st.date_input("Replay date", value=datetime(2025, 5, 10).date())

    with c2:
        selected_time = st.time_input("Replay time", value=time(14, 35, 0), step=60)

    with c3:
        st.markdown("<div style='height: 1.85rem;'></div>", unsafe_allow_html=True)
        run_btn = st.button("Run replay", use_container_width=True)

quick_col1, quick_col2, quick_col3 = st.columns(3)
with quick_col1:
    if st.button("Load example 1", use_container_width=True):
        st.session_state["quick_anchor"] = "2025-12-27 23:56:15"
with quick_col2:
    if st.button("Load example 2", use_container_width=True):
        st.session_state["quick_anchor"] = "2025-05-10 14:30:00"
with quick_col3:
    if st.button("Load example 3", use_container_width=True):
        st.session_state["quick_anchor"] = "2025-05-10 14:35:00"

if "quick_anchor" in st.session_state and not run_btn:
    anchor_str = st.session_state["quick_anchor"]
else:
    anchor_dt = datetime.combine(selected_date, selected_time)
    anchor_str = anchor_dt.strftime("%Y-%m-%d %H:%M:%S")

baseline = get_baseline_cached()
baseline_key = (
    f"{baseline.get('scope', 'unknown')}_"
    f"{baseline.get('window_days', 'na')}_"
    f"{baseline.get('rate_p95', 'na')}_"
    f"{baseline.get('rate_p99', 'na')}"
)

if run_btn or "result" not in st.session_state or st.session_state.get("last_anchor") != anchor_str:
    with st.spinner("Running historical replay..."):
        result = run_replay_cached(anchor_str, baseline_key)
        st.session_state["result"] = result
        st.session_state["last_anchor"] = anchor_str

result = st.session_state.get("result")

if not result:
    st.stop()

timeline_df = build_risk_timeline(anchor_str, lookback_minutes=15, baseline_key=baseline_key)
if not timeline_df.empty:
    timeline_df["state_level"] = timeline_df["current_state"].apply(state_to_level)

state = result.get("current_state", "NORMAL")
status_class = get_status_class(state)
risk_score = int(result.get("risk_score", 0))
regime_change = result.get("regime_change", False)
posture = result.get("operational_posture", "Continue normal monitoring.")
features = result.get("recent_features", {})
event = result.get("current_event")
reasons = result.get("reasons", [])

event_type = event.get("flood_type_v11", "NO ACTIVE FLOOD") if event else "NO ACTIVE FLOOD"
event_severity = event.get("severity_v11", "none") if event else "none"
recommended_action = event.get("recommended_action", posture) if event else posture

k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown(f"""
    <div class="card">
        <div class="card-label">Current state</div>
        <div class="card-value {status_class}">{state}</div>
        <div class="card-help">Assessment at the selected replay time</div>
    </div>
    """, unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="card">
        <div class="card-label">Risk score</div>
        <div class="card-value">{risk_score}</div>
        <div class="card-help">Heuristic early-warning score from recent alarm behavior</div>
    </div>
    """, unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="card">
        <div class="card-label">Regime change</div>
        <div class="card-value">{str(regime_change).upper()}</div>
        <div class="card-help">Whether recent alarm dynamics differ from normal baseline</div>
    </div>
    """, unsafe_allow_html=True)

with k4:
    st.markdown(f"""
    <div class="card">
        <div class="card-label">Replay time</div>
        <div class="card-value" style="font-size:1.05rem;">{result.get("anchor_time", anchor_str)}</div>
        <div class="card-help">Historical point being evaluated</div>
    </div>
    """, unsafe_allow_html=True)

left, right = st.columns([1.15, 1])

with left:
    st.markdown(f"""
    <div class="panel">
        <div class="panel-title">Operator view</div>
        <div class="panel-body">
            {operator_message(result)}
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="panel">
        <div class="panel-title">Current posture</div>
        <div class="panel-body">
            <b>Recommended posture:</b> {posture}<br>
            <b>1-minute alarm rate:</b> {features.get("rate_1m", 0):.1f} alarms/min<br>
            <b>5-minute average:</b> {features.get("rate_5m_avg", 0):.1f} alarms/min<br>
            <b>15-minute average:</b> {features.get("rate_15m_avg", 0):.1f} alarms/min
        </div>
    </div>
    """, unsafe_allow_html=True)

with right:
    badge_class = get_severity_badge(event_severity)
    st.markdown(f"""
    <div class="panel">
        <div class="panel-title">Event interpretation</div>
        <div style="margin-bottom:10px;">
            <span class="badge badge-blue">{event_type}</span>
            <span class="badge {badge_class}">{str(event_severity).upper()}</span>
        </div>
        <div class="panel-body">
            <b>Recommended action:</b> {recommended_action}<br>
            <b>Flood detected:</b> {str(result.get("flood_detected", False))}<br>
            <b>Affected tags in 5 min:</b> {features.get("unique_tags_5m", 0)}<br>
            <b>Priority 1 share (5 min):</b> {features.get("prio1_share_5m", 0):.3f}
        </div>
    </div>
    """, unsafe_allow_html=True)

    if event:
        st.markdown(f"""
        <div class="panel">
            <div class="panel-title">Active event summary</div>
            <div class="panel-body">
                <b>Start:</b> {event.get("start", "N/A")}<br>
                <b>End:</b> {event.get("end", "N/A")}<br>
                <b>Duration:</b> {event.get("duration_min", "N/A")} min<br>
                <b>Total alarms:</b> {event.get("n", "N/A")}<br>
                <b>Unique tags:</b> {event.get("unique_tags", "N/A")}<br>
                <b>Max alarm rate:</b> {event.get("max_rate", "N/A")} alarms/min
            </div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("## Risk timeline")

if timeline_df.empty:
    st.markdown("""
    <div class="panel">
        <div class="panel-body">No timeline data could be computed for the selected replay time.</div>
    </div>
    """, unsafe_allow_html=True)
else:
    left_chart, right_chart = st.columns([1.3, 1])

    with left_chart:
        st.markdown("""
        <div class="panel">
            <div class="panel-title">Risk score evolution</div>
        </div>
        """, unsafe_allow_html=True)
        st.altair_chart(make_risk_score_chart(timeline_df), use_container_width=True)

    with right_chart:
        st.markdown("""
        <div class="panel">
            <div class="panel-title">Operational state evolution</div>
        </div>
        """, unsafe_allow_html=True)
        st.altair_chart(make_state_timeline_chart(timeline_df), use_container_width=True)

    st.markdown("""
    <div class="panel">
        <div class="panel-title">Timeline detail</div>
    </div>
    """, unsafe_allow_html=True)

    timeline_show = timeline_df.copy()
    timeline_show["timestamp"] = timeline_show["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    st.dataframe(
        timeline_show,
        use_container_width=True,
        hide_index=True,
    )

st.markdown("## Why the agent said this")

if reasons:
    for reason in reasons:
        st.markdown(f"""
        <div class="panel">
            <div class="panel-body">{reason}</div>
        </div>
        """, unsafe_allow_html=True)
else:
    st.markdown("""
    <div class="panel">
        <div class="panel-body">No significant early warning signals were activated for this replay window.</div>
    </div>
    """, unsafe_allow_html=True)

with st.expander("Show technical signals"):
    tech_cols_left, tech_cols_right = st.columns(2)

    with tech_cols_left:
        st.write("window_end:", features.get("window_end"))
        st.write("n_recent:", features.get("n_recent"))
        st.write("rate_1m:", features.get("rate_1m"))
        st.write("rate_5m_avg:", features.get("rate_5m_avg"))
        st.write("rate_15m_avg:", features.get("rate_15m_avg"))

    with tech_cols_right:
        st.write("rate_growth_1m_vs_5m:", features.get("rate_growth_1m_vs_5m"))
        st.write("prio1_share_5m:", features.get("prio1_share_5m"))
        st.write("unique_tags_5m:", features.get("unique_tags_5m"))
        st.write("new_tags_1m:", features.get("new_tags_1m"))
        st.write("rate_vs_p95:", features.get("rate_vs_p95"))
        st.write("rate_vs_p99:", features.get("rate_vs_p99"))