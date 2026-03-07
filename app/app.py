import sys
from pathlib import Path
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st
import base64

# Asegura que se pueda importar src/
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import DBConfig, FloodConfig, DataSourceConfig
from src.data_loader import load_alarms
from src.operational_agent import assess_current_state


st.set_page_config(
    page_title="Agente inteligente de alarmas",
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
.status-moderado { color: #FCD34D; font-weight: 700; }
.status-alto { color: #FB923C; font-weight: 700; }
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
    background: transparent;
    border: none;
    padding: 0;
    margin: 0;
    width: 100%;
    overflow: hidden;
}
.chart-panel {
    background: #111827;
    border: 1px solid #243041;
    border-radius: 16px;
    padding: 14px 14px 8px 14px;
    margin-bottom: 14px;
    overflow: hidden;
}
</style>
""", unsafe_allow_html=True)


@st.cache_data(show_spinner=False)
def load_alarms_cached() -> pd.DataFrame:
    df = load_alarms(
        data_cfg=DataSourceConfig(),
        db_cfg=DBConfig(),
        flood_cfg=FloodConfig(),
    )

    flood_cfg = FloodConfig()
    time_col = flood_cfg.time_col

    df = df.copy()
    if time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.dropna(subset=[time_col]).sort_values(time_col)

    return df


@st.cache_resource(show_spinner=False)
def get_baseline_cached():
    df_alarms = load_alarms_cached()
    flood_cfg = FloodConfig()
    time_col = flood_cfg.time_col

    df = df_alarms.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col]).sort_values(time_col)

    per_minute = (
        df.set_index(time_col)
        .resample("1min")
        .size()
        .rename("alarm_count")
    )

    if per_minute.empty:
        return {
            "scope": "csv_snapshot",
            "window_days": 0,
            "rate_p95": 0.0,
            "rate_p99": 0.0,
            "rate_p999": 0.0,
            "max_per_minute": 0,
            "n_minutes": 0,
        }

    return {
        "scope": "csv_snapshot",
        "window_days": 0,
        "rate_p95": float(per_minute.quantile(0.95)),
        "rate_p99": float(per_minute.quantile(0.99)),
        "rate_p999": float(per_minute.quantile(0.999)),
        "max_per_minute": int(per_minute.max()),
        "n_minutes": int(len(per_minute)),
    }


def traducir_estado(state: str) -> str:
    mapping = {
        "NORMAL": "NORMAL",
        "ELEVATED RISK": "RIESGO MODERADO",
        "HIGH RISK OF FLOOD": "RIESGO ALTO",
        "FLOOD DETECTED": "FLOOD",
    }
    return mapping.get(state, state)


def traducir_postura(posture: str) -> str:
    mapping = {
        "Continue normal monitoring.": "Continuar con monitoreo normal.",
        "Maintain enhanced monitoring and review the affected area.": "Mantener vigilancia reforzada y revisar el área afectada.",
        "Increase monitoring attention and prepare flood response.": "Incrementar la atención operativa y preparar respuesta ante flood.",
        "Escalate and prioritize alarm flood handling.": "Escalar el evento y priorizar la atención del flood de alarmas.",
    }
    return mapping.get(posture, posture)


def traducir_accion(action: str) -> str:
    mapping = {
        "no_action": "No se requiere acción inmediata",
        "notify_and_prioritize": "Notificar al operador y priorizar revisión",
        "group_and_prioritize": "Agrupar alarmas repetitivas y priorizar revisión",
        "auto_incident": "Generar incidente y escalar atención",
        "No se requiere acción inmediata": "No se requiere acción inmediata",
        "Revisar el área afectada y seguir la evolución de alarmas": "Revisar el área afectada y seguir la evolución de alarmas",
        "Priorizar revisión operativa y preparar escalamiento": "Priorizar revisión operativa y preparar escalamiento",
        "Escalar atención del evento de inmediato": "Escalar atención del evento de inmediato",
        "Revisar condición actual": "Revisar la condición actual",
        "Revisar evento": "Revisar evento",
    }
    return mapping.get(action, action)


def traducir_severidad(severity: str) -> str:
    mapping = {
        "none": "Sin severidad",
        "medium": "Media",
        "severe": "Severa",
    }
    return mapping.get((severity or "").lower(), severity)


def traducir_tipo_evento(event_type: str) -> str:
    mapping = {
        "SUBSYSTEM_TRIP_EVENT": "Disparo de subsistema",
        "CHATTERING_POINT": "Punto con alarmas repetitivas",
        "LOCAL_PROCESS_INSTABILITY": "Inestabilidad local de proceso",
        "INFRASTRUCTURE_EVENT": "Evento de infraestructura",
        "OTHER_OR_NO_FLOOD": "Sin flood clasificado",
        "SIN FLOOD ACTIVO": "SIN FLOOD ACTIVO",
    }
    return mapping.get(event_type, event_type)


def traducir_booleano(value: bool) -> str:
    return "Sí" if value else "No"


def get_status_class(state: str) -> str:
    if state == "FLOOD DETECTED":
        return "status-flood"
    if state == "HIGH RISK OF FLOOD":
        return "status-alto"
    if state == "ELEVATED RISK":
        return "status-moderado"
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
            "La actividad reciente de alarmas se mantiene dentro de un comportamiento operativo normal. "
            f"En el último minuto se observaron {features.get('rate_1m', 0):.1f} alarmas por minuto y, "
            "por el momento, no hay evidencia de un flood activo. "
            f"Postura recomendada: {posture}"
        )

    if state == "ELEVATED RISK":
        return (
            "El agente detectó una desviación temprana frente al patrón reciente normal. "
            "Todavía no confirma un flood, pero sí sugiere aumentar la atención sobre el área afectada "
            "y seguir de cerca la evolución de las alarmas. "
            f"Postura recomendada: {posture}"
        )

    if state == "HIGH RISK OF FLOOD":
        return (
            "El comportamiento de alarmas muestra una escalada anormal y sostenida frente a lo habitual. "
            "Si esta tendencia continúa, el sistema podría entrar en flood en los próximos minutos. "
            f"Postura recomendada: {posture}"
        )

    if state == "FLOOD DETECTED" and event is not None:
        ev_type = event.get("flood_type_v11", "UNKNOWN")
        action = event.get("recommended_action", "no_action")
        return (
            f"Se detectó un patrón de flood activo y fue interpretado como {ev_type}. "
            f"La acción recomendada es {action}. "
            f"Postura recomendada: {posture}"
        )

    return posture


def explain_reasons_for_operator(result: dict) -> list[str]:
    features = result.get("recent_features", {})
    state = result.get("current_state", "NORMAL")

    explanations = []

    rate_1m = float(features.get("rate_1m", 0))
    rate_5m = float(features.get("rate_5m_avg", 0))
    growth = float(features.get("rate_growth_1m_vs_5m", 0))
    prio1 = float(features.get("prio1_share_5m", 0))
    unique_tags = int(features.get("unique_tags_5m", 0))
    new_tags = int(features.get("new_tags_1m", 0))
    rate_vs_p95 = float(features.get("rate_vs_p95", 0))
    rate_vs_p99 = float(features.get("rate_vs_p99", 0))

    if rate_vs_p99 >= 1.0:
        explanations.append(
            "La intensidad reciente de alarmas entró en un rango superior al 99% del comportamiento histórico normal. "
            "Eso significa que el nivel actual es más alto que prácticamente todo lo que el sistema suele mostrar en operación habitual."
        )
    elif rate_vs_p95 >= 1.0:
        explanations.append(
            "La intensidad reciente de alarmas superó el nivel que históricamente solo se observa en el 5% de los periodos más altos. "
            "En otras palabras, el sistema está operando en una zona de actividad inusualmente elevada."
        )

    if growth >= 1.5:
        explanations.append(
            "La velocidad de llegada de alarmas aumentó con respecto a los últimos minutos. "
            "Esto sugiere que el evento no solo sigue activo, sino que además se está acelerando."
        )

    if rate_1m >= 1.3 * max(rate_5m, 1e-6):
        explanations.append(
            "En el último minuto se observó una concentración de alarmas mayor que la tendencia reciente. "
            "Ese cambio repentino suele ser una señal temprana de deterioro operativo."
        )

    if prio1 >= 0.60:
        explanations.append(
            "Una proporción alta de las alarmas recientes corresponde a señales de mayor prioridad. "
            "Esto indica que no se trata solo de más alarmas, sino de alarmas potencialmente más críticas para la operación."
        )
    elif prio1 >= 0.35:
        explanations.append(
            "La presencia de alarmas prioritarias aumentó en la ventana reciente. "
            "Aunque todavía no domina por completo el evento, sí apunta a una mayor exigencia operativa."
        )

    if unique_tags >= 20:
        explanations.append(
            "Las alarmas recientes involucran muchos puntos distintos del sistema. "
            "Eso sugiere que el evento se está propagando y no está concentrado en un solo origen."
        )
    elif unique_tags >= 8:
        explanations.append(
            "El evento reciente afecta varios puntos del sistema, no solo un único origen. "
            "Esto puede indicar que la perturbación ya se extendió más allá de una condición local."
        )

    if new_tags >= 10:
        explanations.append(
            "En el último minuto aparecieron nuevas alarmas en distintos puntos del proceso. "
            "Eso refuerza la idea de que el evento está creciendo en alcance."
        )
    elif new_tags >= 4:
        explanations.append(
            "El evento comenzó a extenderse hacia nuevos puntos del sistema. "
            "Aunque el crecimiento todavía es moderado, merece seguimiento cercano."
        )

    if state == "NORMAL" and not explanations:
        explanations.append(
            "No se observan señales recientes de deterioro operativo en el flujo de alarmas. "
            "La actividad actual se mantiene dentro de un comportamiento consistente con la operación normal."
        )

    return explanations


@st.cache_data(show_spinner=False)
def run_replay_cached(anchor_time_str: str, baseline_key: str) -> dict:
    df_alarms = load_alarms_cached()
    baseline = get_baseline_cached()

    result = assess_current_state(
        df_alarms=df_alarms,
        flood_config=FloodConfig(),
        anchor_time=anchor_time_str,
        baseline=baseline,
    )
    return result


@st.cache_data(show_spinner=False)
def build_risk_timeline(anchor_time_str: str, lookback_minutes: int, baseline_key: str) -> pd.DataFrame:
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

    return (
        alt.Chart(df)
        .mark_line(
            point=alt.OverlayMarkDef(size=60),
            strokeWidth=3,
            color="#60A5FA",
        )
        .encode(
            x=alt.X(
                "timestamp:T",
                title="Hora",
                axis=alt.Axis(
                    labelColor="#9CA3AF",
                    titleColor="#E5E7EB",
                    gridColor="#243041",
                    domainColor="#243041",
                    tickColor="#243041",
                    format="%H:%M",
                    labelAngle=0,
                ),
            ),
            y=alt.Y(
                "risk_score:Q",
                title="Puntaje de riesgo",
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
                alt.Tooltip("timestamp:T", title="Tiempo"),
                alt.Tooltip("risk_score:Q", title="Puntaje de riesgo", format=".1f"),
                alt.Tooltip("estado_mostrado:N", title="Estado"),
            ],
        )
        .properties(
            height=260,
            width="container",
        )
        .configure_view(stroke=None)
        .configure(background="transparent")
    )


def make_state_timeline_chart(timeline_df: pd.DataFrame) -> alt.Chart:
    df = timeline_df.copy()

    return (
        alt.Chart(df)
        .mark_line(
            point=alt.OverlayMarkDef(size=60),
            strokeWidth=3,
            color="#F59E0B",
        )
        .encode(
            x=alt.X(
                "timestamp:T",
                title="Hora",
                axis=alt.Axis(
                    labelColor="#9CA3AF",
                    titleColor="#E5E7EB",
                    gridColor="#243041",
                    domainColor="#243041",
                    tickColor="#243041",
                    format="%H:%M",
                    labelAngle=0,
                ),
            ),
            y=alt.Y(
                "state_level:Q",
                title="Estado operacional",
                axis=alt.Axis(
                    values=[0, 1, 2, 3],
                    labelExpr="datum.value == 0 ? 'NORMAL' : datum.value == 1 ? 'RIESGO MODERADO' : datum.value == 2 ? 'RIESGO ALTO' : 'FLOOD'",
                    labelColor="#9CA3AF",
                    titleColor="#E5E7EB",
                    gridColor="#243041",
                    domainColor="#243041",
                    tickColor="#243041",
                ),
                scale=alt.Scale(domain=[0, 3]),
            ),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Tiempo"),
                alt.Tooltip("estado_mostrado:N", title="Estado"),
                alt.Tooltip("risk_score:Q", title="Puntaje de riesgo", format=".1f"),
            ],
        )
        .properties(
            height=260,
            width="container",
        )
        .configure_view(stroke=None)
        .configure(background="transparent")
    )


# -----------------------------
# Preparación de datos para UI
# -----------------------------
df_ui = load_alarms_cached()
flood_cfg = FloodConfig()
time_col = flood_cfg.time_col

if df_ui.empty:
    st.error("El archivo CSV no contiene datos.")
    st.stop()

if time_col not in df_ui.columns:
    st.error(f"No existe la columna de tiempo esperada: {time_col}")
    st.stop()

df_ui = df_ui.copy()
df_ui[time_col] = pd.to_datetime(df_ui[time_col], errors="coerce")
df_ui = df_ui.dropna(subset=[time_col]).sort_values(time_col)

if df_ui.empty:
    st.error("No hay timestamps válidos en el CSV.")
    st.stop()

df_ui["timestamp_minute"] = df_ui[time_col].dt.floor("min")

available_dates = sorted(df_ui["timestamp_minute"].dt.date.unique())
min_dt = df_ui["timestamp_minute"].min()
max_dt = df_ui["timestamp_minute"].max()

example_points = (
    df_ui["timestamp_minute"]
    .drop_duplicates()
    .sort_values()
    .reset_index(drop=True)
)

if len(example_points) >= 3:
    example_1 = str(example_points.iloc[len(example_points) // 4].to_pydatetime())
    example_2 = str(example_points.iloc[len(example_points) // 2].to_pydatetime())
    example_3 = str(example_points.iloc[-1].to_pydatetime())
elif len(example_points) == 2:
    example_1 = str(example_points.iloc[0].to_pydatetime())
    example_2 = str(example_points.iloc[1].to_pydatetime())
    example_3 = str(example_points.iloc[1].to_pydatetime())
else:
    only_ts = str(example_points.iloc[0].to_pydatetime())
    example_1 = only_ts
    example_2 = only_ts
    example_3 = only_ts

def image_to_base64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def render_header_logos():
    se_logo_path = ROOT / "se_logo.png"
    ypf_logo_path = ROOT / "ypf_logo.png"

    if not se_logo_path.exists() or not ypf_logo_path.exists():
        st.warning("No se encontraron los logos en la raíz del proyecto.")
        return

    se_logo_b64 = image_to_base64(str(se_logo_path))
    ypf_logo_b64 = image_to_base64(str(ypf_logo_path))

    st.markdown(
        f"""
        <style>
        .logos-wrapper {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 2rem;
            background: #111827;
            border: 1px solid #243041;
            border-radius: 18px;
            padding: 18px 28px;
            margin-bottom: 1.2rem;
        }}
        .logo-box {{
            flex: 1;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 110px;
        }}
        .logo-box img {{
            max-height: 72px;
            max-width: 100%;
            width: auto;
            height: auto;
            object-fit: contain;
            display: block;
        }}
        .logo-se img {{
            max-height: 100px;
        }}
        
        .logo-ypf img {{
            max-height: 65px;
        }}
        @media (max-width: 900px) {{
            .logos-wrapper {{
                flex-direction: column;
                gap: 1rem;
            }}
        }}
        </style>

        <div class="logos-wrapper">
            <div class="logo-box logo-se">
                <img src="data:image/png;base64,{se_logo_b64}" alt="Schneider Electric">
            </div>
            
            <div class="logo-box logo-ypf">
                <img src="data:image/png;base64,{ypf_logo_b64}" alt="YPF">
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

render_header_logos()
st.markdown('<div class="main-title">Agente de alarmas industriales</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="main-subtitle">Reproducción histórica de la evaluación operacional del agente</div>',
    unsafe_allow_html=True
)

with st.container():
    c1, c2, c3 = st.columns([1, 1, 0.8])

    with c1:
        selected_date = st.date_input(
            "Fecha de replay",
            value=max_dt.date(),
            min_value=min_dt.date(),
            max_value=max_dt.date(),
        )

        if selected_date not in available_dates:
            selected_date = available_dates[-1]

    day_df = df_ui[df_ui["timestamp_minute"].dt.date == selected_date].copy()
    available_times = sorted(day_df["timestamp_minute"].dt.strftime("%H:%M:%S").unique().tolist())

    if not available_times:
        st.warning("No hay horas disponibles para la fecha seleccionada.")
        st.stop()

    with c2:
        selected_time_str = st.selectbox(
            "Hora de replay",
            options=available_times,
            index=len(available_times) - 1,
        )

    with c3:
        st.markdown("<div style='height: 1.85rem;'></div>", unsafe_allow_html=True)
        run_btn = st.button("Ejecutar replay", use_container_width=True)

quick_col1, quick_col2, quick_col3 = st.columns(3)
with quick_col1:
    if st.button("Cargar ejemplo 1", use_container_width=True):
        st.session_state["quick_anchor"] = example_1
with quick_col2:
    if st.button("Cargar ejemplo 2", use_container_width=True):
        st.session_state["quick_anchor"] = example_2
with quick_col3:
    if st.button("Cargar ejemplo 3", use_container_width=True):
        st.session_state["quick_anchor"] = example_3

if "quick_anchor" in st.session_state and not run_btn:
    anchor_str = st.session_state["quick_anchor"]
else:
    anchor_str = f"{selected_date} {selected_time_str}"

baseline = get_baseline_cached()
baseline_key = (
    f"{baseline.get('scope', 'unknown')}_"
    f"{baseline.get('window_days', 'na')}_"
    f"{baseline.get('rate_p95', 'na')}_"
    f"{baseline.get('rate_p99', 'na')}"
)

if run_btn or "result" not in st.session_state or st.session_state.get("last_anchor") != anchor_str:
    with st.spinner("Ejecutando replay histórico..."):
        result = run_replay_cached(anchor_str, baseline_key)
        st.session_state["result"] = result
        st.session_state["last_anchor"] = anchor_str

result = st.session_state.get("result")

if not result:
    st.stop()

timeline_df = build_risk_timeline(anchor_str, lookback_minutes=15, baseline_key=baseline_key)
if not timeline_df.empty:
    timeline_df["state_level"] = timeline_df["current_state"].apply(state_to_level)
    timeline_df["estado_mostrado"] = timeline_df["current_state"].apply(traducir_estado)

state = result.get("current_state", "NORMAL")
state_label = traducir_estado(state)
status_class = get_status_class(state)
risk_score = int(result.get("risk_score", 0))
regime_change = result.get("regime_change", False)

posture_raw = result.get("operational_posture", "Continue normal monitoring.")
posture = traducir_postura(posture_raw)

recommended_action_raw = result.get("recommended_action", "No se requiere acción inmediata")
recommended_action = traducir_accion(recommended_action_raw)

features = result.get("recent_features", {})
event = result.get("current_event")
operator_reasons = explain_reasons_for_operator(result)

event_type_raw = event.get("flood_type_v11", "SIN FLOOD ACTIVO") if event else "SIN FLOOD ACTIVO"
event_type = traducir_tipo_evento(event_type_raw)
event_severity_raw = event.get("severity_v11", "none") if event else "none"
event_severity = traducir_severidad(event_severity_raw)

k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown(f"""
    <div class="card">
        <div class="card-label">Estado actual</div>
        <div class="card-value {status_class}">{state_label}</div>
        <div class="card-help">Evaluación en el instante histórico seleccionado</div>
    </div>
    """, unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="card">
        <div class="card-label">Puntaje de riesgo</div>
        <div class="card-value">{risk_score}</div>
        <div class="card-help">Señal temprana basada en el comportamiento reciente de alarmas</div>
    </div>
    """, unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="card">
        <div class="card-label">Cambio de régimen</div>
        <div class="card-value">{str(regime_change).upper()}</div>
        <div class="card-help">Indica si la dinámica reciente difiere de la operación normal</div>
    </div>
    """, unsafe_allow_html=True)

with k4:
    st.markdown(f"""
    <div class="card">
        <div class="card-label">Hora evaluada</div>
        <div class="card-value" style="font-size:1.05rem;">{result.get("anchor_time", anchor_str)}</div>
        <div class="card-help">Punto histórico que está analizando el agente</div>
    </div>
    """, unsafe_allow_html=True)

left, right = st.columns([1.15, 1])

with left:
    st.markdown(f"""
    <div class="panel">
        <div class="panel-title">Visión para operación</div>
        <div class="panel-body">
            {operator_message(result)}
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="panel">
        <div class="panel-title">Postura actual</div>
        <div class="panel-body">
            <b>Postura recomendada:</b> {posture}<br>
            <b>Tasa de alarmas en 1 minuto:</b> {features.get("rate_1m", 0):.1f} alarmas/min<br>
            <b>Promedio en 5 minutos:</b> {features.get("rate_5m_avg", 0):.1f} alarmas/min<br>
            <b>Promedio en 15 minutos:</b> {features.get("rate_15m_avg", 0):.1f} alarmas/min
        </div>
    </div>
    """, unsafe_allow_html=True)

with right:
    flood_detectado_txt = "Sí" if result.get("flood_detected", False) else "No"
    badge_class = get_severity_badge(event_severity_raw)

    if event:
        badges_html = (
            f'<span class="badge badge-blue">{event_type}</span>'
            f'<span class="badge {badge_class}">{event_severity.upper()}</span>'
        )
    else:
        badges_html = '<span class="badge badge-green">SIN FLOOD ACTIVO</span>'

    panel_html = f"""
    <div class="panel">
        <div class="panel-title">Interpretación del evento</div>
        <div style="margin-bottom:10px;">
            {badges_html}
        </div>
        <div class="panel-body">
            <b>Acción recomendada:</b> {recommended_action}<br>
            <b>Flood detectado:</b> {flood_detectado_txt}<br>
            <b>Puntos afectados en 5 min:</b> {features.get('unique_tags_5m', 0)}<br>
            <b>Proporción de prioridad 1 en 5 min:</b> {features.get('prio1_share_5m', 0):.3f}
        </div>
    </div>
    """

    st.markdown(panel_html, unsafe_allow_html=True)

    if event:
        st.markdown(f"""
        <div class="panel">
            <div class="panel-title">Resumen del evento activo</div>
            <div class="panel-body">
                <b>Inicio:</b> {event.get("start", "N/A")}<br>
                <b>Fin:</b> {event.get("end", "N/A")}<br>
                <b>Duración:</b> {event.get("duration_min", "N/A")} min<br>
                <b>Total de alarmas:</b> {event.get("n", "N/A")}<br>
                <b>Tags únicos:</b> {event.get("unique_tags", "N/A")}<br>
                <b>Tasa máxima:</b> {event.get("max_rate", "N/A")} alarmas/min
            </div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("## Evolución del riesgo")

if timeline_df.empty:
    st.markdown("""
    <div class="panel">
        <div class="panel-body">No fue posible construir la línea de tiempo para el instante seleccionado.</div>
    </div>
    """, unsafe_allow_html=True)
else:
    left_chart, right_chart = st.columns([1, 1])

    with left_chart:
        st.markdown("""
        <div class="chart-panel">
            <div class="panel-title">Evolución del puntaje de riesgo</div>
        """, unsafe_allow_html=True)
        st.altair_chart(make_risk_score_chart(timeline_df), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with right_chart:
        st.markdown("""
        <div class="chart-panel">
            <div class="panel-title">Evolución del estado operacional</div>
        """, unsafe_allow_html=True)
        st.altair_chart(make_state_timeline_chart(timeline_df), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

st.markdown("## Señales operacionales observadas")

for reason in operator_reasons:
    st.markdown(f"""
    <div class="panel">
        <div class="panel-body">{reason}</div>
    </div>
    """, unsafe_allow_html=True)
