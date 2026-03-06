import streamlit as st


def load_css(css_file: str):
    with open(css_file, "r", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


def section_title(title: str, subtitle: str | None = None):
    st.markdown(f"""
    <div class="section-header">
        <div class="section-title">{title}</div>
        {f'<div class="section-subtitle">{subtitle}</div>' if subtitle else ''}
    </div>
    """, unsafe_allow_html=True)


def status_badge(label: str, kind: str = "neutral"):
    st.markdown(
        f'<span class="badge badge-{kind}">{label}</span>',
        unsafe_allow_html=True
    )


def summary_card(title: str, value: str, help_text: str = ""):
    st.markdown(f"""
    <div class="summary-card">
        <div class="summary-card-title">{title}</div>
        <div class="summary-card-value">{value}</div>
        <div class="summary-card-help">{help_text}</div>
    </div>
    """, unsafe_allow_html=True)


def info_card(title: str, body: str):
    st.markdown(f"""
    <div class="info-card">
        <div class="info-card-title">{title}</div>
        <div class="info-card-body">{body}</div>
    </div>
    """, unsafe_allow_html=True)