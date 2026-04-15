import io
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from azure.storage.blob import BlobServiceClient

# Azure config
STORAGE_ACCOUNT_NAME = "groupdata479storage"
STORAGE_ACCOUNT_KEY  = "0/+rC2RZGQpeAB1ZwXd+9Flw0uaFibsUqHBC6YhhlrAE/AFLIbORx/vMVxpA6LvB4KtyT9aXeWz7+AStFSutyw=="
CONTAINER_NAME       = "gsod-data"
TASK2_PREFIX         = "task2_output/"
INVALID_TEMP         = 9999.9

st.set_page_config(
    page_title="NOAA Climate Dashboard",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Global CSS — Apple Weather inspired
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

/* Sky gradient background */
html, body,
[data-testid="stAppViewContainer"],
[data-testid="stMain"] {
    background: linear-gradient(170deg,
        #0d1b35 0%,
        #0f2f5a 25%,
        #163d6e 50%,
        #1a4a82 70%,
        #1e5490 100%) !important;
    font-family: 'Inter', system-ui, sans-serif !important;
    color: #f0f4ff !important;
    min-height: 100vh;
}

/* Main block padding */
[data-testid="block-container"] {
    padding-top: 2rem;
    padding-bottom: 4rem;
}

/* Sidebar — deep glass */
[data-testid="stSidebar"] {
    background: rgba(8, 20, 45, 0.75) !important;
    backdrop-filter: blur(24px) !important;
    -webkit-backdrop-filter: blur(24px) !important;
    border-right: 1px solid rgba(255,255,255,0.08) !important;
}
[data-testid="stSidebar"] * { color: #d8e8ff !important; }

/* Hide Streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stToolbar"] { display: none; }

/* Glass card */
.glass-card {
    background: rgba(255, 255, 255, 0.07);
    backdrop-filter: blur(20px);
    -webkit-backdrop-filter: blur(20px);
    border: 1px solid rgba(255,255,255,0.13);
    border-radius: 20px;
    padding: 22px 26px;
    margin-bottom: 16px;
    transition: border-color 0.2s;
}
.glass-card:hover { border-color: rgba(255,255,255,0.22); }

/* Hero temp display */
.hero-wrap {
    text-align: center;
    padding: 32px 0 24px;
}
.hero-label {
    font-size: 0.78rem;
    font-weight: 600;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: rgba(255,255,255,0.45);
    margin-bottom: 4px;
}
.hero-temp {
    font-size: 5.5rem;
    font-weight: 200;
    color: #ffffff;
    line-height: 1;
    letter-spacing: -4px;
}
.hero-temp sup {
    font-size: 2rem;
    font-weight: 400;
    vertical-align: super;
    letter-spacing: 0;
}
.hero-sub {
    font-size: 0.95rem;
    color: rgba(255,255,255,0.55);
    margin-top: 8px;
}

/* KPI strip */
.kpi-row { display: flex; gap: 12px; margin-bottom: 24px; }
.kpi-card {
    flex: 1;
    background: rgba(255,255,255,0.07);
    backdrop-filter: blur(16px);
    border: 1px solid rgba(255,255,255,0.11);
    border-radius: 16px;
    padding: 16px 18px;
    text-align: center;
}
.kpi-icon { font-size: 1.4rem; margin-bottom: 4px; }
.kpi-val  { font-size: 1.5rem; font-weight: 700; color: #fff; line-height: 1.1; }
.kpi-lbl  { font-size: 0.7rem; font-weight: 500; letter-spacing: 0.06em;
             text-transform: uppercase; color: rgba(255,255,255,0.42); margin-top: 3px; }

/* Section headers */
.sec-title {
    font-size: 1.15rem;
    font-weight: 700;
    color: #fff;
    margin: 32px 0 2px;
    display: flex;
    align-items: center;
    gap: 8px;
}
.sec-sub {
    font-size: 0.82rem;
    color: rgba(255,255,255,0.42);
    margin-bottom: 14px;
}

/* Insight pill */
.insight {
    background: rgba(255,255,255,0.07);
    border: 1px solid rgba(255,255,255,0.12);
    border-radius: 10px;
    padding: 10px 16px;
    font-size: 0.83rem;
    color: rgba(255,255,255,0.7);
    margin: 8px 0 16px;
}

/* Sidebar brand */
.sb-brand {
    font-size: 1rem; font-weight: 700;
    color: #7ec8ff !important;
    letter-spacing: 0.02em;
}
.sb-stat {
    display: flex; justify-content: space-between;
    font-size: 0.8rem; padding: 5px 0;
    border-bottom: 1px solid rgba(255,255,255,0.07);
    color: rgba(255,255,255,0.45) !important;
}
.sb-stat span { color: #ffffff !important; font-weight: 600; }

/* Preset buttons */
button[kind="secondary"] {
    background: rgba(255,255,255,0.08) !important;
    border: 1px solid rgba(255,255,255,0.14) !important;
    color: #c8dfff !important;
    border-radius: 10px !important;
    font-size: 0.78rem !important;
    font-weight: 500 !important;
}
button[kind="secondary"]:hover {
    background: rgba(255,255,255,0.16) !important;
    border-color: rgba(255,255,255,0.28) !important;
}

/* Dataframe */
[data-testid="stDataFrame"] { border-radius: 14px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)


# Chart defaults — transparent glass feel
GLASS_BG  = "rgba(255,255,255,0.05)"
GRID_COL  = "rgba(255,255,255,0.07)"
TEXT_COL  = "rgba(255,255,255,0.85)"
FONT      = "Inter, system-ui, sans-serif"

# Temperature colour scales
HOT_SCALE  = [[0,"#1a3560"],[0.4,"#e67e22"],[1,"#ff3b30"]]
COLD_SCALE = [[0,"#1a3560"],[0.4,"#4fc3f7"],[1,"#b3e5fc"]]
DIV_SCALE  = [[0,"#4fc3f7"],[0.5,"rgba(255,255,255,0.15)"],[1,"#ff6b35"]]
YEAR_COLS  = ["#7ec8ff","#ffb347","#82e0aa"]

def glassy(fig, title="", height=420):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=GLASS_BG,
        plot_bgcolor=GLASS_BG,
        font=dict(family=FONT, color=TEXT_COL, size=11),
        title=dict(text=title, font=dict(size=13, color="#ffffff", weight=600),
                   x=0.02, xanchor="left") if title else None,
        height=height,
        margin=dict(l=44, r=20, t=44 if title else 16, b=40),
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0, font=dict(size=10)),
        xaxis=dict(gridcolor=GRID_COL, zerolinecolor=GRID_COL,
                   tickfont=dict(size=10), linecolor="rgba(0,0,0,0)"),
        yaxis=dict(gridcolor=GRID_COL, zerolinecolor=GRID_COL,
                   tickfont=dict(size=10), linecolor="rgba(0,0,0,0)"),
    )
    fig.update_traces(hoverlabel=dict(
        bgcolor="rgba(10,25,55,0.92)",
        bordercolor="rgba(255,255,255,0.2)",
        font=dict(family=FONT, size=12, color="#ffffff"),
    ))
    return fig


# Data loaders
@st.cache_data(show_spinner=False)
def load_aggregated():
    svc = BlobServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        credential=STORAGE_ACCOUNT_KEY,
    )
    ct = svc.get_container_client(CONTAINER_NAME)
    frames = []
    for blob in ct.list_blobs(name_starts_with=TASK2_PREFIX):
        if blob.name.endswith(".csv") and "part-" in blob.name:
            frames.append(pd.read_csv(io.BytesIO(ct.get_blob_client(blob.name).download_blob().readall())))
    df = pd.concat(frames, ignore_index=True)
    df.columns = ["station", "year", "average_temperature"]
    df["year"]                = df["year"].astype(int)
    df["average_temperature"] = df["average_temperature"].astype(float)
    return df

@st.cache_data(show_spinner=False)
def load_daily():
    svc = BlobServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        credential=STORAGE_ACCOUNT_KEY,
    )
    ct = svc.get_container_client(CONTAINER_NAME)
    frames = []
    for blob in ct.list_blobs():
        n = blob.name
        if not n.endswith(".csv") or n.startswith("task2_output"):
            continue
        if not n.split("/")[-1].startswith("01"):
            continue
        try:
            frames.append(pd.read_csv(
                io.BytesIO(ct.get_blob_client(n).download_blob().readall()),
                usecols=["STATION","DATE","TEMP"],
            ))
        except Exception:
            continue
    daily = pd.concat(frames, ignore_index=True)
    daily.rename(columns={"STATION":"station"}, inplace=True)
    daily["year"] = daily["DATE"].astype(str).str[:4].astype(int)
    daily["TEMP"] = pd.to_numeric(daily["TEMP"], errors="coerce")
    return daily[(daily["TEMP"].notna()) & (daily["TEMP"] != INVALID_TEMP)][["station","year","TEMP"]]


# Sidebar
with st.sidebar:
    st.markdown('<div class="sb-brand">🌡️ NOAA Climate</div>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    with st.spinner("Loading data…"):
        df  = load_aggregated()
        raw = load_daily()

    years        = sorted(df["year"].unique())
    all_stations = sorted(df["station"].unique())
    station_means = df.groupby("station")["average_temperature"].mean()

    PRESETS = {
        "🔴 Hottest 10":  list(station_means.nlargest(10).index),
        "🔵 Coldest 10":  list(station_means.nsmallest(10).index),
        "〰️ Most Variable": list(raw.groupby("station")["TEMP"].std().nlargest(10).index),
        "✏️ Custom":      st.session_state.get("sel_stations", all_stations[:10]),
    }

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("**Quick Presets**")
    for label, col_pair in [
        ("🔴 Hottest 10",     (st.columns(2)[0])),
        ("🔵 Coldest 10",     (st.columns(2)[1])),
    ]:
        pass

    # 2×2 preset grid
    r1c1, r1c2 = st.columns(2)
    r2c1, r2c2 = st.columns(2)
    for label, col in [
        ("🔴 Hottest 10", r1c1), ("🔵 Coldest 10", r1c2),
        ("〰️ Variable",   r2c1), ("✏️ Custom",     r2c2),
    ]:
        if col.button(label, use_container_width=True):
            key = label if label in PRESETS else "✏️ Custom"
            st.session_state["preset"]       = label
            st.session_state["sel_stations"] = PRESETS.get(label, [])

    st.markdown("<br>", unsafe_allow_html=True)
    sel_stations = st.multiselect(
        label="Search stations",
        options=all_stations,
        default=[s for s in st.session_state.get("sel_stations", all_stations[:10]) if s in all_stations],
        placeholder="🔍  Type a station ID…",
    )
    st.session_state["sel_stations"] = sel_stations

    active = st.session_state.get("preset", "—")
    st.caption(f"Preset: **{active}** · {len(sel_stations)} selected")

    filtered = df[df["station"].isin(sel_stations)].copy() if sel_stations else df.copy()

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("**Overview**")
    for label, val in [
        ("Stations",      df["station"].nunique()),
        ("Years",         f"{min(years)} – {max(years)}"),
        ("Agg records",   f"{len(df):,}"),
        ("Daily records", f"{len(raw):,}"),
    ]:
        st.markdown(
            f'<div class="sb-stat">{label}<span>{val}</span></div>',
            unsafe_allow_html=True,
        )
    st.markdown("<br>", unsafe_allow_html=True)
    st.caption("DATA 479 · Winter 2026")


# ══════════════════════════════════════════════════════════════════════════════
# Hero
# ══════════════════════════════════════════════════════════════════════════════
global_avg   = df["average_temperature"].mean()
global_max   = df["average_temperature"].max()
global_min   = df["average_temperature"].min()
n_stations   = df["station"].nunique()

st.markdown(f"""
<div class="hero-wrap">
  <div class="hero-label">Global Average Temperature · 2020 – 2022</div>
  <div class="hero-temp">{global_avg:.1f}<sup>°</sup></div>
  <div class="hero-sub">NOAA GSOD · {n_stations} weather stations · Fahrenheit × 0.1</div>
</div>
""", unsafe_allow_html=True)

# KPI strip
st.markdown(f"""
<div class="kpi-row">
  <div class="kpi-card">
    <div class="kpi-icon">🌡️</div>
    <div class="kpi-val">{global_avg:.1f}</div>
    <div class="kpi-lbl">Global Avg</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-icon">🔴</div>
    <div class="kpi-val">{global_max:.1f}</div>
    <div class="kpi-lbl">Highest Recorded</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-icon">🔵</div>
    <div class="kpi-val">{global_min:.1f}</div>
    <div class="kpi-lbl">Lowest Recorded</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-icon">📡</div>
    <div class="kpi-val">{n_stations}</div>
    <div class="kpi-lbl">Stations</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-icon">📅</div>
    <div class="kpi-val">{len(raw):,}</div>
    <div class="kpi-lbl">Daily Records</div>
  </div>
</div>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# 1 · Long-term Temperature Trends
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="sec-title">🌤  Long-term Temperature Trends</div>', unsafe_allow_html=True)
st.markdown('<div class="sec-sub">How each station\'s average annual temperature shifted over 2020 – 2022.</div>', unsafe_allow_html=True)

col_l, col_r = st.columns([2, 1], gap="medium")

with col_l:
    fig = px.line(
        filtered.sort_values(["station","year"]),
        x="year", y="average_temperature", color="station",
        markers=True,
        labels={"average_temperature":"Avg Temp (°F×0.1)", "year":"Year", "station":"Station"},
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig.update_traces(line=dict(width=2.5), marker=dict(size=8, line=dict(color="rgba(0,0,0,0.4)", width=1)))
    fig.update_layout(xaxis=dict(tickmode="array", tickvals=years))
    glassy(fig, "Annual Average Temperature by Station", 420)
    st.plotly_chart(fig, use_container_width=True)

with col_r:
    global_trend = df.groupby("year")["average_temperature"].mean().reset_index()
    global_trend.rename(columns={"average_temperature":"global_avg"}, inplace=True)

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=global_trend["year"], y=global_trend["global_avg"],
        mode="lines+markers+text",
        line=dict(color="#7ec8ff", width=3),
        marker=dict(size=11, color="#7ec8ff", line=dict(color="rgba(0,0,0,0.5)", width=2)),
        text=[f"{v:.1f}°" for v in global_trend["global_avg"]],
        textposition="top center",
        textfont=dict(color="#ffffff", size=13, weight=700),
        fill="tozeroy",
        fillcolor="rgba(126,200,255,0.08)",
    ))
    fig2.update_layout(xaxis=dict(tickmode="array", tickvals=years), showlegend=False)
    glassy(fig2, "Global Average Trend", 210)
    st.plotly_chart(fig2, use_container_width=True)

    delta = (
        filtered.sort_values(["station","year"])
        .assign(temp_change=lambda d: d.groupby("station")["average_temperature"].diff())
        .dropna(subset=["temp_change"])
    )
    warming = (delta["temp_change"] > 0).sum()
    cooling = (delta["temp_change"] <= 0).sum()
    st.markdown(
        f'<div class="insight">📈 <b>{warming}</b> station-years warmed &nbsp;·&nbsp; ❄️ <b>{cooling}</b> cooled</div>',
        unsafe_allow_html=True,
    )

# YoY delta chart
if not delta.empty:
    fig3 = px.bar(
        delta.sort_values(["year","temp_change"]),
        x="station", y="temp_change", color="year",
        barmode="group",
        color_discrete_sequence=YEAR_COLS,
        labels={"temp_change":"Temp Change (°F×0.1)", "station":"Station", "year":"Year"},
    )
    fig3.update_layout(xaxis_tickangle=-45)
    glassy(fig3, "Year-over-Year Temperature Change per Station", 360)
    st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# 2 · Station Comparison
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="sec-title">📊  Station Comparison</div>', unsafe_allow_html=True)
st.markdown('<div class="sec-sub">Side-by-side temperature profiles to highlight regional and climatic differences.</div>', unsafe_allow_html=True)

pivot = filtered.pivot_table(index="station", columns="year", values="average_temperature")
fig_heat = px.imshow(
    pivot,
    color_continuous_scale="RdYlBu_r",
    aspect="auto",
    labels={"color":"Avg Temp (°F×0.1)", "x":"Year", "y":"Station"},
)
fig_heat.update_coloraxes(colorbar=dict(
    thickness=12, len=0.75,
    tickfont=dict(size=10, color=TEXT_COL),
    title=dict(text="°F×0.1", font=dict(size=10)),
))
glassy(fig_heat, "Temperature Heatmap — Station × Year", 500)
st.plotly_chart(fig_heat, use_container_width=True)

col_l, col_r = st.columns(2, gap="medium")
with col_l:
    fig_bar = px.bar(
        filtered.sort_values(["year","average_temperature"], ascending=[True,False]),
        x="station", y="average_temperature", color="year",
        barmode="group",
        color_discrete_sequence=YEAR_COLS,
        labels={"average_temperature":"Avg Temp", "station":"Station", "year":"Year"},
    )
    fig_bar.update_layout(xaxis_tickangle=-45)
    glassy(fig_bar, "Avg Temp per Station per Year", 380)
    st.plotly_chart(fig_bar, use_container_width=True)

with col_r:
    fig_box = px.box(
        filtered, x="year", y="average_temperature", points="all",
        color="year", color_discrete_sequence=YEAR_COLS,
        labels={"average_temperature":"Avg Temp", "year":"Year"},
    )
    fig_box.update_layout(xaxis=dict(tickmode="array", tickvals=years), showlegend=False)
    glassy(fig_box, "Temperature Distribution per Year", 380)
    st.plotly_chart(fig_box, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# 3 · Temperature Variability
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="sec-title">〰️  Temperature Variability</div>', unsafe_allow_html=True)
st.markdown('<div class="sec-sub">Std deviation of daily temps per station/year — higher = more volatile weather.</div>', unsafe_allow_html=True)

raw_f = raw[raw["station"].isin(sel_stations)].copy() if sel_stations else raw.copy()
variability = (
    raw_f.groupby(["station","year"])["TEMP"]
    .agg(std_temp="std", count="count")
    .reset_index()
    .dropna(subset=["std_temp"])
)
variability = variability[variability["count"] >= 30]

if variability.empty:
    st.warning("Not enough daily observations for selected stations.")
else:
    fig_var = px.bar(
        variability.sort_values(["year","std_temp"], ascending=[True,False]),
        x="station", y="std_temp", color="year",
        barmode="group",
        color_discrete_sequence=YEAR_COLS,
        labels={"std_temp":"Std Dev (°F×0.1)", "station":"Station", "year":"Year"},
    )
    fig_var.update_layout(xaxis_tickangle=-45)
    glassy(fig_var, "Daily Temperature Variability (Std Dev) per Station", 360)
    st.plotly_chart(fig_var, use_container_width=True)

    col_l, col_r = st.columns(2, gap="medium")
    with col_l:
        mv = variability.groupby("station")["std_temp"].mean().nlargest(8).reset_index().rename(columns={"std_temp":"avg_std"})
        fig_mv = go.Figure(go.Bar(
            x=mv["avg_std"], y=mv["station"].astype(str), orientation="h",
            marker=dict(color=mv["avg_std"], colorscale=HOT_SCALE, showscale=False),
            text=[f"{v:.1f}" for v in mv["avg_std"]], textposition="outside",
            textfont=dict(color=TEXT_COL, size=10),
        ))
        fig_mv.update_layout(yaxis=dict(autorange="reversed"))
        glassy(fig_mv, "🌡️ Most Variable Stations", 340)
        st.plotly_chart(fig_mv, use_container_width=True)

    with col_r:
        lv = variability.groupby("station")["std_temp"].mean().nsmallest(8).reset_index().rename(columns={"std_temp":"avg_std"})
        fig_lv = go.Figure(go.Bar(
            x=lv["avg_std"], y=lv["station"].astype(str), orientation="h",
            marker=dict(color=lv["avg_std"], colorscale=COLD_SCALE, showscale=False),
            text=[f"{v:.1f}" for v in lv["avg_std"]], textposition="outside",
            textfont=dict(color=TEXT_COL, size=10),
        ))
        fig_lv.update_layout(yaxis=dict(autorange="reversed"))
        glassy(fig_lv, "❄️ Most Stable Stations", 340)
        st.plotly_chart(fig_lv, use_container_width=True)

    yv = variability.groupby("year")["std_temp"].mean().reset_index().rename(columns={"std_temp":"mean_std"})
    om, os_ = yv["mean_std"].mean(), yv["mean_std"].std()
    yv["z_score"]   = (yv["mean_std"] - om) / (os_ if os_ > 0 else 1)
    yv["Assessment"] = yv["z_score"].apply(
        lambda z: "⚠️ High" if z > 1 else ("✅ Low" if z < -1 else "— Normal")
    )
    st.dataframe(
        yv.rename(columns={"year":"Year","mean_std":"Mean Std Dev","z_score":"Z-Score"}).set_index("Year"),
        use_container_width=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# 4 · Extreme Weather Indicators
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="sec-title">⚡  Extreme Weather Indicators</div>', unsafe_allow_html=True)
st.markdown('<div class="sec-sub">Hottest/coldest stations and IQR-based outlier detection across all years.</div>', unsafe_allow_html=True)

col_l, col_r = st.columns(2, gap="medium")
sm = df.groupby("station")["average_temperature"].mean()

with col_l:
    top8 = sm.nlargest(8).reset_index().rename(columns={"average_temperature":"mean_temp"})
    fig_hot = go.Figure(go.Bar(
        x=top8["mean_temp"], y=top8["station"].astype(str), orientation="h",
        marker=dict(color=top8["mean_temp"], colorscale=HOT_SCALE, showscale=False),
        text=[f"{v:.1f}°" for v in top8["mean_temp"]], textposition="outside",
        textfont=dict(color="#ffffff", size=11),
    ))
    fig_hot.update_layout(yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False))
    glassy(fig_hot, "🔴  Hottest Stations (avg 2020–22)", 340)
    st.plotly_chart(fig_hot, use_container_width=True)

with col_r:
    bot8 = sm.nsmallest(8).reset_index().rename(columns={"average_temperature":"mean_temp"})
    fig_cold = go.Figure(go.Bar(
        x=bot8["mean_temp"], y=bot8["station"].astype(str), orientation="h",
        marker=dict(color=bot8["mean_temp"], colorscale=COLD_SCALE, showscale=False),
        text=[f"{v:.1f}°" for v in bot8["mean_temp"]], textposition="outside",
        textfont=dict(color="#ffffff", size=11),
    ))
    fig_cold.update_layout(yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False))
    glassy(fig_cold, "🔵  Coldest Stations (avg 2020–22)", 340)
    st.plotly_chart(fig_cold, use_container_width=True)

# IQR outlier scatter
outlier_records = []
for yr, grp in df.groupby("year"):
    q1, q3 = grp["average_temperature"].quantile([0.25,0.75])
    iqr = q3 - q1
    lo, hi = q1 - 1.5*iqr, q3 + 1.5*iqr
    out = grp[(grp["average_temperature"] < lo) | (grp["average_temperature"] > hi)].copy()
    out["flag"] = out["average_temperature"].apply(lambda t: "Extreme Hot" if t > hi else "Extreme Cold")
    out["lower_fence"] = round(lo, 2)
    out["upper_fence"] = round(hi, 2)
    outlier_records.append(out)

outliers = pd.concat(outlier_records, ignore_index=True)
normal   = df[~df.index.isin(outliers.index)]

fig_out = go.Figure()
fig_out.add_trace(go.Scatter(
    x=normal["year"], y=normal["average_temperature"],
    mode="markers",
    marker=dict(color="rgba(255,255,255,0.15)", size=5),
    name="Normal",
    hovertemplate="Station: %{customdata}<br>Temp: %{y:.1f}°<extra></extra>",
    customdata=normal["station"],
))
for flag, color, sym in [("Extreme Hot","#ff6b35","diamond"), ("Extreme Cold","#7ec8ff","diamond")]:
    sub = outliers[outliers["flag"]==flag]
    if not sub.empty:
        fig_out.add_trace(go.Scatter(
            x=sub["year"], y=sub["average_temperature"],
            mode="markers",
            marker=dict(color=color, size=14, symbol=sym,
                        line=dict(color="rgba(255,255,255,0.6)", width=1.5)),
            name=flag,
            hovertemplate="<b>%{customdata}</b><br>Temp: %{y:.1f}°<br>Year: %{x}<extra></extra>",
            customdata=sub["station"],
        ))
fig_out.update_layout(xaxis=dict(tickmode="array", tickvals=years))
glassy(fig_out, f"⚡  IQR Outlier Detection — {len(outliers)} Extreme Records", 420)
st.plotly_chart(fig_out, use_container_width=True)

# Anomaly deviation bubble chart
yearly_mean = df.groupby("year")["average_temperature"].mean().rename("global_mean")
anomaly     = df.merge(yearly_mean, on="year")
anomaly["deviation"] = anomaly["average_temperature"] - anomaly["global_mean"]
anom_plot            = anomaly[anomaly["station"].isin(sel_stations)].copy() if sel_stations else anomaly.copy()
anom_plot["abs_dev"] = anom_plot["deviation"].abs().clip(lower=0.5)

fig_anom = px.scatter(
    anom_plot, x="year", y="deviation",
    color="deviation", size="abs_dev", size_max=22,
    color_continuous_scale="RdBu_r",
    range_color=[anom_plot["deviation"].min(), anom_plot["deviation"].max()],
    hover_data={"station":True,"year":True,"average_temperature":":.1f",
                "global_mean":":.1f","deviation":":.2f","abs_dev":False},
    labels={"deviation":"Deviation from Global Mean", "year":"Year"},
)
fig_anom.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.3)", line_width=1.5)
fig_anom.update_layout(
    xaxis=dict(tickmode="array", tickvals=years),
    coloraxis_colorbar=dict(title="Dev", thickness=12, len=0.7,
                            tickfont=dict(size=9, color=TEXT_COL)),
)
glassy(fig_anom, "Temperature Anomaly — Deviation from Global Annual Mean", 440)
st.plotly_chart(fig_anom, use_container_width=True)

hot_st = anom_plot.loc[anom_plot["deviation"].idxmax()]
cld_st = anom_plot.loc[anom_plot["deviation"].idxmin()]
st.markdown(
    f'<div class="insight">'
    f'🔴 Largest warm anomaly: <b>{hot_st["deviation"]:.2f}°</b> — station {hot_st["station"]} &nbsp;·&nbsp; '
    f'🔵 Largest cold anomaly: <b>{cld_st["deviation"]:.2f}°</b> — station {cld_st["station"]}'
    f'</div>',
    unsafe_allow_html=True,
)

with st.expander("View extreme weather records"):
    st.dataframe(
        outliers[["station","year","average_temperature","flag","lower_fence","upper_fence"]]
        .sort_values(["year","average_temperature"]).reset_index(drop=True)
        .rename(columns={"station":"Station","year":"Year","average_temperature":"Avg Temp",
                         "flag":"Type","lower_fence":"Lower Fence","upper_fence":"Upper Fence"}),
        use_container_width=True,
    )


# References
#
# [1] Streamlit Inc. st.multiselect API Reference. (Widgets, custom HTML/CSS injection)
#     https://docs.streamlit.io/develop/api-reference/widgets/st.multiselect
#
# [2] Plotly Technologies. Python Graph Objects API Reference. (Scatter, Bar, Heatmap charts)
#     https://plotly.com/python-api-reference/generated/plotly.graph_objects.Scatter.html
#
# [3] Comeau, J. An Interactive Guide to the CSS backdrop-filter Property.
#     (Glassmorphism / frosted glass card design)
#     https://www.joshwcomeau.com/css/backdrop-filter/
#
# [4] Apple Inc. Human Interface Guidelines. (Weather app-inspired colour, typography, layout)
#     https://developer.apple.com/design/human-interface-guidelines
#
# [5] NOAA. Global Surface Summary of the Day (GSOD). AWS Open Data Registry.
#     (Source dataset — 9000+ weather stations, daily observations)
#     https://registry.opendata.aws/noaa-gsod/
#
# [6] Microsoft. BlobServiceClient class — Azure Storage Blob Python SDK.
#     (Cloud data access via azure-storage-blob)
#     https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient
#
# [7] Google Fonts. Inter. (UI typeface used throughout the dashboard)
#     https://fonts.google.com/specimen/Inter
