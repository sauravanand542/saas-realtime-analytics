"""SaaS revenue dashboard on the published dbt marts.

Run from the repo root:  streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
from data import load_marts  # noqa: E402

BG, CARD, GRID, TEXT, MUTED = "#0b1020", "#131b31", "#22304f", "#e5e9f5", "#8b97b8"
TEAL, INDIGO, GREEN, RED, AMBER, SKY, PINK = (
    "#2dd4bf", "#818cf8", "#34d399", "#f87171", "#fbbf24", "#38bdf8", "#f472b6",
)
PLAN_COLORS = {"enterprise": INDIGO, "growth": TEAL, "starter": SKY, "trial": MUTED}
MOVE_COLORS = {"new": GREEN, "expansion": TEAL, "reactivation": SKY,
               "contraction": AMBER, "churn": RED}

st.set_page_config(page_title="SaaS Revenue Dashboard", page_icon="📈", layout="wide")

st.markdown(
    f"""
    <style>
    header[data-testid="stHeader"], footer, #MainMenu, [data-testid="stToolbar"],
    [data-testid="stDecoration"], [data-testid="stStatusWidget"] {{ display: none !important; }}
    .block-container {{ padding: 1.6rem 3rem 3rem 3rem; max-width: 1840px; }}
    .stApp {{ background: radial-gradient(1200px 600px at 10% -10%, #17224a 0%, {BG} 55%); }}
    h1, h2, h3 {{ letter-spacing: -0.01em; }}
    .dash-title {{ font-size: 2.1rem; font-weight: 700; color: {TEXT}; margin: 0; }}
    .dash-sub {{ color: {MUTED}; font-size: 1rem; margin-top: .2rem; }}
    .pill {{ display:inline-block; padding: .18rem .7rem; border-radius: 999px;
             background: rgba(45,212,191,.12); color: {TEAL}; font-size: .85rem;
             border: 1px solid rgba(45,212,191,.35); margin-left: .6rem; }}
    .kpi {{ background: linear-gradient(180deg, #16203b 0%, {CARD} 100%);
            border: 1px solid {GRID}; border-radius: 16px; padding: 1.05rem 1.2rem;
            height: 138px; box-shadow: 0 8px 24px rgba(0,0,0,.25); }}
    .kpi {{ transition: transform .25s ease, border-color .25s ease, box-shadow .25s ease; }}
    .kpi:hover {{ transform: translateY(-3px); border-color: {TEAL};
                  box-shadow: 0 10px 30px rgba(45,212,191,.18); }}
    .kpi .label {{ color: {MUTED}; font-size: .86rem; text-transform: uppercase;
                   letter-spacing: .06em; }}
    .kpi .value {{ color: {TEXT}; font-size: 2.15rem; font-weight: 700; margin-top: .25rem; }}
    .kpi .delta {{ font-size: .95rem; margin-top: .15rem; }}
    .up {{ color: {GREEN}; }} .down {{ color: {RED}; }} .flat {{ color: {MUTED}; }}
    .section {{ color: {TEXT}; font-size: 1.25rem; font-weight: 650; margin: 1.6rem 0 .1rem 0; }}
    .section-sub {{ color: {MUTED}; font-size: .92rem; margin-bottom: .4rem; }}
    [data-testid="stPlotlyChart"] {{ background: {CARD}; border: 1px solid {GRID};
            border-radius: 16px; padding: .4rem .6rem; }}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=3600, show_spinner="Loading marts…")
def get_data():
    return load_marts()


frames, target = get_data()
mrr = frames["mrr"].copy()
orgs = frames["orgs"].copy()
logins = frames["logins"].copy()
changes = frames["changes"].copy()

mrr["month_start"] = pd.to_datetime(mrr["month_start"])
mrr["label"] = mrr["month_start"].dt.strftime("%b %Y")
for col in [c for c in mrr.columns if c.endswith("_cents")]:
    mrr[col.replace("_cents", "")] = mrr[col] / 100
cur, prev = mrr.iloc[-1], mrr.iloc[-2]


def money(v: float, signed: bool = False) -> str:
    sign = ("+" if v > 0 else "−" if v < 0 else "") if signed else ("−" if v < 0 else "")
    return f"{sign}${abs(v):,.0f}"


def pct(rate: float) -> str:
    value = Decimal(str(rate * 100)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    return f"{value}%"


def base_layout(fig: go.Figure, height: int, **kw) -> go.Figure:
    titled = "title" in kw
    fig.update_layout(
        height=height, paper_bgcolor=CARD, plot_bgcolor=CARD,
        font=dict(color=TEXT, size=14), margin=dict(l=20, r=20, t=60, b=20),
        hoverlabel=dict(bgcolor="#1d2744", bordercolor=GRID, font=dict(color=TEXT, size=15)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    x=1 if titled else 0, xanchor="right" if titled else "left",
                    bgcolor="rgba(0,0,0,0)"),
        **kw,
    )
    fig.update_xaxes(gridcolor=GRID, zeroline=False, linecolor=GRID)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, linecolor=GRID)
    return fig


built = pd.to_datetime(mrr["_dbt_built_at"]).max()
source = "Snowflake · ANALYTICS.MARTS" if target == "snowflake" else "DuckDB · marts"
st.markdown(
    f"""<div class="dash-title">SaaS Revenue &amp; Engagement
    <span class="pill">{source}</span></div>
    <div class="dash-sub">Simulated B2B SaaS · {len(mrr)} months of history ·
    marts published {built:%b %d, %Y %H:%M} UTC after dbt tests passed</div>""",
    unsafe_allow_html=True,
)
st.write("")

# ---------------- KPI tiles ----------------
mrr_change = cur["ending_mrr"] - prev["ending_mrr"]
mrr_pct = mrr_change / prev["ending_mrr"] * 100 if prev["ending_mrr"] else 0
cust_change = int(cur["active_orgs_end"] - prev["active_orgs_end"])
churn_pts = (cur["logo_churn_rate"] - prev["logo_churn_rate"]) * 100


def kpi(col, label, value, delta, cls):
    col.markdown(
        f"""<div class="kpi"><div class="label">{label}</div>
        <div class="value">{value}</div><div class="delta {cls}">{delta}</div></div>""",
        unsafe_allow_html=True,
    )


def cls_for(v, good_up=True):
    if v == 0:
        return "flat"
    return "up" if (v > 0) == good_up else "down"


month = cur["label"]
cols = st.columns(6, gap="medium")
kpi(cols[0], f"Current MRR · {month}", money(cur["ending_mrr"]),
    f"vs {prev['label']}: {money(prev['ending_mrr'])}", "flat")
kpi(cols[1], "MRR change (MoM)", money(mrr_change, signed=True),
    f"{mrr_pct:+.1f}% month over month", cls_for(mrr_change))
kpi(cols[2], "Active customers", f"{int(cur['active_orgs_end'])}",
    f"{cust_change:+d} vs last month", cls_for(cust_change))
kpi(cols[3], "Logo churn rate", pct(cur["logo_churn_rate"]),
    f"{churn_pts:+.1f} pts vs last month · {int(cur['churned_orgs'])} lost",
    cls_for(churn_pts, good_up=False))
kpi(cols[4], "New MRR", money(cur["new_mrr"]),
    f"{int(cur['new_orgs'])} new customers", "up" if cur["new_mrr"] > 0 else "flat")
kpi(cols[5], "Expansion MRR", money(cur["expansion_mrr"]),
    "upgrades from existing customers", "up" if cur["expansion_mrr"] > 0 else "flat")

# ---------------- MRR trend ----------------
st.markdown('<div class="section" id="sec-trend">MRR trend</div>'
            '<div class="section-sub">Ending MRR each month, with paying customers</div>',
            unsafe_allow_html=True)
fig = go.Figure()
fig.add_trace(go.Bar(x=mrr["label"], y=mrr["active_orgs_end"], name="Active customers",
                     marker_color="rgba(129,140,248,.35)", yaxis="y2",
                     hovertemplate="%{y} customers<extra></extra>"))
fig.add_trace(go.Scatter(x=mrr["label"], y=mrr["ending_mrr"], name="Ending MRR",
                         mode="lines+markers", line=dict(color=TEAL, width=4, shape="spline"),
                         marker=dict(size=8), fill="tozeroy", fillcolor="rgba(45,212,191,.12)",
                         hovertemplate="MRR $%{y:,.0f}<extra></extra>"))
base_layout(fig, 420, hovermode="x unified",
            yaxis=dict(title="MRR ($)", tickprefix="$", tickformat=",.0f"),
            yaxis2=dict(title="Customers", overlaying="y", side="right", showgrid=False,
                        rangemode="tozero", dtick=10))
st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

# ---------------- MRR bridge ----------------
st.markdown('<div class="section" id="sec-bridge">MRR bridge</div>'
            '<div class="section-sub">What moved MRR each month: gains above zero, '
            'losses below</div>', unsafe_allow_html=True)
left, right = st.columns([1.7, 1], gap="medium")
fig = go.Figure()
for col, name, color in [("new_mrr", "New", GREEN), ("expansion_mrr", "Expansion", TEAL),
                         ("reactivation_mrr", "Reactivation", SKY),
                         ("contraction_mrr", "Contraction", AMBER), ("churn_mrr", "Churn", RED)]:
    fig.add_trace(go.Bar(x=mrr["label"], y=mrr[col], name=name, marker_color=color,
                         hovertemplate=f"{name} $%{{y:,.0f}}<extra></extra>"))
net = mrr["ending_mrr"] - mrr["beginning_mrr"]
fig.add_trace(go.Scatter(x=mrr["label"], y=net, name="Net change", mode="lines+markers",
                         line=dict(color=TEXT, width=2, dash="dot"),
                         hovertemplate="Net $%{y:,.0f}<extra></extra>"))
base_layout(fig, 460, barmode="relative", hovermode="x unified",
            yaxis=dict(tickprefix="$", tickformat=",.0f"))
left.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

wf = go.Figure(go.Waterfall(
    x=["Start", "New", "Expansion", "Reactivation", "Contraction", "Churn", "End"],
    measure=["absolute", "relative", "relative", "relative", "relative", "relative", "total"],
    y=[cur["beginning_mrr"], cur["new_mrr"], cur["expansion_mrr"], cur["reactivation_mrr"],
       cur["contraction_mrr"], cur["churn_mrr"], cur["ending_mrr"]],
    text=[money(cur["beginning_mrr"]), money(cur["new_mrr"], True),
          money(cur["expansion_mrr"], True), money(cur["reactivation_mrr"], True),
          money(cur["contraction_mrr"], True), money(cur["churn_mrr"], True),
          money(cur["ending_mrr"])],
    textposition="outside", connector=dict(line=dict(color=GRID)),
    increasing=dict(marker=dict(color=GREEN)), decreasing=dict(marker=dict(color=RED)),
    totals=dict(marker=dict(color=INDIGO)),
    hovertemplate="%{x}: %{text}<extra></extra>",
))
base_layout(wf, 460, hovermode="x",
            title=dict(text=f"{month} waterfall", x=0.02, font=dict(size=16)),
            yaxis=dict(tickprefix="$", tickformat=",.0f",
                       range=[0, max(cur["beginning_mrr"], cur["ending_mrr"]) * 1.38]),
            showlegend=False)
right.plotly_chart(wf, width="stretch", config={"displayModeBar": False})

# ---------------- By plan ----------------
st.markdown('<div class="section" id="sec-plan">Customers and MRR by plan</div>'
            '<div class="section-sub">Current state from the organization dimension</div>',
            unsafe_allow_html=True)
plan = (orgs.assign(mrr=orgs["mrr_cents"] / 100)
        .groupby("plan").agg(mrr=("mrr", "sum"), orgs=("org_id", "count"),
                             paying=("is_paying", "sum"), active_users=("active_user_count", "sum"))
        .reindex(["enterprise", "growth", "starter", "trial"]).dropna().reset_index())
plan["arpa"] = plan["mrr"] / plan["paying"].where(plan["paying"] > 0)
c1, c2 = st.columns(2, gap="medium")
fig = go.Figure(go.Pie(labels=plan["plan"], values=plan["mrr"], hole=.62,
                       marker=dict(colors=[PLAN_COLORS[p] for p in plan["plan"]],
                                   line=dict(color=CARD, width=3)),
                       textinfo="label+percent", textfont=dict(size=15),
                       hovertemplate="%{label}: $%{value:,.0f} MRR<extra></extra>", sort=False))
base_layout(fig, 420, title=dict(text="MRR share by plan", x=0.02, font=dict(size=16)),
            showlegend=False,
            annotations=[dict(text=f"<b>{money(plan['mrr'].sum())}</b><br>MRR", showarrow=False,
                              font=dict(size=22, color=TEXT))])
c1.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
fig = go.Figure()
fig.add_trace(go.Bar(x=plan["plan"], y=plan["paying"], name="Paying customers",
                     marker_color=[PLAN_COLORS[p] for p in plan["plan"]],
                     text=plan["paying"].astype(int), textposition="outside",
                     customdata=plan[["mrr", "arpa"]].fillna(0).values,
                     hovertemplate="%{x}: %{y} paying<br>MRR $%{customdata[0]:,.0f}"
                                   "<br>Avg per customer $%{customdata[1]:,.0f}<extra></extra>"))
fig.add_trace(go.Bar(x=plan["plan"], y=plan["orgs"], name="All organizations",
                     marker_color="rgba(139,151,184,.25)", text=plan["orgs"].astype(int),
                     textposition="outside",
                     hovertemplate="%{x}: %{y} organizations<extra></extra>"))
base_layout(fig, 420, title=dict(text="Customers by plan", x=0.02, font=dict(size=16)),
            barmode="group", hovermode="x unified",
            yaxis=dict(range=[0, plan["orgs"].max() * 1.25]))
c2.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

# ---------------- Logins ----------------
st.markdown('<div class="section" id="sec-logins">Daily login activity</div>'
            '<div class="section-sub">SSO logins and unique users per day, '
            'from the incremental login fact</div>', unsafe_allow_html=True)
logins["activity_date"] = pd.to_datetime(logins["activity_date"])
fig = go.Figure()
fig.add_trace(go.Bar(x=logins["activity_date"], y=logins["logins"], name="Logins",
                     marker_color="rgba(56,189,248,.45)",
                     hovertemplate="%{y} logins<extra></extra>"))
fig.add_trace(go.Scatter(x=logins["activity_date"], y=logins["successful_logins"],
                         name="Successful", mode="lines", line=dict(color=GREEN, width=3),
                         hovertemplate="%{y} successful<extra></extra>"))
fig.add_trace(go.Scatter(x=logins["activity_date"], y=logins["active_users"],
                         name="Unique users", mode="lines",
                         line=dict(color=PINK, width=3, dash="dot"),
                         hovertemplate="%{y} unique users<extra></extra>"))
base_layout(fig, 400, hovermode="x unified", xaxis=dict(tickformat="%b %d"))
st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

# ---------------- Plan changes ----------------
st.markdown('<div class="section" id="sec-changes">Plan changes</div>'
            '<div class="section-sub">Every subscription event classified as a revenue '
            'movement</div>', unsafe_allow_html=True)
moves = changes[changes["movement_type"].isin(MOVE_COLORS)].copy()
moves["occurred_month"] = pd.to_datetime(moves["occurred_month"])
moves["mrr_delta"] = moves["mrr_delta_cents"] / 100
c1, c2 = st.columns([1.5, 1], gap="medium")
counts = (moves.groupby([moves["occurred_month"].dt.strftime("%Y-%m"), "movement_type"])
          .size().unstack(fill_value=0).sort_index())
fig = go.Figure()
for m in ["new", "expansion", "reactivation", "contraction", "churn"]:
    if m in counts:
        fig.add_trace(go.Bar(x=pd.to_datetime(counts.index).strftime("%b %Y"), y=counts[m],
                             name=m.capitalize(), marker_color=MOVE_COLORS[m],
                             hovertemplate=f"{m.capitalize()}: %{{y}}<extra></extra>"))
base_layout(fig, 440, barmode="stack", hovermode="x unified",
            title=dict(text="Movements per month", x=0.02, font=dict(size=16)))
c1.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
summary = (moves.groupby("movement_type").agg(events=("mrr_delta", "size"),
                                               mrr=("mrr_delta", "sum"))
           .reindex(["new", "expansion", "reactivation", "contraction", "churn"]).dropna())
fig = go.Figure(go.Bar(
    y=[m.capitalize() for m in summary.index], x=summary["mrr"], orientation="h",
    marker_color=[MOVE_COLORS[m] for m in summary.index],
    text=[f"{money(v, True)} · {int(n)} events"
          for v, n in zip(summary["mrr"], summary["events"], strict=True)],
    textposition="outside", hovertemplate="%{y}: %{text}<extra></extra>"))
span = summary["mrr"].abs().max() * 2.2
base_layout(fig, 440, hovermode="y",
            title=dict(text="All-time MRR by movement", x=0.02, font=dict(size=16)),
            xaxis=dict(tickprefix="$", tickformat=",.0f", range=[-span, span]),
            yaxis=dict(autorange="reversed"), showlegend=False)
c2.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

recent = moves.sort_values("occurred_at", ascending=False).head(8)
table = pd.DataFrame({
    "When": pd.to_datetime(recent["occurred_at"]).dt.strftime("%b %d, %Y"),
    "Customer": recent["org_name"],
    "Movement": recent["movement_type"].str.capitalize(),
    "From → to": [
        f"{f or '—'} → canceled" if m == "churn" else f"{f or '—'} → {t or '—'}"
        for f, t, m in zip(recent["from_plan"], recent["to_plan"], recent["movement_type"],
                           strict=True)
    ],
    "MRR change": recent["mrr_delta"].map(lambda v: money(v, True)),
})
st.markdown('<div class="section-sub" style="margin-top:1rem">Most recent plan changes</div>',
            unsafe_allow_html=True)
st.dataframe(table, hide_index=True, width="stretch")
st.markdown(f'<div class="dash-sub" style="margin-top:1.2rem">Source: dbt marts '
            f'fct_mrr_monthly, dim_organizations, fct_login_activity_daily, '
            f'fct_plan_changes on {source.split(" ·")[0]}.</div>', unsafe_allow_html=True)
