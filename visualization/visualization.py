"""Visualization module using Abstract Base Classes for alert dashboards."""

import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go


# rag colors and labels based on severity thresholds

RAG_TIERS = [
    {"label": "HIGH",   "min": 30, "color": "#e74c3c"},
    {"label": "MEDIUM", "min": 15, "color": "#f39c12"},
    {"label": "LOW",    "min": 0,  "color": "#27ae60"},
]

def severity_to_rag(severity: float) -> str:
    for t in RAG_TIERS:
        if severity >= t["min"]:
            return t["color"]

def get_risk_label(severity: float) -> str:
    for t in RAG_TIERS:
        if severity >= t["min"]:
            return t["label"]

THEME = dict(
    template="plotly_white",
    font=dict(family="Arial", size=13),
    title_font=dict(size=16, color="#2c3e50"),
    margin=dict(t=100, b=180, l=70, r=200),
    hoverlabel=dict(bgcolor="white", font_size=12, bordercolor="#ccc"),
)

LEGEND_DEFAULTS = dict(
    orientation="v", x=1.02, y=1.0, xanchor="left", yanchor="top",
    bgcolor="rgba(255,255,255,0.9)", bordercolor="#ccc", borderwidth=1,
)


# data loading and processing

class AlertsLoader:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._df = None

    def load(self) -> "AlertsLoader":
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM MICROALERTS", conn)
        conn.close()

        df["START_TIME"] = pd.to_datetime(df["START_TIME"])
        df["SEVERITY"]   = pd.to_numeric(df["SEVERITY"], errors="coerce")
        df["ORG_ID"]     = pd.to_numeric(df["ORG_ID"], errors="coerce").fillna(0).astype(int)
        df["WARD_LABEL"] = "Ward " + df["WARD_ID"].astype(str)
        df["RISK"]       = df["SEVERITY"].apply(get_risk_label)
        df["RAG_COLOR"]  = df["SEVERITY"].apply(severity_to_rag)

        self._df = df.sort_values("START_TIME").reset_index(drop=True)
        return self

    @property
    def data(self) -> pd.DataFrame:
        if self._df is None:
            raise RuntimeError("Call .load() before accessing .data")
        return self._df

    def __repr__(self):
        return f"AlertsLoader(db='{self.db_path}', rows={len(self._df) if self._df is not None else 0})"


# abstract visualization base class

class Visualization(ABC):
    """Abstract base class for all alert visualizations."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir

    @abstractmethod
    def render(self, df: pd.DataFrame) -> go.Figure: ...
    """Subclasses must implement render() to create a Plotly figure from the data."""

    @property
    @abstractmethod
    def filename(self) -> str: ...

    @property
    @abstractmethod
    def description(self) -> str: ...

    def save(self, fig: go.Figure) -> None:
        fig.write_html(
            self.output_dir / self.filename,
            include_plotlyjs="cdn",
            config={"displayModeBar": True, "scrollZoom": True},
        )

    def build(self, df: pd.DataFrame) -> None:
        fig = self.render(df)
        self.save(fig)


# visualization implementations

class CumulativeAlertsOverTime(Visualization):
    """Cumulative alert lines per ward with RAG-colored markers."""

    @property
    def filename(self) -> str:
        return "ward_alerts_over_time.html"

    @property
    def description(self) -> str:
        return "Cumulative alerts per ward | RAG severity markers"

    def render(self, df: pd.DataFrame) -> go.Figure:
        df = df.copy().sort_values("START_TIME")
        df["CUMULATIVE"] = df.groupby("WARD_LABEL").cumcount() + 1

        wards = sorted(df["WARD_LABEL"].unique())
        n = len(wards)
        greys = [f"rgb({int(200 - i * 140 / max(n-1, 1))},"
                 f"{int(200 - i * 140 / max(n-1, 1))},"
                 f"{int(200 - i * 140 / max(n-1, 1))})" for i in range(n)]

        fig = go.Figure()

        for i, ward in enumerate(wards):
            sub = df[df["WARD_LABEL"] == ward]
            fig.add_trace(go.Scatter(
                x=sub["START_TIME"], y=sub["CUMULATIVE"],
                mode="lines+markers", name=ward,
                legendgroup="wards",
                legendgrouptitle=dict(text="Wards") if i == 0 else None,
                line=dict(color=greys[i], width=2),
                marker=dict(color=sub["RAG_COLOR"].tolist(), size=11,
                            line=dict(width=2, color=greys[i])),
                customdata=sub[["ORG_NAME", "SEVERITY", "RISK", "NUM_PATIENTS"]].values,
                hovertemplate=(
                    f"<b>{ward}</b><br>Date: %{{x|%Y-%m-%d}}<br>"
                    "Alerts: %{y}<br>Organism: <b>%{customdata[0]}</b><br>"
                    "Severity: %{customdata[1]} — %{customdata[2]}<br>"
                    "Patients: %{customdata[3]}<extra></extra>"
                ),
            ))

        for t in RAG_TIERS:
            fig.add_trace(go.Scatter(
                x=[None], y=[None], mode="markers",
                marker=dict(size=11, color=t["color"]),
                name=f"{t['label']} (>= {t['min']})",
                legendgroup="severity",
                legendgrouptitle=dict(text="Severity") if t is RAG_TIERS[0] else None,
                showlegend=True,
            ))

        fig.update_layout(
            title=dict(text="Ward Alert Activity Over Time<br>"
                       "<sub>Grey lines = wards | Marker = RAG severity | Hover for detail</sub>",
                       y=0.97, x=0.5, xanchor="center"),
            xaxis=dict(title="Date", tickangle=-45, tickformat="%d %b %Y",
                       rangeselector=dict(buttons=[
                           dict(count=7,  step="day", stepmode="backward", label="7d"),
                           dict(count=14, step="day", stepmode="backward", label="14d"),
                           dict(count=30, step="day", stepmode="backward", label="30d"),
                           dict(step="all", label="All")],
                           bgcolor="#ecf0f1", activecolor="#3498db", y=1.02),
                       rangeslider=dict(visible=True, thickness=0.03),
                       type="date", automargin=True),
            yaxis=dict(title="Cumulative Alerts", rangemode="tozero", automargin=True),
            legend=dict(groupclick="toggleitem", **LEGEND_DEFAULTS),
            height=650, **THEME,
        )
        return fig


class SeverityComposition(Visualization):
    """Stacked bar chart of alert counts per severity tier, grouped by ward."""
 
    @property
    def filename(self) -> str:
        return "severity_composition.html"
 
    @property
    def description(self) -> str:
        return "Alert count by severity tier per ward"
 
    def _classify(self, severity: float) -> str:
        for t in RAG_TIERS:
            if severity >= t["min"]:
                return t["label"]
        return RAG_TIERS[-1]["label"]
 
    def render(self, df: pd.DataFrame) -> go.Figure:
        df = df.copy()
        df["TIER"] = df["SEVERITY"].apply(self._classify)
 
        # Low to High order for stacking
        tier_labels = [t["label"] for t in reversed(RAG_TIERS)]
        tier_colors = {t["label"]: t["color"] for t in RAG_TIERS}
 
        counts = df.groupby(["WARD_ID", "TIER"]).size().unstack(fill_value=0)
        for t in tier_labels:
            if t not in counts.columns:
                counts[t] = 0
        counts = counts[tier_labels]
 
        ward_labels = [f"Ward {w}" for w in counts.index]
        fig = go.Figure()
 
        for tier_name in tier_labels:
            fig.add_trace(go.Bar(
                x=ward_labels, y=counts[tier_name].values,
                name=tier_name, marker_color=tier_colors[tier_name],
                hovertemplate=f"<b>%{{x}}</b><br>{tier_name}: %{{y}} alerts<extra></extra>",
            ))
 
        fig.update_layout(
            barmode="stack",
            title=dict(text="Alert Severity Composition by Ward<br>"
                       "<sub>Stacked by severity tier</sub>",
                       y=0.97, x=0.5, xanchor="center"),
            xaxis=dict(title="Ward", automargin=True),
            yaxis=dict(title="Number of Alerts", rangemode="tozero", automargin=True),
            legend=dict(title="Severity Tier", **LEGEND_DEFAULTS),
            height=550, **THEME,
        )
        return fig


# dashboard to orchestrate visualizations polymorphically

class AlertsDashboard:

    def __init__(self, df: pd.DataFrame, output_dir: str = "./viz_output/"):
        self.df = df
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def _get_visualizations(self) -> list[Visualization]:
        return [
            CumulativeAlertsOverTime(self.output_dir),
            SeverityComposition(self.output_dir),
        ]

    def build_all(self) -> None:
        for viz in self._get_visualizations():
            viz.build(self.df)
            print(f"  ✅ {viz.filename} — {viz.description}")
        print(f"\n✅ All visualizations saved → {self.output_dir.resolve()}")

    def __repr__(self):
        return f"AlertsDashboard(rows={len(self.df)}, output='{self.output_dir}')"