"""Visualization module using Abstract Base Classes for alert dashboards."""

import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


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
    return RAG_TIERS[-1]["color"]

def get_risk_label(severity: float) -> str:
    for t in RAG_TIERS:
        if severity >= t["min"]:
            return t["label"]
    return RAG_TIERS[-1]["label"]

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


def ward_sort_key(label: str) -> tuple[int, str]:
    suffix = label.replace("Ward ", "", 1)
    return (int(suffix), label) if suffix.isdigit() else (10**9, label)


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
        org_ids = pd.Series(pd.to_numeric(df["ORG_ID"], errors="coerce"), index=df.index)
        df["ORG_ID"]     = org_ids.fillna(0).astype(int)
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


class PathogenPropagationAcrossWards(Visualization):
    """Separate time-series panels per pathogen showing ward coverage over time."""

    @property
    def filename(self) -> str:
        return "pathogen_propagation_across_wards.html"

    @property
    def description(self) -> str:
        return "Per-pathogen ward coverage over time"

    def _format_ward_list(self, ward_labels: list[str], chunk_size: int = 4) -> str:
        chunks = [ward_labels[i:i + chunk_size] for i in range(0, len(ward_labels), chunk_size)]
        return "<br>".join(", ".join(chunk) for chunk in chunks) if chunks else "None"

    def render(self, df: pd.DataFrame) -> go.Figure:
        df = df.copy().sort_values(["ORG_NAME", "START_TIME", "WARD_ID"])
        organism_counts = df["ORG_NAME"].value_counts().sort_values(ascending=False)
        organisms = organism_counts.index.tolist()

        if not organisms:
            return go.Figure()

        subplot_titles = [f"{organism}" for organism in organisms]
        vertical_spacing = min(0.04, 0.8 / max(len(organisms) - 1, 1))
        fig = make_subplots(
            rows=len(organisms),
            cols=1,
            shared_xaxes=True,
            vertical_spacing=vertical_spacing,
            subplot_titles=subplot_titles,
        )

        max_wards_present = 0

        for row_index, organism in enumerate(organisms, start=1):
            sub = df[df["ORG_NAME"] == organism].copy()
            sub["DATE_BUCKET"] = sub["START_TIME"].dt.floor("D")

            aggregated = (
                sub.groupby("DATE_BUCKET")
                .agg(
                    wards_present=("WARD_LABEL", lambda values: len(sorted(set(values), key=ward_sort_key))),
                    ward_list=("WARD_LABEL", lambda values: ", ".join(sorted(set(values), key=ward_sort_key))),
                    total_alerts=("WARD_LABEL", "size"),
                    max_severity=("SEVERITY", "max"),
                    patients_affected=("NUM_PATIENTS", "sum"),
                )
                .reset_index()
            )

            if aggregated.empty:
                continue

            max_wards_present = max(max_wards_present, int(aggregated["wards_present"].max()))
            unique_wards = sorted(sub["WARD_LABEL"].unique(), key=ward_sort_key)
            annotation_text = self._format_ward_list(unique_wards)

            fig.add_trace(
                go.Scatter(
                    x=aggregated["DATE_BUCKET"],
                    y=aggregated["wards_present"],
                    mode="lines+markers",
                    name=organism,
                    showlegend=False,
                    line=dict(color="#2c3e50", width=3),
                    marker=dict(
                        size=11,
                        color=aggregated["max_severity"],
                        colorscale="YlOrRd",
                        cmin=float(df["SEVERITY"].min()),
                        cmax=float(df["SEVERITY"].max()),
                        line=dict(color="#ffffff", width=1.5),
                    ),
                    customdata=aggregated[["ward_list", "total_alerts", "max_severity", "patients_affected"]].values,
                    hovertemplate=(
                        "Pathogen: <b>" + organism + "</b><br>"
                        "Date: %{x|%Y-%m-%d}<br>"
                        "Wards present: %{y}<br>"
                        "Wards: %{customdata[0]}<br>"
                        "Alerts: %{customdata[1]}<br>"
                        "Max severity: %{customdata[2]}<br>"
                        "Patients affected: %{customdata[3]}<extra></extra>"
                    ),
                ),
                row=row_index,
                col=1,
            )

            fig.add_annotation(
                x=1.01,
                y=0.5,
                xref=f"x domain",
                yref=f"y{row_index if row_index > 1 else ''} domain",
                text=f"<b>Wards</b><br>{annotation_text}",
                showarrow=False,
                align="left",
                bgcolor="rgba(255,255,255,0.92)",
                bordercolor="#d0d7de",
                borderwidth=1,
                font=dict(size=11, color="#2c3e50"),
            )

            fig.update_yaxes(
                title_text="Wards present",
                rangemode="tozero",
                dtick=1,
                row=row_index,
                col=1,
            )

        fig.update_xaxes(
            title_text="Date",
            tickangle=-45,
            tickformat="%d %b %Y",
            type="date",
            automargin=True,
            row=len(organisms),
            col=1,
        )

        fig.update_layout(
            title=dict(
                text="Pathogen Presence Across Wards Over Time<br>"
                     "<sub>Each panel tracks one pathogen by the number of wards where it appears; the right margin lists the wards involved</sub>",
                y=0.99,
                x=0.5,
                xanchor="center",
            ),
            height=max(360 * len(organisms), 700),
            margin=dict(t=120, b=80, l=80, r=260),
            hovermode="x unified",
            **{key: value for key, value in THEME.items() if key != "margin"},
        )

        if max_wards_present:
            fig.update_yaxes(range=[0, max_wards_present + 1])

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


class WardPathogenSpreadHeatmap(Visualization):
    """Heatmap of repeated organism alerts inside each ward over time buckets."""

    @property
    def filename(self) -> str:
        return "ward_pathogen_spread_heatmap.html"

    @property
    def description(self) -> str:
        return "Within-ward pathogen activity over time"

    def render(self, df: pd.DataFrame) -> go.Figure:
        df = df.copy()
        df["PERIOD"] = df["START_TIME"].dt.to_period("W").dt.start_time
        df["WARD_ORGANISM"] = df["WARD_LABEL"] + " | " + df["ORG_NAME"]

        counts = (
            df.groupby(["WARD_ORGANISM", "PERIOD"])
            .size()
            .reset_index(name="ALERT_COUNT")
        )

        ranking = (
            counts.groupby("WARD_ORGANISM")["ALERT_COUNT"]
            .sum()
            .sort_values(ascending=False)
        )
        top_series = ranking.head(15).index.tolist()
        counts = counts[counts["WARD_ORGANISM"].isin(top_series)]

        pivot = counts.pivot(index="WARD_ORGANISM", columns="PERIOD", values="ALERT_COUNT").fillna(0)
        pivot = pivot.reindex(top_series)

        fig = go.Figure(
            data=go.Heatmap(
                z=pivot.values,
                x=pivot.columns,
                y=pivot.index,
                colorscale="YlOrRd",
                colorbar=dict(title="Alerts"),
                hovertemplate=(
                    "Series: <b>%{y}</b><br>"
                    "Week: %{x|%Y-%m-%d}<br>"
                    "Alerts: %{z}<extra></extra>"
                ),
            )
        )

        fig.update_layout(
            title=dict(
                text="Within-Ward Pathogen Activity Over Time<br>"
                     "<sub>Higher intensity means repeated alerts for the same organism in the same ward during a week</sub>",
                y=0.97,
                x=0.5,
                xanchor="center",
            ),
            xaxis=dict(title="Week", tickformat="%d %b %Y", automargin=True),
            yaxis=dict(title="Ward | Organism", automargin=True),
            height=750,
            **THEME,
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
            PathogenPropagationAcrossWards(self.output_dir),
            SeverityComposition(self.output_dir),
            WardPathogenSpreadHeatmap(self.output_dir),
        ]

    def build_all(self) -> None:
        for viz in self._get_visualizations():
            viz.build(self.df)
            print(f" {viz.filename} — {viz.description}")
        print(f"\n All visualizations saved → {self.output_dir.resolve()}")

    def __repr__(self):
        return f"AlertsDashboard(rows={len(self.df)}, output='{self.output_dir}')"