"""This takes the log.txt file and creates visualization from it. """
# visualization/visualization.py
import sqlite3
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path

DB_PATH = "/Volumes/T7/OOP_SM_MO_CN-final_branch/Final_ver_9_march/OOP_database.db"

# ─── RAG System ───────────────────────────────────────────────────────────────

def severity_to_rag(severity: float) -> str:
    if severity >= 30:   return "#e74c3c"
    elif severity >= 15: return "#f39c12"
    else:                return "#27ae60"

def get_risk_label(s: float) -> str:
    if s >= 30:   return "HIGH"
    elif s >= 15: return "MEDIUM"
    else:         return "LOW"

THEME = dict(
    template="plotly_white",
    font=dict(family="Arial", size=13),
    title_font=dict(size=16, color="#2c3e50"),
    margin=dict(t=100, b=180, l=70, r=200),
    hoverlabel=dict(bgcolor="white", font_size=12, bordercolor="#ccc")
)

# ─── Data Loader ──────────────────────────────────────────────────────────────

class AlertsLoader:

    def __init__(self, db_path: str = DB_PATH):
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

    def filter_recent(self, days: int) -> pd.DataFrame:
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        return self.data[self.data["START_TIME"] >= cutoff]

    def __len__(self):
        return len(self._df) if self._df is not None else 0

    def __repr__(self):
        return f"AlertsLoader(db='{self.db_path}', loaded={self._df is not None}, rows={len(self)})"


# ─── Dashboard ────────────────────────────────────────────────────────────────

class AlertsDashboard:

    def __init__(self, df: pd.DataFrame, output_dir: str = "./viz_output/"):
        self.df = df
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def _save(self, fig, name: str) -> None:
        fig.write_html(
            self.output_dir / name,
            include_plotlyjs="cdn",
            config={"displayModeBar": True, "scrollZoom": True}
        )

    # ── Alerts Over Time ──────────────────────────────────────────────────────
    def alerts_over_time(self) -> "AlertsDashboard":
        df = self.df.copy().sort_values("START_TIME")
        df["CUMULATIVE"] = df.groupby("WARD_LABEL").cumcount() + 1

        wards = sorted(df["WARD_LABEL"].unique())
        n_wards = len(wards)

        grey_values = [
            int(200 - i * (140 / max(n_wards - 1, 1)))
            for i in range(n_wards)
        ]
        grey_shades = [f"rgb({v},{v},{v})" for v in grey_values]

        fig = go.Figure()

        # ── One line per ward ──────────────────────────────────────────────
        for i, ward in enumerate(wards):
            sub = df[df["WARD_LABEL"] == ward].sort_values("START_TIME")
            line_color = grey_shades[i]

            fig.add_trace(go.Scatter(
                x=sub["START_TIME"],
                y=sub["CUMULATIVE"],
                mode="lines+markers",
                name=ward,
                legendgroup="wards",
                legendgrouptitle=dict(text="Wards") if i == 0 else None,
                line=dict(color=line_color, width=2),
                marker=dict(
                    color=sub["RAG_COLOR"].tolist(),
                    size=11,
                    symbol="circle",
                    line=dict(width=2, color=line_color)
                ),
                customdata=sub[["ORG_NAME", "SEVERITY", "RISK", "NUM_PATIENTS"]].values,
                hovertemplate=(
                    f"<b>{ward}</b><br>"
                    "📅 Date: %{x|%Y-%m-%d}<br>"
                    "📊 Total Alerts: %{y}<br>"
                    "🦠 Organism: <b>%{customdata[0]}</b><br>"
                    "⚠ Severity: %{customdata[1]} — %{customdata[2]}<br>"
                    "🧑‍⚕️ Patients: %{customdata[3]}"
                    "<extra></extra>"
                )
            ))

        # ── RAG dummy traces for severity legend ──────────────────────────
        for label, color in [
            ("HIGH  (severity ≥ 30)",   "#e74c3c"),
            ("MEDIUM (severity 15–29)", "#f39c12"),
            ("LOW   (severity < 15)",   "#27ae60"),
        ]:
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode="markers",
                marker=dict(size=11, color=color, symbol="circle"),
                name=label,
                legendgroup="severity",
                legendgrouptitle=dict(text="Marker = Alert Severity") if label.startswith("HIGH") else None,
                showlegend=True
            ))

        # ── Layout ────────────────────────────────────────────────────────
        fig.update_layout(
            title=dict(
                text=(
                    "📈 Ward Alert Activity Over Time<br>"
                    "<sub>Grey lines = wards (click to hide/show)  ·  "
                    "Marker fill = RAG severity  ·  "
                    "Hover for organism  ·  "
                    "Drag slider or use buttons to zoom</sub>"
                ),
                y=0.97,
                x=0.5,
                xanchor="center"
            ),
            xaxis=dict(
                title=dict(text="Date", standoff=25),
                tickangle=-45,              # ← Tilted date labels on main axis
                tickformat="%d %b %Y",      # ← "19 Mar 2026" format
                tickfont=dict(size=11),
                rangeselector=dict(
                    buttons=[
                        dict(count=7,  step="day", stepmode="backward", label="7 days"),
                        dict(count=14, step="day", stepmode="backward", label="14 days"),
                        dict(count=30, step="day", stepmode="backward", label="30 days"),
                        dict(step="all", label="All time")
                    ],
                    bgcolor="#ecf0f1",
                    activecolor="#3498db",
                    y=1.02                  # Push buttons above the plot
                ),
                rangeslider=dict(
                    visible=True,
                    thickness=0.03,         # ← Very thin: just a date bar, no points
                    bgcolor="#f7f7f7",
                    bordercolor="#ddd",
                    borderwidth=1,
                ),
                type="date",
                automargin=True
            ),
            yaxis=dict(
                title="Cumulative Number of Alerts",
                rangemode="tozero",
                automargin=True
            ),
            legend=dict(
                groupclick="toggleitem",
                orientation="v",
                x=1.02,
                y=1.0,
                xanchor="left",
                yanchor="top",
                bgcolor="rgba(255,255,255,0.9)",
                bordercolor="#ccc",
                borderwidth=1
            ),
            height=650,
            **THEME
        )

        self._save(fig, "ward_alerts_over_time.html")
        return self

    # ── Build All ─────────────────────────────────────────────────────────────
    def build_all(self) -> None:
        self.alerts_over_time()
        print(f"\n✅ Visualization saved → {self.output_dir.resolve()}")
        print("   ward_alerts_over_time.html")
        print("   → Grey lines = wards | Marker fill = RAG severity | Hover for organism")

    def __repr__(self):
        return f"AlertsDashboard(rows={len(self.df)}, output='{self.output_dir}')"
