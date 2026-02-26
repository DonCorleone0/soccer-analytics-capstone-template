import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def load_eda_data(data_dir):
    """Scans parquets using Polars for memory efficiency."""
    matches = pl.scan_parquet(f"../{data_dir}/matches.parquet")
    events = pl.scan_parquet(f"../{data_dir}/events.parquet")
    return matches, events

def get_seasonal_metrics(matches_lf, events_lf, target_team):
    """
    Calculates team vs opponent stats for every match.
    Ensures a clean 1-row-per-match output for the target team.
    """


    match_stats = (
        events_lf.group_by(["match_id", "team"])
        .agg([
            pl.col("type").filter(pl.col("type") == "Pass").count().alias("pass_count"),
            pl.col("shot_statsbomb_xg").sum().fill_null(0).alias("total_xg"),
            # CLIP FIX: Ensure denominator is at least 1
            (
                pl.col("type").filter((pl.col("type") == "Pass") & (pl.col("pass_outcome").is_null())).count() /
                pl.col("type").filter(pl.col("type") == "Pass").count().clip(lower_bound=1)
            ).alias("pass_accuracy")
        ])
    )


    team_matches = matches_lf.filter(
        (pl.col("home_team") == target_team) | (pl.col("away_team") == target_team)
    )




    # (one for the team, one for their opponent).
    joined = team_matches.join(match_stats, on="match_id")



    # We assign 'target' or 'opponent' labels to each row
    processed = (
        joined.with_columns([
            pl.when(pl.col("team") == target_team)
            .then(pl.lit("target"))
            .otherwise(pl.lit("opponent"))
            .alias("role")
        ])
        .collect() # Pivot requires collected data in Polars
        .pivot(
            index=["match_id", "match_date", "home_team", "away_team", "home_score", "away_score"],
            on="role",
            values=["pass_count", "total_xg", "pass_accuracy"]
        )
    )


    # We explicitly calculate the result from the perspective of the target team
    processed = processed.with_columns([
        pl.when((pl.col("home_team") == target_team) & (pl.col("home_score") > pl.col("away_score")))
          .then(pl.lit("Win"))
          .when((pl.col("away_team") == target_team) & (pl.col("away_score") > pl.col("home_score")))
          .then(pl.lit("Win"))
          .when(pl.col("home_score") == pl.col("away_score"))
          .then(pl.lit("Draw"))
          .otherwise(pl.lit("Loss"))
          .alias("result")
    ]).sort("match_date")


    return processed

def plot_comparative_index(df, team_name, metric="total_xg"):
    """
    Creates the Green/Red Seasonal Comparative Index visualization.
    """
    target_col = f"{metric}_target"
    opp_col = f"{metric}_opponent"

    # Calculate difference
    df = df.with_columns([
        (pl.col(target_col) - pl.col(opp_col)).alias("delta")
    ])

    fig = go.Figure()

    # Green for Outperforming, Red for Underperforming
    fig.add_trace(go.Bar(
        x=df["match_date"],
        y=df["delta"],
        marker_color=["#2ecc71" if d > 0 else "#e74c3c" for d in df["delta"]],
        text=df["result"],
        customdata=df["home_team"] + " vs " + df["away_team"],
        hovertemplate="<b>%{customdata}</b><br>Result: %{text}<br>Delta: %{y:.2f}<extra></extra>"
    ))

    fig.update_layout(
        title=f"Seasonal Dominance: {team_name} ({metric})",
        yaxis_title=f"Delta ({metric}) vs Opponent",
        xaxis_title="Match Date",
        template="plotly_white",
        height=450,
        showlegend=False
    )

    return fig

def soap_title(metric, team):
    return f"Seasonal Comparative Index: {team} ({metric})"
