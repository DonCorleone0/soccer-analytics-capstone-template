import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path
import warnings
import json
import os


# --- Load all parquet files ---
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data" / "Statsbomb"

def load_data():
  if not DATA_DIR.exists():
      print(f"Error: The directory {DATA_DIR} was not found.")
  else:
      matches     = pd.read_parquet(DATA_DIR / "matches.parquet")
      events      = pd.read_parquet(DATA_DIR / "events.parquet")
      lineups     = pd.read_parquet(DATA_DIR / "lineups.parquet")
      three_sixty = pd.read_parquet(DATA_DIR / "three_sixty.parquet")
      reference   = pd.read_parquet(DATA_DIR / "reference.parquet")

  return matches, events, lineups, three_sixty, reference

def check(matches, events, lineups, three_sixty, reference):
      print("=" * 60)
      print("DATA LOADED SUCCESSFULLY")
      print("=" * 60)
      print(f"{'Table':<20} {'Rows':>12} {'Columns':>10} {'Memory (MB)':>14}")
      print("-" * 60)
      for name, df in [("matches", matches), ("events", events),
                        ("lineups", lineups), ("three_sixty", three_sixty),
                        ("reference", reference)]:
          mem = df.memory_usage(deep=True).sum() / 1024**2
          print(f"{name:<20} {len(df):>12,} {len(df.columns):>10} {mem:>14.2f}")
      print("=" * 60)