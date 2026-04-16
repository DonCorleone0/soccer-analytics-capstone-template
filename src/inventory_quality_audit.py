import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path
import warnings
import json



def profile_dataframe(df, name):
    """Generate a comprehensive profile of a dataframe."""
    profile = pd.DataFrame({
        'dtype': df.dtypes,
        'non_null_count': df.count(),
        'null_count': df.isnull().sum(),
        'null_pct': (df.isnull().sum() / len(df) * 100).round(2),
        'n_unique': df.nunique(),
        'sample_value': df.iloc[0] if len(df) > 0 else None
    })

    return profile

def show_profiles(profile, df, name):
    print(f"\n{'='*100}")
    print(f"PROFILE: {name.upper()} ({len(df):,} rows x {len(df.columns)} cols)")
    print(f"{'='*100}")
    print(profile.to_string())

