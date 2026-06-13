"""Module to read and parse the Environment Canada Station Inventory CSV."""

from pathlib import Path

import pandas as pd

from _column_config import clean_column_names


def read_station_inventory(csv_path: str | Path = "data/Station Inventory EN.csv") -> pd.DataFrame:
    """
    Read the Environment Canada Station Inventory CSV and return a pandas DataFrame.

    The CSV has a "Modified Date:" line followed by two disclaimer rows before the
    header row. This function skips those rows automatically and cleans up column names.

    Parameters
    ----------
    csv_path : str | Path
        Path to the Station Inventory EN.csv file. Defaults to "data/Station Inventory EN.csv".

    Returns
    -------
    pd.DataFrame
        DataFrame containing station inventory data with proper column types.
    """
    df = pd.read_csv(
        csv_path,
        skiprows=3,          # Skip the "Modified Date:" line and two disclaimer lines
        dtype={
            "Climate ID": str,
            "Station ID": "Int64",
            "WMO ID": str,
            "TC ID": str,
            "Elevation (m)": float,
            "First Year": "Int64",
            "Last Year": "Int64",
            "HLY First Year": "Int64",
            "HLY Last Year": "Int64",
            "DLY First Year": "Int64",
            "DLY Last Year": "Int64",
            "MLY First Year": "Int64",
            "MLY Last Year": "Int64",
        },
    )

    # Strip whitespace from string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Clean column names: lowercase, spaces/hyphens → underscores, remove other special chars
    df = clean_column_names(df)

    return df
