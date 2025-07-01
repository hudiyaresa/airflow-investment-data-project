import pandas as pd
import json

def profile_and_quality(df: pd.DataFrame, table_name) -> pd.DataFrame:
    data_profile = {}
    data_quality = {}

    for col in df.columns:
        series = df[col]

        # Profiling
        data_type = str(series.dtype)
        sample_data = series.dropna().astype(str).unique()[:5].tolist()

        # Quality
        not_null_count = series.notnull().sum()
        completeness = (not_null_count / n_rows * 100) if n_rows > 0 else 0.0
        quality = "Good" if completeness > 90 else "Bad"

        # Only for numeric
        is_numeric = pd.api.types.is_numeric_dtype(series)
        is_negative_values = bool((series < 0).sum()) if is_numeric else False

        data_profile[col] = {
            "data_type": data_type,
            "sample_data": sample_data
        }

        data_quality[col] = {
            "percentage_completeness": round(completeness, 2),
            "data_quality_completeness_result": quality,
            "is_negative_values": is_negative_values
        }

    result_df = pd.DataFrame([{
        "schema": "-",
        "table_name": table_name,
        "n_rows": len(df),
        "n_cols": len(df.columns),
        "data_profile": json.dumps(data_profile),
        "data_quality": json.dumps(data_quality)
    }])

    return result_df