import pandas as pd

# Define rules for value ranges and required fields for each metric
# This is a central place to manage your data quality standards.
VALIDATION_RULES = {
    "weather-data__air-temperature": {"min": -60, "max": 60, "required": True},
    "weather-data__relative-humidity": {"min": 0, "max": 100, "required": True},
    "weather-data__avg-wind-speed": {"min": 0, "max": 200},
    "surface-data__surface-temperature": {"min": -60, "max": 60},
    "surface-data__surface-condition-code": {"required": True},
    "obs_iso8601": {"required": True},
    "lat": {"required": True},
    "lon": {"required": True}
}

def validate_data(df, rules):
    """
    Applies a set of validation rules to the DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame to validate.
        rules (dict): A dictionary of validation rules.

    Returns:
        tuple: A tuple containing two DataFrames: one with valid data and
               one with invalid (quarantined) data.
    """
    if df.empty:
        return df, df

    # --- 1. Drop all rows with any null values for complete cleaning ---
    # This addresses the "all columns" and "value column should not be null or empty" requirements.
    # It also handles non-numeric/empty values in 'value' by coercing them to NaN first.
    initial_clean_df = df.replace('', pd.NA).dropna(how='any').copy()
    
    # Separate the dropped rows to be quarantined
    quarantine_df = df[~df.index.isin(initial_clean_df.index)].copy()

    # --- 2. Filter out-of-range outliers based on defined rules ---
    valid_rows_mask = pd.Series([True] * len(initial_clean_df), index=initial_clean_df.index)
    
    for index, row in initial_clean_df.iterrows():
        metric = row.get('metric_full')
        value_str = row.get('value')
        
        # Coerce value to numeric, handling non-numeric cases
        value = pd.to_numeric(value_str, errors='coerce')
        
        if metric in rules:
            rule = rules[metric]
            if "min" in rule and "max" in rule:
                # Check if the value is a valid number before comparing
                if not pd.isna(value):
                    if not (rule["min"] <= value <= rule["max"]):
                        valid_rows_mask.loc[index] = False
                else:
                    # If a metric should be numeric but is not, it's an outlier
                    valid_rows_mask.loc[index] = False

    valid_df = initial_clean_df[valid_rows_mask].reset_index(drop=True)
    invalid_range_df = initial_clean_df[~valid_rows_mask].reset_index(drop=True)

    quarantine_df = pd.concat([quarantine_df, invalid_range_df], ignore_index=True)

    return valid_df, quarantine_df