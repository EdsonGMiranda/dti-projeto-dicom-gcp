import glob
import os
import gc
import json
import time
from datetime import datetime

import pandas as pd
import numpy as np


def delete_flex_field_cols(df):
    flex_field_cols = [col for col in df.columns if "FL_FIELD" in col]
    return df.drop(flex_field_cols, axis=1)


def mascarar_coluna_obs(df, coluna, mascara='***'):
    if coluna in df.columns:
        df[coluna] = df[coluna].apply(lambda x: mascara if pd.notna(x) else x)
    else:
        print(f"A coluna '{coluna}' não existe no DataFrame.")
    return df


def upper_columns_values(df, coluna):
    if coluna in df.columns:
        df[coluna] = df[coluna].apply(lambda x: x.upper() if pd.notna(x) and isinstance(x, str) else x)
    else:
        print(f"A coluna '{coluna}' não existe no DataFrame.")
    return df


