import polars as pl

import jpinfectpy as jp

pl.Config.set_tbl_rows(10)
pl.Config.set_tbl_cols(10)

print("--- SEX PREFECTURE ---")
try:
    df_sex = jp.load("sex", return_type="polars")
    print(df_sex.schema)
    print(df_sex.head())
except Exception as e:
    print(e)

print("\n--- PLACE PREFECTURE ---")
try:
    df_place = jp.load("place", return_type="polars")
    print(df_place.schema)
    print(df_place.head())
except Exception as e:
    print(e)
