import lzma
import shutil
from pathlib import Path

import polars as pl
import pycountry
from geoacled import AcledYear

def _get_numeric_iso(alpha_3:str) -> int | None:
    norm = alpha_3.strip().strip('"').strip("'").upper()
    country = pycountry.countries.get(alpha_3=norm)
    if country:
        return str(country.numeric)
    return None

def numeric_iso_col(df: pl.DataFrame, col: 'str') -> pl.DataFrame:
    return df.with_columns(pl.col(col)
                         .map_elements(
                             _get_numeric_iso, return_dtype=pl.String
                             )
                             .alias('iso'))

def fetch_acled_for_countries(df:pl.DataFrame) -> pl.DataFrame:
    isos = df['iso'].unique().to_list()
    periods = df['TIME_PERIOD'].unique().to_list()
    years = []
    dfs = []
    for period in periods:
        years.append(period.split('-')[0])
    years_set = set(years)
    for iso in isos:
        for year in years_set:
            year_df = AcledYear(iso=iso, year=year).df
            if year_df.width > 0 and year_df.height > 0:
                dfs.append(year_df)
    return pl.concat(dfs)

def compress(path: str | Path) -> Path:
    path = Path(path)
    out_path = path.with_suffix(path.suffix + '.xz')

    with path.open('rb') as src, lzma.open(out_path, 'wb', preset=9) as dst:
        shutil.copyfileobj(src, dst)
        return out_path

def decompress(path: str | Path) -> Path:
    path = Path(path)

    if not path.suffix == '.xz':
        raise ValueError(f'File does not end with .xz {path}')

    out_path = path.with_suffix('')

    with lzma.open(path, 'rb') as src, out_path.open('wb') as dst:
        shutil.copyfileobj(src,dst)
    
    return out_path