import polars as pl
from utils import numeric_iso_col

def _clean_oecd(file: str) -> pl.DataFrame:
    return numeric_iso_col(pl.read_csv(file), 'REF_AREA').drop_nulls(
    'iso'
    )['iso',
      'TIME_PERIOD',
      'Measure',
      'OBS_VALUE'].with_columns(
    pl.concat_str([pl.col('TIME_PERIOD'),
                   pl.col('iso')],
                   separator='-')
                   .alias('date_iso')
                   ).unique().pivot(
    index=['iso', 'TIME_PERIOD'],
    on='Measure',
    values='OBS_VALUE',
    aggregate_function='first').rename({'TIME_PERIOD': 'year_month'})

def _clean_acled(file: str) -> pl.DataFrame:
    return pl.read_csv(file,
                        schema_overrides={'iso': pl.String}).with_columns(
    pl.col('event_date')
    .str.to_date()
    .dt.strftime("%Y-%m")
    .alias('year_month')).select(['year_month',
                                         'iso']).group_by(['year_month',
                               'iso']).len().rename({'len':'incidents'})

def build_dataset(oecd_csv: str, acled_csv: str) -> pl.DataFrame:
    return _clean_acled(acled_csv).join(
    _clean_oecd(oecd_csv), on=['year_month', 'iso'], how='left')