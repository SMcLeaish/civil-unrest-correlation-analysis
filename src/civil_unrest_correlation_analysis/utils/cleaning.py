import polars as pl
from civil_unrest_correlation_analysis.utils.compression import check_file_compression
from civil_unrest_correlation_analysis.utils.misc import numeric_iso_col

def clean_oecd(filepath: str) -> pl.DataFrame:
    file = check_file_compression(filepath)
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

def clean_acled(filepath: str) -> pl.DataFrame:
    file = check_file_compression(filepath)
    return pl.read_csv(file, schema_overrides={'iso': pl.String}).with_columns(
    pl.col('event_date')
    .str.to_date()
    .dt.strftime("%Y-%m")
    .alias('year_month')).select(['year_month',
                                         'iso']).group_by(['year_month',
                               'iso']).len().rename({'len':'incidents'})