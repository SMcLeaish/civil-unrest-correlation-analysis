import os

import polars as pl

from utils import numeric_iso_col, compress, decompress


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

def build_dataset(
    oecd_csv: str,
    acled_csv: str,
    data_csv: str,
    read_data_csv: bool = True,
) -> pl.DataFrame:
    compressed_oecd = f"{oecd_csv}.xz"
    compressed_acled = f"{acled_csv}.xz"
    compressed_data = f"{data_csv}.xz"

    if read_data_csv:
        if os.path.exists(data_csv):
            return pl.read_csv(data_csv)

        if os.path.exists(compressed_data):
            try:
                decompress(compressed_data)
            except Exception as e:
                raise FileNotFoundError(
                    f"Neither {data_csv} nor {compressed_data} could be used"
                ) from e

        if not os.path.exists(data_csv):
            raise FileNotFoundError(
                f"Neither {data_csv} nor {compressed_data} exists"
            )

        return pl.read_csv(data_csv)

    for raw, comp in [(oecd_csv, compressed_oecd), (acled_csv, compressed_acled)]:
        if not os.path.exists(raw):
            if os.path.exists(comp):
                try:
                    decompress(comp) 
                except Exception as e:
                    raise FileNotFoundError(
                        f"Failed to decompress {comp} to {raw}"
                    ) from e
            else:
                raise FileNotFoundError(
                    f"Missing both {raw} and {comp}"
                )

    if not os.path.exists(compressed_oecd):
        compress(oecd_csv)
    if not os.path.exists(compressed_acled):
        compress(acled_csv)

    data = _clean_acled(acled_csv).join(
        _clean_oecd(oecd_csv),
        on=["year_month", "iso"],
        how="left",
    )
    base_cols = ['iso', 'year_month', 'incidents']
    feature_cols = sorted([c for c in data.columns if c not in base_cols])

    data = data.select(base_cols + feature_cols)

    data.write_csv(data_csv)
    if not os.path.exists(compressed_data):
        compress(data_csv)

    return data
