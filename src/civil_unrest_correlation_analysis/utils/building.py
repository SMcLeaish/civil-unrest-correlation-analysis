import os

import polars as pl

from civil_unrest_correlation_analysis.schema import AcledEvent, CountryMeta
from civil_unrest_correlation_analysis.utils.cleaning import (
    clean_acled,
    clean_oecd,
)
from civil_unrest_correlation_analysis.utils.compression import (
    check_file_compression,
    compress,
    decompress,
)


def build_acled_events(
    df: pl.DataFrame,
    iso: str,
    start: str,
    end: str
    ) -> list[AcledEvent]:
    return [AcledEvent(**row) for row in df.with_columns(
      pl.col('event_date')
      .str.to_date()
      .dt.strftime("%Y-%m")
      .alias('year_month')).filter(
        (pl.col("iso") == iso)
        & (pl.col("year_month") >= start)
        & (pl.col("year_month") <= end)
    ).select(
        [
            "year_month",
            "event_date",
            "admin1",
            "location",
            "event_type",
            "sub_event_type",
            "fatalities",
            "notes",
        ]
    ).to_dicts()]

def build_countries_dict(acled_df: pl.DataFrame) -> dict[str, CountryMeta]:
    countries = {}
    for row in acled_df.select(['iso', 'country']).unique().iter_rows(named=True):
        countries[row['iso']] = CountryMeta(
            iso=row['iso'],
            name=row['country'],
        )
    return countries

def raw_acled(filepath: str) -> pl.DataFrame:
    file = check_file_compression(filepath)
    return pl.read_csv(file, schema_overrides={'iso': pl.String})

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
    
    data = clean_acled(acled_csv).join(
        clean_oecd(oecd_csv),
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
