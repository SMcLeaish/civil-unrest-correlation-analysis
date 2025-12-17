import os
from typing import Any

import geopandas as gpd
import polars as pl
import pycountry
from geoacled.chart import Choropleth
from geoacled.geojson import build_geo_df, get_region_list
from geoacled.utils.clean import clean_column, clean_set_to_dataframe
from geoacled.utils.fetch import fetch_geojson
from shapely import Point

from civil_unrest_correlation_analysis.schema import (
    AcledEvent,
    CountryMeta,
    OecdMetric,
    SnapshotResponse,
)
from civil_unrest_correlation_analysis.utils.cleaning import (
    clean_acled,
    clean_oecd,
)
from civil_unrest_correlation_analysis.utils.compression import (
    check_file_compression,
    compress,
    decompress,
)


def build_filtered_acled_events(
    df: pl.DataFrame,
    iso: str,
    start: str,
    end: str
    ) -> pl.DataFrame:
    return df.with_columns(
      pl.col('event_date')
      .str.to_date()
      .dt.strftime("%Y-%m")
      .alias('year_month')).filter(
        (pl.col("iso") == iso)
        & (pl.col("year_month") >= start)
        & (pl.col("year_month") <= end)
    ).select(
        [
            "iso",
            "year_month",
            "event_date",
            "admin1",
            "location",
            "event_type",
            "sub_event_type",
            "fatalities",
            "notes",
        ]
    )

def build_acled_events_dict(acled_df: pl.DataFrame, iso: str, start: str, end: str) -> list[AcledEvent]:
    filtered = build_filtered_acled_events(acled_df, iso, start, end)
    return [AcledEvent(**row) for row in filtered.to_dicts()]

def build_geojson_dict(countries: dict[str, CountryMeta]):
    geo_dict = {}
    for iso in countries:
        country = pycountry.countries.get(numeric=iso)
        geo, _ = fetch_geojson(country.name, adm='ADM1')
        geo_dict[iso] = geo
    return geo_dict
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

def build_choropleth(geojson: dict[str, Any],
                     acled_df: pl.DataFrame,
                     country: str,
                     start: str,
                     end: str) -> Choropleth:
    geo_df = build_geo_df(geojson)
    pdf = acled_df.to_pandas()

    geometry = [
        Point(lon, lat)
        for lon, lat in zip(pdf["longitude"], pdf["latitude"])
    ]

    points_gdf = gpd.GeoDataFrame(
        pdf,
        geometry=geometry,
        crs="EPSG:4326",
    )
    geo_df = geo_df.to_crs(points_gdf.crs)

    joined = gpd.sjoin(
        points_gdf,
        geo_df,
        how="left",
        predicate="within",
    )
    counts = (
        joined
        .groupby("shapeName")
        .size()
        .reset_index(name="incident_count")
    )        
    incident_count_df = pl.from_pandas(counts)
    incident_count_df = incident_count_df.rename({'incident_count': 'Number of incidents'})
    return Choropleth(
    title=f'Incidents of Civil Unrest in {country}: {start} to {end}',
    lookup_df=incident_count_df,
    lookup_column="shapeName",
    geo_df=geo_df,
    geojson_id="shapeName",
    basemap_color_column='Number of incidents',
    basemap_color_scheme='reds',
    basemap_tooltips={'shapeName': 'Region',
                      'Number of incidents': 'Number of incidents'}
)

def build_snapshot(countries_geo, acled_df, iso, start, end, adm) -> SnapshotResponse:
    country= pycountry.countries.get(numeric=iso).name
    incident_count_df = joined_df.group_by(
        'shapeName').len().rename({'len': 'incident_count'}
        )
    map = Choropleth(lookup_df=incident_count_df,
                     lookup_column='shapeName',
                     geo_df=build_geo_df(geojson),
                     geojson_id='shapeName')
    return SnapshotResponse(
        iso=iso,
        country=country,
        start=start,
        end=end,
        acled_events=acled_dict,
        map_spec={'map': map}
        )

