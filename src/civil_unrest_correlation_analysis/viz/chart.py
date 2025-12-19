import copy
from typing import Any
import altair as alt
import geopandas as gpd
import polars as pl
from geoacled.chart import Choropleth
from geoacled.geojson import build_geo_df
from shapely import Point
from sklearn.pipeline import Pipeline


def pop_toplevel_blocks(spec: dict) -> tuple[dict, dict, dict]:
    """Return (clean_spec, datasets, config)."""
    s = copy.deepcopy(spec)
    datasets = s.pop("datasets", {}) or {}
    config = s.pop("config", {}) or {}
    s.pop("$schema", None)
    return s, datasets, config


def prediction_line_chart(df: pl.DataFrame, pipe: Pipeline) -> alt.Chart:
    feature_cols = list(pipe.feature_names_in_)

    X = df.select(feature_cols).to_pandas()

    y_pred = pipe.predict(X)

    results = (
        df.with_columns(
            pl.Series("predicted", y_pred)
        )
        .with_columns(
            pl.col("year_month")
            .str.strptime(pl.Date, format="%Y-%m")
            .cast(pl.Datetime)
            .alias("year_month")
        )
        .sort("year_month")
    )

    plot_df = (
        results.select(
            "year_month",
            pl.col("incidents").alias("Actual"),
            pl.col("predicted").alias("Predicted"),
        )
        .to_pandas()
        .melt(
            id_vars="year_month",
            var_name="Series",
            value_name="Incidents",
        )
    )

    line_chart = (
        alt.Chart(plot_df)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("year_month:T", title="Month"),
            y=alt.Y("Incidents:Q", title="Incidents", scale=alt.Scale(zero=True)),
            color=alt.Color(
                "Series:N",
                scale=alt.Scale(
                    domain=["Actual", "Predicted"],
                    range=["#1f77b4", "#ff7f0e"],
                ),
                legend=alt.Legend(title=""),
            ),
            tooltip=[
                alt.Tooltip("year_month:T", title="Month"),
                alt.Tooltip("Series:N"),
                alt.Tooltip("Incidents:Q"),
            ],
        )
        .properties(
            title="Actual vs Predicted Incidents",
            width=700,
            height=300,
        )
    )

    return line_chart

def choropleth(geojson: dict[str, Any],
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
    title='',
    lookup_df=incident_count_df,
    lookup_column="shapeName",
    geo_df=geo_df,
    geojson_id="shapeName",
    basemap_color_column='Number of incidents',
    basemap_color_scheme='reds',
    basemap_tooltips={'shapeName': 'Region',
                      'Number of incidents': 'Number of incidents'}
)
def concat_chart(line: alt.Chart,
                 choropleth: alt.LayerChart) -> alt.VConcatChart:

    map_spec, map_datasets, map_config = pop_toplevel_blocks(choropleth.chart.to_dict())
    line_spec, line_datasets, line_config = pop_toplevel_blocks(line.to_dict())

    datasets = {}
    datasets.update(map_datasets)
    datasets.update(line_datasets)

    config = {}
    config.update(map_config)
    config.update(line_config)

    font_stack = 'Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif'
    cfg = dict(config) 
    cfg["font"] = font_stack

    cfg.setdefault("title", {})
    cfg["title"]["font"] = font_stack

    cfg.setdefault("axis", {})
    cfg["axis"]["labelFont"] = font_stack
    cfg["axis"]["titleFont"] = font_stack

    cfg.setdefault("legend", {})
    cfg["legend"]["labelFont"] = font_stack
    cfg["legend"]["titleFont"] = font_stack
    
    combined_spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
        "datasets": datasets if datasets else None,
        "config": cfg if cfg else None,
        "vconcat": [map_spec, line_spec],
    }

    combined_spec = {k: v for k, v in combined_spec.items() if v is not None}

    combined_chart = alt.VConcatChart.from_dict(combined_spec)
    return(combined_chart)