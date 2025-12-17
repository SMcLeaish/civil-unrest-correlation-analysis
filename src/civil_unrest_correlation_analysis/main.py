from contextlib import asynccontextmanager
from typing import Any

import polars as pl
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from civil_unrest_correlation_analysis.utils.building import (
    build_acled_events_dict,
    build_countries_dict,
    build_dataset,
    clean_acled,
    clean_oecd,
    raw_acled,
    build_geojson_dict
)
from civil_unrest_correlation_analysis.schema import (
    AcledEvent,
    BaseModel,
    CountryMeta,
    OecdMetric,
    SnapshotResponse,
)

OECD_CSV = 'data/final/oecd.csv'
ACLED_CSV = 'data/final/acled.csv'
DATA_CSV = 'data/final/data.csv'
DATAFRAMES: dict[str, pl.DataFrame] = {}
COUNTRIES: dict[str, CountryMeta] = {}
COUNTRIES_GEO: dict[str, Any] = {}
LIFESPAN_OBJS: list[dict[str,Any]] = []
ORIGINS = ['http://localhost:5173']

@asynccontextmanager
async def lifespan(app: FastAPI):
    DATAFRAMES['raw_acled'] = raw_acled(ACLED_CSV)
    DATAFRAMES['clean_acled'] = clean_acled(ACLED_CSV)
    DATAFRAMES['oecd'] = clean_oecd(OECD_CSV)
    DATAFRAMES['data'] = build_dataset(acled_csv=ACLED_CSV,
                                       oecd_csv=DATA_CSV,
                                       data_csv=DATA_CSV)
    LIFESPAN_OBJS.append(DATAFRAMES)
    if DATAFRAMES.get('raw_acled') is not None:
        COUNTRIES.update(build_countries_dict(DATAFRAMES['raw_acled']))
        COUNTRIES_GEO.update(build_geojson_dict(COUNTRIES))
        LIFESPAN_OBJS.append(COUNTRIES)
    yield
    for obj in LIFESPAN_OBJS:
        obj.clear()

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
@app.get('/countries', response_model=list[CountryMeta])
async def list_countries() -> list[CountryMeta]:
    if not COUNTRIES:
        raise HTTPException(status_code=500, detail='Countries not loaded')
    return sorted(COUNTRIES.values(), key=lambda c: c.name)

@app.get('/snapshot', response_model=SnapshotResponse)
async def snapshot(
    iso: str = Query(...,
                     min_length=3,
                     max_length=3,
                     description='Numeric ISO code'),
    start: str = Query(...,
                       regex=r"^\d{4}-\d{2}$"),
    end: str = Query(...,
                     regex=r"^\d{4}-\d{2}$")
) -> SnapshotResponse:
    acled = DATAFRAMES['raw_acled']
    if acled is None:
        raise HTTPException(status_code=500, detail='Data not loaded')
    country_meta = COUNTRIES.get(iso)
    if country_meta is None:
        raise HTTPException(status_code=404, detail=f'Unknown ISO {iso}')
    #BREAK OUT INTO FUNCTION
    acled_slice = acled.with_columns(
      pl.col('event_date')
      .str.to_date()
      .dt.strftime("%Y-%m")
      .alias('year_month')).filter(
        (pl.col("iso") == iso)
        & (pl.col("year_month") >= start)
        & (pl.col("year_month") <= end)
    )

    acled_events = build_acled_events_dict(acled_slice, iso, start, end)
    return SnapshotResponse(
        iso=iso,
        country=country_meta.name,
        start=start,
        end=end,
        acled_events=acled_events,
    )
