from contextlib import asynccontextmanager
from typing import Any

import polars as pl
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

from civil_unrest_correlation_analysis.schema import (
    CountryMeta,
    SnapshotResponse,
)
from civil_unrest_correlation_analysis.utils.building import (
    build_countries_dict,
    build_dataset,
    build_geojson_dict,
    build_snapshot,
    clean_acled,
    clean_oecd,
    raw_acled,
)
from civil_unrest_correlation_analysis.utils.model import import_pipeline

OECD_CSV = 'data/final/oecd.csv'
ACLED_CSV = 'data/final/acled.csv'
DATA_CSV = 'data/final/data.csv'
MODEL_PKL = 'random_forest.pkl'
DATAFRAMES: dict[str, pl.DataFrame] = {}
MODELS: dict[str, Pipeline] = {}
COUNTRIES: dict[str, CountryMeta] = {}
COUNTRIES_GEO: dict[str, Any] = {}
LIFESPAN_OBJS: list[dict[str,Any]] = []
ORIGINS = [
    'http://localhost',
    'http://localhost:5173',
    'http://127.0.0.1',
    'http://127.0.0.1:5173']

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
    data = DATAFRAMES['data']
    X = data.drop(['iso', 'year_month', 'incidents'])  # noqa: N806
    y = data['incidents']
    X_train, _, y_train, _ = train_test_split(X, y, random_state=42)  # noqa: N806
    MODELS['pipe'] = import_pipeline(X_train, y_train, MODEL_PKL)
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
    data = DATAFRAMES['data']
    pipe = MODELS['pipe']
    return build_snapshot(countries_geo=COUNTRIES_GEO,
                          acled_df=acled,
                          pipe=pipe,
                          data=data,
                          iso=iso,
                          start=start,
                          end=end)
