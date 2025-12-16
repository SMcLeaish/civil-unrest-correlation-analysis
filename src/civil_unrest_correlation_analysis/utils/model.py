import os

import joblib
import numpy as np
import polars as pl
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def build_pipeline(X_train: pl.DataFrame, # noqa: N803
                   y_train: pl.Series,
                   file: str) -> Pipeline:
    pipe = Pipeline([
        ('impute', SimpleImputer(strategy='mean')),
        ('scale', StandardScaler()),
        ('model', RandomForestRegressor()),
    ])

    pipe = pipe.fit(X_train, y_train)
    save_pipeline(pipe, file)
    return pipe


def save_pipeline(pipeline: Pipeline, file: str):
    metadata = {
        'model_name': 'OECD/ACLED Random Forest Regression',
        'version': '1.0',
        'trained_date': '2025-12-08',
        'algorithm': 'Random Forest Regressor',
        'performance_metrics': {'r2_score': '.84'},
        'training_data': {'size': '398',
                          'description':
                          'Random Forest Regression on OECD/ACLED data'},
        'authors': 'Jacque Sheehan, Sean McLeaish',
    }
    joblib.dump({'pipeline': pipeline, 'metadata': metadata}, file)


def import_pipeline(X_train: pl.DataFrame,  # noqa: N803
                    y_train: pl.Series,
                    file: str) -> Pipeline:
    if os.path.exists(file):
        loaded = joblib.load(file)
        loaded = joblib.load(file)
        if isinstance(loaded, dict) and "pipeline" in loaded:
            return loaded["pipeline"]
    return build_pipeline(X_train, y_train, file)

def build_feature_df(pipe) -> pl.DataFrame | None:
        model = pipe.named_steps['model']
        cols = pipe.named_steps['impute'].get_feature_names_out()
        
        if hasattr(model, "coef_"):
            values = np.transpose(model.coef_)
            name = "coefficient"
        elif hasattr(model, "feature_importances_"):
            values = model.feature_importances_.reshape(-1, 1)
            name = "importance"
        else:
            return None

        return pl.DataFrame({ "feature": cols, name: values.flatten() })
