import os
import numpy as np
import polars as pl
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

def build_model(X_train, y_train):
    pipe = Pipeline([
        ('impute', SimpleImputer(strategy='mean')),
        ('scale', StandardScaler()),
        ('model', LinearRegression()),
    ])

    pipe = pipe.fit(X_train, y_train)
    save_model(pipe)
    return pipe


def save_model(model):
    metadata = {
        'model_name': 'Cars Linear Regression',
        'version': '1.0',
        'trained_date': '2025-12-08',
        'algorithm': 'Linear Regression',
        'performance_metrics': {'r2_score': '.73'},
        'training_data': {'size': '398', 'description': 'LinearRegression example'},
        'author': 'Sean',
    }
    joblib.dump({'model': model, 'metadata': metadata}, 'model.pkl')


def import_pipeline(X_train: pl.DataFrame,
                    y_train: pl.Series,
                    file: str)
                    
    if os.path.exists(file):
        pipeline = joblib.load(file)
        return pipeline
    return build_model(X_train, y_train)