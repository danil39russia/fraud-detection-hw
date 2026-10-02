"""Загрузка модели и скоринг подготовленных признаков (только CPU)."""
import logging
import os

import pandas as pd
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

MODEL_PATH = os.getenv('MODEL_PATH', './models/my_catboost.cbm')
MODEL_THRESHOLD = float(os.getenv('MODEL_THRESHOLD', '0.98'))

logger.info('Loading pretrained model from %s ...', MODEL_PATH)
model = CatBoostClassifier(task_type='CPU')
model.load_model(MODEL_PATH)
FEATURES = model.feature_names_
CAT_FEATURES = [FEATURES[i] for i in model.get_cat_feature_indices()]
logger.info('Model loaded: %d features, threshold=%.2f', len(FEATURES), MODEL_THRESHOLD)


def make_pred(df: pd.DataFrame) -> pd.DataFrame:
    """Возвращает DataFrame с колонками score и fraud_flag."""
    df = df[FEATURES].copy()            # порядок признаков как при обучении
    for col in CAT_FEATURES:
        df[col] = df[col].astype(str)   # CatBoost ждёт категории строками

    scores = model.predict_proba(df)[:, 1]
    return pd.DataFrame({
        'score': scores.astype(float),
        'fraud_flag': (scores > MODEL_THRESHOLD).astype(int),
    })
