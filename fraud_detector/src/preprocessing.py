"""
Препроцессинг транзакций.

Логика полностью повторяет препроцессинг из соревнования (семинар 4),
но всё, что раньше вычислялось по train.csv при каждом старте контейнера
(маппинги категорий, таблицы mean-encoding, средние для импьютации),
заранее сохранено в лёгкий JSON-артефакт artifacts/preproc_artifacts.json.

Благодаря этому сервису не нужен train.csv (сотни МБ) при запуске,
а одна транзакция обрабатывается за миллисекунды.
"""
import json
import logging
import os

import numpy as np
import pandas as pd
from geopy.distance import great_circle

logger = logging.getLogger(__name__)

TARGET_COL = 'target'
CATEGORICAL_COLS = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs']
TIME_COLS = ['hour', 'year', 'month', 'day_of_month', 'day_of_week']
CONTINUOUS_COLS = ['amount', 'population_city', 'distance']
DROP_COLS = ['name_1', 'name_2', 'street', 'post_code']
N_CATS = 50

ARTIFACTS_PATH = os.getenv('PREPROC_ARTIFACTS_PATH', './artifacts/preproc_artifacts.json')
TRAIN_PATH = os.getenv('TRAIN_DATA_PATH', './train_data/train.csv')


# ---------------------------------------------------------------------------
# Общие функции признаков
# ---------------------------------------------------------------------------
def add_time_features(df):
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    dt = df['transaction_time'].dt
    df['hour'] = dt.hour
    df['year'] = dt.year
    df['month'] = dt.month
    df['day_of_month'] = dt.day
    df['day_of_week'] = dt.dayofweek
    return df.drop(columns='transaction_time')


def _distance(row):
    coords = [row['lat'], row['lon'], row['merchant_lat'], row['merchant_lon']]
    if any(pd.isna(c) for c in coords):
        return np.nan  # будет заполнено средним из train
    return great_circle((row['lat'], row['lon']),
                        (row['merchant_lat'], row['merchant_lon'])).km


def add_distance_features(df):
    df['distance'] = df.apply(_distance, axis=1)
    return df.drop(columns=['lat', 'lon', 'merchant_lat', 'merchant_lon'])


def _key(value):
    """Единый строковый ключ для JSON-словарей (int-часы, строки категорий)."""
    if isinstance(value, (float, np.floating)) and float(value).is_integer():
        value = int(value)
    return str(value)


# ---------------------------------------------------------------------------
# Построение артефактов по train.csv (выполняется один раз, офлайн)
# ---------------------------------------------------------------------------
def build_artifacts(train):
    """Считает по сырому train всё, что нужно для inference."""
    train = train.drop(columns=[c for c in DROP_COLS if c in train.columns])
    train = add_time_features(train)

    cat_maps = {}
    for col in CATEGORICAL_COLS:
        new_col = col + '_cat'
        counts = (train.groupby(col, dropna=False)[[TARGET_COL]].count()
                  .sort_values(TARGET_COL, ascending=False)
                  .reset_index().set_axis([col, 'count'], axis=1)
                  .reset_index())
        # Как в исходном коде соревнования: если в колонке есть пропуски,
        # индекс становится float и метки выглядят как 'cat_1.0'
        has_nan = counts[col].isna().any()
        counts[new_col] = [
            'cat_NAN' if pd.isna(val)
            else (f'cat_{float(i) if has_nan else i}' if i < N_CATS else f'cat_{N_CATS}+')
            for i, val in zip(counts['index'], counts[col])
        ]
        cat_maps[col] = {_key(v): c for v, c in zip(counts[col], counts[new_col]) if not pd.isna(v)}
        train = train.merge(counts[[col, new_col]], how='left', on=col)

    train = add_distance_features(train)

    mean_enc = {}
    for col in [c + '_cat' for c in CATEGORICAL_COLS] + TIME_COLS:
        means = train.groupby(col)[TARGET_COL].mean()
        mean_enc[col] = {_key(k): float(v) for k, v in means.items()}

    impute_means = {col: float(train[col].mean()) for col in CONTINUOUS_COLS}

    return {'cat_maps': cat_maps, 'mean_enc': mean_enc, 'impute_means': impute_means}


def load_artifacts():
    if os.path.exists(ARTIFACTS_PATH):
        logger.info('Loading preprocessing artifacts from %s', ARTIFACTS_PATH)
        with open(ARTIFACTS_PATH, encoding='utf-8') as f:
            return json.load(f)

    # Фолбэк: артефакта нет, но есть train.csv — строим на лету
    if os.path.exists(TRAIN_PATH):
        logger.warning('Artifacts not found, building them from %s ...', TRAIN_PATH)
        return build_artifacts(pd.read_csv(TRAIN_PATH))

    raise FileNotFoundError(
        f'Neither {ARTIFACTS_PATH} nor {TRAIN_PATH} found. '
        'Run `python tools/build_artifacts.py` (see README).'
    )


# ---------------------------------------------------------------------------
# Inference-препроцессинг
# ---------------------------------------------------------------------------
def run_preproc(artifacts, input_df):
    df = input_df.drop(columns=[c for c in DROP_COLS if c in input_df.columns]).copy()

    # 1. Кодирование категорий в топ-50 (неизвестные и пропуски -> cat_NAN)
    for col in CATEGORICAL_COLS:
        mapping = artifacts['cat_maps'][col]
        df[col + '_cat'] = [
            'cat_NAN' if pd.isna(v) else mapping.get(_key(v), 'cat_NAN') for v in df[col]
        ]
        df = df.drop(columns=col)

    # 2. Временные признаки
    df = add_time_features(df)

    # 3. Mean-encoding (неизвестное значение -> NaN, как при left merge)
    for col in [c + '_cat' for c in CATEGORICAL_COLS] + TIME_COLS:
        table = artifacts['mean_enc'][col]
        df[col + '_mean_enc'] = [table.get(_key(v), np.nan) for v in df[col]]

    # 4. Расстояние клиент-мерчант
    df = add_distance_features(df)

    # 5. Импьютация средним по train и log-преобразование
    for col in CONTINUOUS_COLS:
        values = pd.to_numeric(df[col], errors='coerce').fillna(artifacts['impute_means'][col])
        df[col + '_log'] = np.log(values + 1)
        df = df.drop(columns=col)

    return df
