"""
Одноразовая офлайн-подготовка артефактов препроцессинга по train.csv.

Запуск из папки fraud_detector:
    python tools/build_artifacts.py --train train_data/train.csv
Результат: artifacts/preproc_artifacts.json (его нужно закоммитить).
"""
import argparse
import json
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from preprocessing import build_artifacts  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument('--train', default='train_data/train.csv')
parser.add_argument('--out', default='artifacts/preproc_artifacts.json')
args = parser.parse_args()

print(f'Reading {args.train} ...')
artifacts = build_artifacts(pd.read_csv(args.train))
os.makedirs(os.path.dirname(args.out), exist_ok=True)
with open(args.out, 'w', encoding='utf-8') as f:
    json.dump(artifacts, f, ensure_ascii=False, indent=1)
print(f'Saved {args.out} ({os.path.getsize(args.out) / 1024:.1f} KB)')
