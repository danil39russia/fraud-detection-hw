"""
Сервис записи результатов: читает топик `scores`
(transaction_id, score, fraud_flag) и складывает записи в Postgres.
"""
import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('score_writer')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
SCORES_TOPIC = os.getenv('KAFKA_SCORES_TOPIC', 'scores')
PG_DSN = dict(
    host=os.getenv('POSTGRES_HOST', 'postgres'),
    port=int(os.getenv('POSTGRES_PORT', '5432')),
    dbname=os.getenv('POSTGRES_DB', 'fraud'),
    user=os.getenv('POSTGRES_USER', 'fraud'),
    password=os.getenv('POSTGRES_PASSWORD', 'fraud'),
)
BATCH_SIZE = 100

INSERT_SQL = """
    INSERT INTO fraud_scores (transaction_id, score, fraud_flag)
    VALUES (%s, %s, %s)
    ON CONFLICT (transaction_id) DO UPDATE
        SET score = EXCLUDED.score, fraud_flag = EXCLUDED.fraud_flag
"""


def connect_pg(retries=30):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**PG_DSN)
            logger.info('Connected to Postgres')
            return conn
        except psycopg2.OperationalError as e:
            logger.warning('Postgres not ready (%d/%d): %s', attempt, retries, e)
            time.sleep(2)
    raise RuntimeError('Could not connect to Postgres')


def parse(raw: bytes):
    """Поддерживает и одиночный объект, и список записей (формат семинара)."""
    payload = json.loads(raw.decode('utf-8'))
    records = payload if isinstance(payload, list) else [payload]
    return [(str(r['transaction_id']), float(r['score']), int(r['fraud_flag'])) for r in records]


def main():
    conn = connect_pg()
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'score-writer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,   # коммитим оффсеты только после записи в БД
    })
    consumer.subscribe([SCORES_TOPIC])
    logger.info('Listening topic "%s"', SCORES_TOPIC)

    total = 0
    try:
        while True:
            msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            rows = []
            for msg in msgs:
                if msg.error():
                    logger.error('Kafka error: %s', msg.error())
                    continue
                try:
                    rows.extend(parse(msg.value()))
                except Exception as e:
                    logger.error('Bad message skipped: %s', e)
            if not msgs:
                continue
            try:
                if rows:
                    with conn.cursor() as cur:
                        cur.executemany(INSERT_SQL, rows)
                    conn.commit()
                consumer.commit(asynchronous=False)
                total += len(rows)
                logger.info('Saved %d rows (total %d)', len(rows), total)
            except psycopg2.Error as e:
                # Оффсеты не закоммичены: контейнер перезапустится (restart policy)
                # и перечитает этот батч заново — данные не потеряются.
                logger.error('DB error: %s', e)
                raise
    finally:
        consumer.close()
        conn.close()


if __name__ == '__main__':
    main()
