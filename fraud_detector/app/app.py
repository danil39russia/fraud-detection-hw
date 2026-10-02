"""
Kafka-сервис скоринга: читает транзакции из топика `transactions`,
прогоняет препроцессинг и модель, пишет результат в топик `scores`.
"""
import json
import logging
import os
import sys

import pandas as pd
from confluent_kafka import Consumer, Producer

sys.path.append(os.path.abspath('./src'))
from preprocessing import load_artifacts, run_preproc  # noqa: E402
from scorer import make_pred  # noqa: E402

os.makedirs('/app/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('/app/logs/service.log'), logging.StreamHandler()],
)
logger = logging.getLogger('fraud_detector')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TRANSACTIONS_TOPIC = os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions')
SCORES_TOPIC = os.getenv('KAFKA_SCORES_TOPIC', 'scores')


class ProcessingService:
    def __init__(self):
        self.artifacts = load_artifacts()
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
        })
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        self.processed = 0

    def handle(self, raw: bytes):
        data = json.loads(raw.decode('utf-8'))
        transaction_id = data['transaction_id']
        input_df = pd.DataFrame([data['data']])

        features = run_preproc(self.artifacts, input_df)
        result = make_pred(features).iloc[0]

        message = {
            'transaction_id': transaction_id,
            'score': float(result['score']),
            'fraud_flag': int(result['fraud_flag']),
        }
        self.producer.produce(SCORES_TOPIC, key=transaction_id, value=json.dumps(message))
        self.producer.poll(0)

        self.processed += 1
        if self.processed % 100 == 0:
            logger.info('Processed %d transactions', self.processed)

    def run(self):
        logger.info('Listening topic "%s" -> writing to "%s"', TRANSACTIONS_TOPIC, SCORES_TOPIC)
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    self.producer.flush()
                    continue
                if msg.error():
                    logger.error('Kafka error: %s', msg.error())
                    continue
                try:
                    self.handle(msg.value())
                except Exception as e:  # битое сообщение не должно ронять сервис
                    logger.exception('Error processing message: %s', e)
        finally:
            self.producer.flush()
            self.consumer.close()


if __name__ == '__main__':
    logger.info('Starting Kafka ML scoring service...')
    ProcessingService().run()
