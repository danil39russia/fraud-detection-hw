import json
import os
import uuid

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st
from kafka import KafkaProducer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
PG_DSN = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "fraud"),
    user=os.getenv("POSTGRES_USER", "fraud"),
    password=os.getenv("POSTGRES_PASSWORD", "fraud"),
)

st.set_page_config(page_title="Fraud Detection", page_icon="🕵️", layout="wide")


# ----------------------------------------------------------------- Kafka
@st.cache_resource
def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )


def send_to_kafka(df: pd.DataFrame) -> int:
    producer = get_producer()
    df = df.reset_index(drop=True)
    df = df.astype(object).where(pd.notna(df), None)   # NaN -> null в JSON
    progress = st.progress(0.0)
    total = len(df)
    for i, row in enumerate(df.to_dict("records")):
        producer.send(KAFKA_TOPIC, value={"transaction_id": str(uuid.uuid4()), "data": row})
        if i % 50 == 0 or i == total - 1:
            progress.progress((i + 1) / total)
    producer.flush()
    return total


# -------------------------------------------------------------- Postgres
def query(sql: str) -> pd.DataFrame:
    with psycopg2.connect(**PG_DSN) as conn:
        return pd.read_sql_query(sql, conn)


LAST_FRAUD_SQL = """
    SELECT transaction_id, score, fraud_flag, created_at
    FROM fraud_scores
    WHERE fraud_flag = 1
    ORDER BY created_at DESC, id DESC
    LIMIT 10
"""
LAST_100_SQL = """
    SELECT score FROM fraud_scores
    ORDER BY created_at DESC, id DESC
    LIMIT 100
"""


# -------------------------------------------------------------------- UI
st.title("🕵️ Real-time Fraud Detection")
tab_send, tab_results = st.tabs(["📤 Отправка транзакций", "📊 Результаты скоринга"])

with tab_send:
    uploaded = st.file_uploader("Загрузите CSV файл с транзакциями (формат test.csv)", type=["csv"])
    if uploaded is not None:
        try:
            df = pd.read_csv(uploaded)
        except Exception as e:
            st.error(f"Ошибка чтения файла: {e}")
            df = None
        if df is not None:
            st.caption(f"Файл `{uploaded.name}`: {len(df)} строк")
            st.dataframe(df.head(), use_container_width=True)
            n = st.number_input("Сколько транзакций отправить", 1, len(df), min(len(df), 500))
            if st.button("Отправить в Kafka", type="primary"):
                try:
                    with st.spinner("Отправка..."):
                        sent = send_to_kafka(df.head(int(n)))
                    st.success(f"Отправлено {sent} транзакций в топик `{KAFKA_TOPIC}`")
                except Exception as e:
                    st.error(f"Ошибка отправки: {e}")

with tab_results:
    if st.button("Посмотреть результаты", type="primary"):
        try:
            fraud_df = query(LAST_FRAUD_SQL)
            scores_df = query(LAST_100_SQL)
        except Exception as e:
            st.error(f"Не удалось получить данные из Postgres: {e}")
        else:
            st.subheader("10 последних транзакций с fraud_flag = 1")
            if fraud_df.empty:
                st.info("Мошеннических транзакций пока нет")
            else:
                st.dataframe(fraud_df, use_container_width=True, hide_index=True)

            st.subheader(f"Распределение скоров последних {len(scores_df)} транзакций")
            if scores_df.empty:
                st.info("В базе пока нет транзакций")
            else:
                chart = (
                    alt.Chart(scores_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("score:Q", bin=alt.Bin(maxbins=20, extent=[0, 1]), title="score"),
                        y=alt.Y("count():Q", title="Количество транзакций"),
                    )
                    .properties(height=350)
                )
                st.altair_chart(chart, use_container_width=True)
