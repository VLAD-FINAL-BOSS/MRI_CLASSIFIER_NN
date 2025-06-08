from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, VARCHAR, TIMESTAMP, Float
from Shmutkin.Lesson3.config import DATABASE_URL
import json
import pika
from datetime import datetime, timezone


Base = declarative_base()
# Создаю модель ORM-модель таблицы
class StockPrice(Base):
    __tablename__ = "sber_current_price"
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    stock_name = Column(VARCHAR(10), nullable=False, default='SBER')
    price = Column(Float, nullable=False)
    date_save = Column(TIMESTAMP, nullable=False, index=True)

engine = create_engine(DATABASE_URL)
Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session = SessionLocal()


def callback(ch, method, properties, body):
    print(body)
    data = json.loads(body)
    session = SessionLocal()

    try:
        for ticker, price in data.items():
            new_record = StockPrice(
                stock_name=ticker,
                price=data[ticker],
                date_save=datetime.now(timezone.utc)
            )

            session.add(new_record)
            session.commit()
            print(f"Данные записаны!")

    except Exception as e:
        session.rollback()
        print(f"Ошибка: {e}")
    finally:
        session.close()


# Подключение к RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
channel = connection.channel()
channel.basic_consume(queue='API_SBER', on_message_callback=callback, auto_ack=True)

print('Ожидание сообщений...')
channel.start_consuming()  # Бесконечный цикл
