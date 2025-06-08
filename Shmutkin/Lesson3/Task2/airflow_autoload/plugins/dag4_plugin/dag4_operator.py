from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR, Date, Boolean, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base
import requests

Base = declarative_base()
class AirflowPlugin(Base):
    __tablename__ = 'airflow_etl_weather'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    city = Column(VARCHAR(25), nullable=False)
    temperature = Column(Float, nullable=False)
    date_save = Column(TIMESTAMP, nullable=False, index=True)


# Кастомный оператор
class ExampleOperator(BaseOperator):
    def __init__(self,
                 postgre_conn: Connection,
                 data_list: list,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.postgre_conn = postgre_conn
        self.DATABASE_URL = (
            f"postgresql://{postgre_conn.login}:{postgre_conn.password}@"
            f"{postgre_conn.host}:{str(postgre_conn.port)}/{postgre_conn.schema}"
        )
        self.data_list = data_list


    def execute(self, context):
        engine = create_engine(self.DATABASE_URL)
        Base.metadata.create_all(bind=engine)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        sessionlocal = SessionLocal()


        for city in self.data_list:
            URL = (
                f'http://api.openweathermap.org/data/2.5/weather?q={city}'
                f'&lang=ru&units=metric&appid=d7eb1364d2828dc990fc6128fb5332c9'
            )

            r = requests.get(URL)
            result = r.json()
            # Конвертируем timestamp в формат YYYY-MM-DD
            date_formatted = datetime.fromtimestamp(result["dt"], timezone.utc).strftime("%Y-%m-%d")

            weather_data = {
                "date": date_formatted,
                "city": result["name"],
                "temperature": result["main"]["temp"]
            }

            new_record_2 = AirflowPlugin(
                city=weather_data['city'],
                temperature=weather_data['temperature'],
                date_save=datetime.now(timezone.utc)
            )

            sessionlocal.add(new_record_2)

        sessionlocal.commit()