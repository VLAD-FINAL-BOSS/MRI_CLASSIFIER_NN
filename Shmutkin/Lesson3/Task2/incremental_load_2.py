import requests
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, VARCHAR, TIMESTAMP, Float
from Shmutkin.Lesson3.config import DATABASE_URL
from Shmutkin.Lesson3.config import API_KEY


Base = declarative_base()

# Создаю модель ORM-модель таблицы, таблицу cityes_weather_update
class Cityes(Base):
    __tablename__ = "cityes_weather_update"
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    city = Column(VARCHAR(25), nullable=False)
    temperature = Column(Float, nullable=False)
    date_save = Column(TIMESTAMP, nullable=False, index=True)

# Инициализация и подключение к БД
# DATABASE_URL и API_URL импортирую из конфига
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
sessionlocal = SessionLocal()


def get_weather_info(city):
    '''
    Функция по подключению к внешнему API по сбору погоды
    '''

    URL = f'http://api.openweathermap.org/data/2.5/weather?q={str(city)}&lang=ru&units=metric&appid={str(API_KEY)}'

    r = requests.get(URL)
    result = r.json()

    # Конвертируем timestamp в формат YYYY-MM-DD
    date_formatted = datetime.fromtimestamp(result["dt"], timezone.utc).strftime("%Y-%m-%d")

    weather_data = {
        "date": date_formatted,
        "city": result["name"],
        "temperature": result["main"]["temp"]
    }

    return (weather_data)


# API будет собирать актуальную погоду городов из списка
data_list = ["Москва", "Бишкек", "Ташкент", "Анталия", "Мадрид", "Кипр", "Самара"]


for elem in data_list:
    weather_data = get_weather_info(elem)

    # Записи с уже имеющимися городами
    record = sessionlocal.query(Cityes).filter(Cityes.city == elem).first()

    if record:
        # Если такой город есть — обновляем поля
        record.temperature = weather_data['temperature']
        record.date_save = datetime.now(timezone.utc)
    else:
        new_record = Cityes(
            city=elem,
            temperature=weather_data['temperature'],
            date_save=datetime.now(timezone.utc)
        )

        sessionlocal.add(new_record)

sessionlocal.commit()