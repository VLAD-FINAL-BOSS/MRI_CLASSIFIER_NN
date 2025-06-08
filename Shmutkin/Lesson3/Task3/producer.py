import requests
import pika
import json
import time


# Кеш последней цены {тикер: цена}
last_price = {}

# Забираем данные с API и отправляем в очередь
def api_get_price(securities: str):

    params = {
        "iss.meta": "off",
        "securities": securities,
        "marketdata.columns": "SECID,LAST"
    }

    try:
        # MOEX_API_URL для акций
        URL = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"
        response = requests.get(URL, params=params)
        response.raise_for_status()
        data = response.json()

        # Парсим ответ
        marketdata = data["marketdata"]["data"]
        current_price = {}
        for item in marketdata:  # Каждый item в списке — это массив вида ['SBER', 280.50].
            ticker = item[0]
            last_price = item[1]
            if last_price is not None:
                current_price[ticker] = last_price

        return current_price

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе к API: {e}")
        return None


def main():
    # Подключение к RabbitMQ
    params = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Создаем очередь
    channel.queue_declare(queue='API_SBER')

    while True:
        try:
            current_price = api_get_price('SBER')

            if not current_price:
                time.sleep(5)
                continue

            changed_price = {}
            for ticker, price in current_price.items():
                # Если цена изменилась (или тикера не было в кеше)
                if ticker not in last_price or last_price[ticker] != price:
                    changed_price[ticker] = price
                    last_price[ticker] = price  # Обновляем кеш

            if changed_price:  # Если есть изменения
                channel.basic_publish(
                    exchange='',
                    routing_key='API_SBER',
                    body=json.dumps(changed_price)
                )
                print(f"Отправлены изменения: {changed_price}")
            else:
                print("Изменений нет")

            time.sleep(1)  # Проверяем каждую секунду

        except Exception as e:
            print(f'Ошибка: {e}')

    connection.close()


if __name__ == "__main__":
    main()






















