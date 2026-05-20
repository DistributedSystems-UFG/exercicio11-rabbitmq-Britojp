
import json
import random
import time

import pika

from const import (
    EXCHANGE_NAME, EXCHANGE_TYPE,
    QUEUE_KITCHEN,
    RABBITMQ_HOST, RABBITMQ_PASS, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_VHOST,
    RK_KITCHEN_STOCK,
)

INGREDIENTS = [
    {"name": "Farinha de Trigo",   "unit": "kg",     "min_threshold": 10.0},
    {"name": "Queijo Mozzarella",  "unit": "kg",     "min_threshold": 5.0},
    {"name": "Molho de Tomate",    "unit": "litros", "min_threshold": 8.0},
    {"name": "Carne Bovina",       "unit": "kg",     "min_threshold": 3.0},
    {"name": "Frango",             "unit": "kg",     "min_threshold": 4.0},
    {"name": "Alface",             "unit": "kg",     "min_threshold": 2.0},
]

stock = {ing["name"]: round(random.uniform(5, 25), 2) for ing in INGREDIENTS}


def make_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST, credentials=credentials,
    )
    return pika.BlockingConnection(params)


def declare_infrastructure(channel):
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=True)
    channel.queue_declare(queue=QUEUE_KITCHEN, durable=True)
    channel.queue_bind(queue=QUEUE_KITCHEN, exchange=EXCHANGE_NAME, routing_key='kitchen.#')


def main():
    conn = make_connection()
    ch = conn.channel()
    declare_infrastructure(ch)

    print("[INVENTORY PRODUCER] Iniciado. Monitorando estoque do restaurante...\n")
    try:
        while True:
            ing = random.choice(INGREDIENTS)
            name = ing["name"]

            stock[name] = max(0.0, stock[name] - round(random.uniform(0.1, 1.5), 2))
            if random.random() < 0.2:
                restock = round(random.uniform(5, 15), 2)
                stock[name] = min(30.0, stock[name] + restock)
                print(f"[INVENTORY PRODUCER] Reabastecimento: +{restock}{ing['unit']} de {name}")

            alert = stock[name] < ing["min_threshold"]
            update = {
                "ingredient":     name,
                "current_stock":  round(stock[name], 2),
                "unit":           ing["unit"],
                "min_threshold":  ing["min_threshold"],
                "alert":          alert,
                "timestamp":      time.strftime("%H:%M:%S"),
            }

            ch.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=RK_KITCHEN_STOCK,
                body=json.dumps(update),
                properties=pika.BasicProperties(delivery_mode=2),
            )

            status = "!!! ESTOQUE BAIXO !!!" if alert else "OK"
            print(f"[INVENTORY PRODUCER] {name}: {stock[name]:.2f} {ing['unit']} [{status}]")
            time.sleep(random.uniform(4, 8))

    except KeyboardInterrupt:
        print("\n[INVENTORY PRODUCER] Encerrado.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
