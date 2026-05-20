
import json
import random
import time
import uuid

import pika

from const import (
    EXCHANGE_NAME, EXCHANGE_TYPE,
    QUEUE_PAYMENT,
    RABBITMQ_HOST, RABBITMQ_PASS, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_VHOST,
    RK_PAYMENT_NEW,
)

MENU = [
    {"name": "Pizza Margherita",     "price": 45.90},
    {"name": "Pizza Calabresa",      "price": 48.50},
    {"name": "Hamburguer Artesanal", "price": 38.00},
    {"name": "Salada Caesar",        "price": 28.00},
    {"name": "Sushi Combo 12 pecas", "price": 62.00},
]

CUSTOMERS = [
    {"name": "Joao Silva",     "phone": "11999990001", "address": "Rua das Flores, 123"},
    {"name": "Maria Santos",   "phone": "11999990002", "address": "Av. Paulista, 456"},
    {"name": "Carlos Oliveira","phone": "11999990003", "address": "Rua Augusta, 789"},
    {"name": "Ana Costa",      "phone": "11999990004", "address": "Rua Oscar Freire, 321"},
]


def make_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST, credentials=credentials,
    )
    return pika.BlockingConnection(params)


def declare_infrastructure(channel):
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=True)
    channel.queue_declare(queue=QUEUE_PAYMENT, durable=True)
    channel.queue_bind(queue=QUEUE_PAYMENT, exchange=EXCHANGE_NAME, routing_key='payment.#')


def main():
    conn = make_connection()
    ch = conn.channel()
    declare_infrastructure(ch)

    print("[ORDER PRODUCER] Iniciado. Gerando pedidos de clientes...\n")
    try:
        while True:
            customer = random.choice(CUSTOMERS)
            items = random.sample(MENU, random.randint(1, 3))
            total = round(sum(i["price"] for i in items), 2)

            order = {
                "order_id":  str(uuid.uuid4())[:8].upper(),
                "customer":  customer["name"],
                "phone":     customer["phone"],
                "address":   customer["address"],
                "items":     items,
                "total":     total,
                "timestamp": time.strftime("%H:%M:%S"),
            }

            ch.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=RK_PAYMENT_NEW,
                body=json.dumps(order),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            print(f"[ORDER PRODUCER] Pedido #{order['order_id']} | {customer['name']} | R${total:.2f}")
            time.sleep(random.uniform(3, 6))

    except KeyboardInterrupt:
        print("\n[ORDER PRODUCER] Encerrado.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
