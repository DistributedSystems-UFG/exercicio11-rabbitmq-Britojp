
import json
import random
import time

import pika

from const import (
    EXCHANGE_NAME, EXCHANGE_TYPE,
    QUEUE_DELIVERY, QUEUE_NOTIFY,
    RABBITMQ_HOST, RABBITMQ_PASS, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_VHOST,
    RK_NOTIFY_DISP,
)

DRIVERS = [
    {"name": "Carlos Mendes",   "vehicle": "Moto Honda CG 160",     "available": True},
    {"name": "Pedro Alves",     "vehicle": "Bicicleta Eletrica",     "available": True},
    {"name": "Luisa Ferreira",  "vehicle": "Moto Yamaha Fazer",      "available": True},
    {"name": "Bruno Costa",     "vehicle": "Moto Honda Biz 125",     "available": True},
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
    channel.queue_declare(queue=QUEUE_DELIVERY, durable=True)
    channel.queue_declare(queue=QUEUE_NOTIFY,   durable=True)
    channel.queue_bind(queue=QUEUE_DELIVERY, exchange=EXCHANGE_NAME, routing_key='delivery.#')
    channel.queue_bind(queue=QUEUE_NOTIFY,   exchange=EXCHANGE_NAME, routing_key='notify.#')


def assign_driver():
    available = [d for d in DRIVERS if d["available"]]
    if not available:
        return None
    driver = random.choice(available)
    driver["available"] = False
    return driver


def on_message(ch, method, properties, body):
    order = json.loads(body)
    order_id = order["order_id"]

    print(f"\n[DELIVERY] Atribuindo entregador para pedido #{order_id}...")
    driver = assign_driver()

    if not driver:
        print(f"[DELIVERY] Sem entregadores disponiveis. Pedido #{order_id} retornado a fila.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        time.sleep(5)
        return

    eta = random.randint(20, 45)
    print(f"[DELIVERY] Pedido #{order_id} -> {driver['name']} ({driver['vehicle']})")
    print(f"[DELIVERY] Destino: {order['address']} | ETA: ~{eta} min")

    time.sleep(random.uniform(0.5, 1.0))
    driver["available"] = True  # Freed after simulated delivery

    notification = {
        "customer":  order["customer"],
        "phone":     order["phone"],
        "order_id":  order_id,
        "message":   (
            f"Pedido #{order_id} a caminho! "
            f"Entregador: {driver['name']} ({driver['vehicle']}). "
            f"Previsao: ~{eta} min."
        ),
        "type":      "sms",
        "timestamp": time.strftime("%H:%M:%S"),
    }
    ch.basic_publish(
        exchange=EXCHANGE_NAME, routing_key=RK_NOTIFY_DISP,
        body=json.dumps(notification),
        properties=pika.BasicProperties(delivery_mode=2),
    )

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    conn = make_connection()
    ch = conn.channel()
    declare_infrastructure(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_DELIVERY, on_message_callback=on_message)
    print("[DELIVERY CONSUMER] Aguardando pedidos para despacho...\n")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        print("\n[DELIVERY CONSUMER] Encerrado.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
