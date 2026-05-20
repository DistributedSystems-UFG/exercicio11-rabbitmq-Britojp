
import json
import time

import pika

from const import (
    EXCHANGE_NAME, EXCHANGE_TYPE,
    QUEUE_NOTIFY,
    RABBITMQ_HOST, RABBITMQ_PASS, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_VHOST,
)

CHANNEL_LABEL = {"push": "[PUSH]", "sms": "[SMS ]", "email": "[MAIL]"}


def make_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST, credentials=credentials,
    )
    return pika.BlockingConnection(params)


def declare_infrastructure(channel):
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=True)
    channel.queue_declare(queue=QUEUE_NOTIFY, durable=True)
    channel.queue_bind(queue=QUEUE_NOTIFY, exchange=EXCHANGE_NAME, routing_key='notify.#')


def dispatch(notification):
    label    = CHANNEL_LABEL.get(notification.get("type", "push"), "[NOTIF]")
    customer = notification["customer"]
    phone    = notification["phone"]
    order_id = notification["order_id"]
    message  = notification["message"]

    print(f"\n[NOTIFY] {label} -> {customer} ({phone})")
    print(f"[NOTIFY]   Pedido #{order_id}: {message}")
    time.sleep(0.1)  # Simulate network call to push/SMS gateway


def on_message(ch, method, properties, body):
    notification = json.loads(body)
    dispatch(notification)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    conn = make_connection()
    ch = conn.channel()
    declare_infrastructure(ch)
    ch.basic_qos(prefetch_count=5)
    ch.basic_consume(queue=QUEUE_NOTIFY, on_message_callback=on_message)
    print("[NOTIFICATION CONSUMER] Aguardando notificacoes para clientes...\n")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        print("\n[NOTIFICATION CONSUMER] Encerrado.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
