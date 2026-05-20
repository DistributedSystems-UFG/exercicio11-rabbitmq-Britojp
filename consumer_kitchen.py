
import json
import random
import time

import pika

from const import (
    EXCHANGE_NAME, EXCHANGE_TYPE,
    QUEUE_DELIVERY, QUEUE_KITCHEN, QUEUE_NOTIFY,
    RABBITMQ_HOST, RABBITMQ_PASS, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_VHOST,
    RK_DELIVERY_PICK, RK_NOTIFY_READY,
)

PREP_TIMES = {
    "Pizza Margherita":     20,
    "Pizza Calabresa":      22,
    "Hamburguer Artesanal": 15,
    "Salada Caesar":        8,
    "Sushi Combo 12 pecas": 25,
}


def make_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST, credentials=credentials,
    )
    return pika.BlockingConnection(params)


def declare_infrastructure(channel):
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=True)
    channel.queue_declare(queue=QUEUE_KITCHEN,  durable=True)
    channel.queue_declare(queue=QUEUE_DELIVERY, durable=True)
    channel.queue_declare(queue=QUEUE_NOTIFY,   durable=True)
    channel.queue_bind(queue=QUEUE_KITCHEN,  exchange=EXCHANGE_NAME, routing_key='kitchen.#')
    channel.queue_bind(queue=QUEUE_DELIVERY, exchange=EXCHANGE_NAME, routing_key='delivery.#')
    channel.queue_bind(queue=QUEUE_NOTIFY,   exchange=EXCHANGE_NAME, routing_key='notify.#')


def handle_stock_update(msg):
    name  = msg["ingredient"]
    qty   = msg["current_stock"]
    unit  = msg["unit"]
    alert = msg.get("alert", False)
    if alert:
        print(f"\n[KITCHEN] *** ALERTA: Estoque baixo de '{name}': "
              f"{qty:.2f} {unit} (minimo: {msg['min_threshold']} {unit}) ***")
    else:
        print(f"[KITCHEN] Estoque OK: {name} = {qty:.2f} {unit}")


def handle_prepare_order(ch, msg):
    order_id = msg["order_id"]
    items    = msg["items"]
    names    = ", ".join(i["name"] for i in items)
    prep_min = max((PREP_TIMES.get(i["name"], 15) for i in items), default=15)

    print(f"\n[KITCHEN] Preparando pedido #{order_id}: {names}")
    print(f"[KITCHEN] Tempo estimado: {prep_min} min. Simulando preparo...")
    time.sleep(random.uniform(1, 3))
    print(f"[KITCHEN] Pedido #{order_id} PRONTO!")

    delivery_msg = {
        "order_id": order_id,
        "customer": msg["customer"],
        "phone":    msg["phone"],
        "address":  msg["address"],
        "items":    items,
        "timestamp": time.strftime("%H:%M:%S"),
    }
    ch.basic_publish(
        exchange=EXCHANGE_NAME, routing_key=RK_DELIVERY_PICK,
        body=json.dumps(delivery_msg),
        properties=pika.BasicProperties(delivery_mode=2),
    )

    notification = {
        "customer":  msg["customer"],
        "phone":     msg["phone"],
        "order_id":  order_id,
        "message":   f"Pedido #{order_id} pronto! Saindo para entrega em instantes.",
        "type":      "push",
        "timestamp": time.strftime("%H:%M:%S"),
    }
    ch.basic_publish(
        exchange=EXCHANGE_NAME, routing_key=RK_NOTIFY_READY,
        body=json.dumps(notification),
        properties=pika.BasicProperties(delivery_mode=2),
    )


def on_message(ch, method, properties, body):
    msg = json.loads(body)
    if "ingredient" in msg:
        handle_stock_update(msg)
    else:
        handle_prepare_order(ch, msg)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    conn = make_connection()
    ch = conn.channel()
    declare_infrastructure(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_KITCHEN, on_message_callback=on_message)
    print("[KITCHEN CONSUMER] Aguardando pedidos e atualizacoes de estoque...\n")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        print("\n[KITCHEN CONSUMER] Encerrado.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
