
import json
import random
import time

import pika

from const import (
    EXCHANGE_NAME, EXCHANGE_TYPE,
    QUEUE_KITCHEN, QUEUE_NOTIFY, QUEUE_PAYMENT,
    RABBITMQ_HOST, RABBITMQ_PASS, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_VHOST,
    RK_KITCHEN_PREP, RK_NOTIFY_PMNT,
)


def make_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST, credentials=credentials,
    )
    return pika.BlockingConnection(params)


def declare_infrastructure(channel):
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=True)
    channel.queue_declare(queue=QUEUE_PAYMENT,  durable=True)
    channel.queue_declare(queue=QUEUE_KITCHEN,  durable=True)
    channel.queue_declare(queue=QUEUE_NOTIFY,   durable=True)
    channel.queue_bind(queue=QUEUE_PAYMENT,  exchange=EXCHANGE_NAME, routing_key='payment.#')
    channel.queue_bind(queue=QUEUE_KITCHEN,  exchange=EXCHANGE_NAME, routing_key='kitchen.#')
    channel.queue_bind(queue=QUEUE_NOTIFY,   exchange=EXCHANGE_NAME, routing_key='notify.#')


def process_payment(order_id, total):
    """Simulate card authorization (90 % approval rate, ~1 s delay)."""
    time.sleep(random.uniform(0.5, 1.5))
    approved = random.random() < 0.90
    txn_id = f"TXN-{order_id}-{int(time.time())}"
    return approved, txn_id


def on_message(ch, method, properties, body):
    order = json.loads(body)
    order_id = order["order_id"]
    total = order["total"]

    print(f"\n[PAYMENT] Autorizando pagamento do pedido #{order_id} | R${total:.2f} ...")
    approved, txn_id = process_payment(order_id, total)
    status_label = "APROVADO" if approved else "RECUSADO"
    print(f"[PAYMENT] Pedido #{order_id}: {status_label} (txn: {txn_id})")

    notification = {
        "customer":  order["customer"],
        "phone":     order["phone"],
        "order_id":  order_id,
        "message": (
            f"Pagamento aprovado! Pedido #{order_id} sendo preparado."
            if approved else
            f"Pagamento recusado no pedido #{order_id}. Verifique seu cartao."
        ),
        "type":      "push",
        "timestamp": time.strftime("%H:%M:%S"),
    }
    ch.basic_publish(
        exchange=EXCHANGE_NAME, routing_key=RK_NOTIFY_PMNT,
        body=json.dumps(notification),
        properties=pika.BasicProperties(delivery_mode=2),
    )

    if approved:
        kitchen_msg = {
            "order_id":       order_id,
            "customer":       order["customer"],
            "phone":          order["phone"],
            "address":        order["address"],
            "items":          order["items"],
            "transaction_id": txn_id,
            "timestamp":      time.strftime("%H:%M:%S"),
        }
        ch.basic_publish(
            exchange=EXCHANGE_NAME, routing_key=RK_KITCHEN_PREP,
            body=json.dumps(kitchen_msg),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"[PAYMENT] Pedido #{order_id} encaminhado para a cozinha.")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    conn = make_connection()
    ch = conn.channel()
    declare_infrastructure(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_PAYMENT, on_message_callback=on_message)
    print("[PAYMENT CONSUMER] Aguardando pedidos de pagamento...\n")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        print("\n[PAYMENT CONSUMER] Encerrado.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
