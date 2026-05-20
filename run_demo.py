
import multiprocessing
import signal
import sys
import time

import consumer_delivery
import consumer_kitchen
import consumer_notifications
import consumer_payment
import producer_inventory
import producer_orders

COMPONENTS = [
    ("NotificationConsumer", consumer_notifications.main),
    ("PaymentConsumer",      consumer_payment.main),
    ("KitchenConsumer",      consumer_kitchen.main),
    ("DeliveryConsumer",     consumer_delivery.main),
    ("InventoryProducer",    producer_inventory.main),
    ("OrderProducer",        producer_orders.main),
]


def run(name, target):
    try:
        target()
    except Exception as exc:
        print(f"[{name}] ERRO: {exc}")


def main():
    processes = []
    for name, target in COMPONENTS:
        p = multiprocessing.Process(target=run, args=(name, target), name=name)
        p.start()
        processes.append(p)
        time.sleep(1)  # Stagger so consumers register before producers fire

    def shutdown(sig, frame):
        print("\n\nEncerrando todos os componentes...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for p in processes:
        p.join()


if __name__ == "__main__":
    print("=" * 60)
    print("  Food Delivery Platform - RabbitMQ Demo")
    print("  Iniciando todos os componentes... (Ctrl+C para parar)")
    print("=" * 60 + "\n")
    main()
