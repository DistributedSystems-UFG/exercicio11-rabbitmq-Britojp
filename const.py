RABBITMQ_ADDR = '32.195.37.234'

# Food Delivery Platform — pika-based example
RABBITMQ_HOST  = RABBITMQ_ADDR
RABBITMQ_PORT  = 5672
RABBITMQ_USER  = 'myuser'
RABBITMQ_PASS  = 'abc123'
RABBITMQ_VHOST = 'my_vhost'

EXCHANGE_NAME  = 'delivery_exchange'
EXCHANGE_TYPE  = 'topic'

QUEUE_PAYMENT  = 'q.payment'
QUEUE_KITCHEN  = 'q.kitchen'
QUEUE_DELIVERY = 'q.delivery'
QUEUE_NOTIFY   = 'q.notifications'

RK_PAYMENT_NEW   = 'payment.new'
RK_KITCHEN_PREP  = 'kitchen.prepare'
RK_KITCHEN_STOCK = 'kitchen.stock'
RK_DELIVERY_PICK = 'delivery.pickup'
RK_NOTIFY_PMNT   = 'notify.payment'
RK_NOTIFY_READY  = 'notify.ready'
RK_NOTIFY_DISP   = 'notify.dispatched'
