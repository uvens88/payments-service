from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings
from app.models import PAYMENTS_NEW

PAYMENTS_RETRY = f"{PAYMENTS_NEW}.retry"
PAYMENTS_DLQ = f"{PAYMENTS_NEW}.dlq"

# on_return_raises: an unroutable message must fail publish(), otherwise the outbox
# relay would mark it as published while RabbitMQ silently drops it.
broker = RabbitBroker(
    settings.rabbitmq_url,
    default_channel=Channel(prefetch_count=settings.consumer_prefetch, on_return_raises=True),
    # On shutdown, let in-flight handlers finish their gateway call and webhook.
    graceful_timeout=settings.gateway_max_delay + settings.webhook_timeout,
)

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
dead_letter_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

payments_queue = RabbitQueue(
    PAYMENTS_NEW,
    durable=True,
    routing_key=PAYMENTS_NEW,
    arguments={"x-dead-letter-exchange": dead_letter_exchange.name, "x-dead-letter-routing-key": PAYMENTS_DLQ},
)
# No consumers: a message waits here for its per-message TTL, then dead-letters back into payments.new.
# Trade-off: one retry queue with per-message TTL has head-of-line blocking (a short delay behind a
# long one waits longer); use a queue per backoff step if delays must be exact.
retry_queue = RabbitQueue(
    PAYMENTS_RETRY,
    durable=True,
    routing_key=PAYMENTS_RETRY,
    arguments={"x-dead-letter-exchange": payments_exchange.name, "x-dead-letter-routing-key": PAYMENTS_NEW},
)
dead_letter_queue = RabbitQueue(PAYMENTS_DLQ, durable=True, routing_key=PAYMENTS_DLQ)


async def declare_topology() -> None:
    bindings = (
        (payments_queue, payments_exchange),
        (retry_queue, payments_exchange),
        (dead_letter_queue, dead_letter_exchange),
    )
    for queue, exchange in bindings:
        await broker.declare_exchange(exchange)
        declared = await broker.declare_queue(queue)
        await declared.bind(exchange.name, routing_key=queue.routing_key)
