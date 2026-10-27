from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: str  # no default: the service refuses to start without a key

    gateway_min_delay: float = 2.0
    gateway_max_delay: float = 5.0
    gateway_success_rate: float = 0.9

    max_attempts: int = 3  # total, the first attempt included
    retry_base_delay: float = 5.0  # seconds; attempt n waits base * 2**(n-1)
    webhook_timeout: float = 5.0
    consumer_prefetch: int = 10

    outbox_poll_interval: float = 0.5
    outbox_batch_size: int = 100


settings = Settings()
