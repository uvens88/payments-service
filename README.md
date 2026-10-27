# Payments service

Микросервис асинхронной обработки платежей: принимает платёж, проводит его через эмуляцию платёжного шлюза и сообщает результат на webhook клиента.

Стек: FastAPI + Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), PostgreSQL 16, RabbitMQ 3.13 + FastStream 0.6, Alembic, Docker Compose.

## Запуск

```bash
docker compose up -d --build
```

Одна команда поднимает `postgres`, `rabbitmq`, `migrate` (применяет миграции и завершается), `api`, `consumer` и `webhook-echo` (приёмник webhook для демо).

| Что | Где |
|-----|-----|
| API | http://localhost:8000 (Swagger: http://localhost:8000/docs) |
| RabbitMQ UI | http://localhost:15672 (`payments` / `payments`) |
| API-ключ по умолчанию | `dev-api-key`; переопределяется через `.env` (см. `.env.example`) |

## Примеры

```bash
# Создание платежа
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: order-1001' \
  -H 'Content-Type: application/json' \
  -d '{"amount": "1500.00", "currency": "RUB", "description": "Order #1001",
       "metadata": {"order_id": 1001}, "webhook_url": "http://webhook-echo:8080/payments"}'
# HTTP/1.1 202 Accepted
# {"payment_id":"6f0c…","status":"pending","created_at":"2026-10-27T15:58:00.123456Z"}

# Получение платежа (через 2–5 с статус станет succeeded или failed)
curl -s http://localhost:8000/api/v1/payments/<payment_id> -H 'X-API-Key: dev-api-key'

# Webhook, который получил приёмник
docker compose logs webhook-echo
```

Повтор запроса с тем же `Idempotency-Key` и тем же телом вернёт тот же платёж, второго платежа и второго события не будет. Тот же ключ с другим телом даёт `409 Conflict`.

| Код | Когда |
|-----|-------|
| 202 | платёж принят (или возвращён ранее созданный по тому же ключу) |
| 401 | нет или неверный `X-API-Key` |
| 404 | платёж не найден |
| 409 | `Idempotency-Key` уже использован с другим телом |
| 422 | нет `Idempotency-Key` или тело не прошло валидацию (сумма > 0, не более 2 знаков после точки; валюта RUB/USD/EUR; корректный URL) |

Сумма в ответах — строка (`"1500.00"`), чтобы не терять точность на float.

## Архитектура

```
POST /payments ─▶ api ─(одна транзакция)─▶ payments + outbox        (API ходит только в Postgres)
                                                │
                     consumer: outbox relay ────┘  SELECT … FOR UPDATE SKIP LOCKED → publish (confirm) → published_at
                                                ▼
             exchange payments ─ payments.new ─▶ consumer: шлюз 2–5 с (90/10) → UPDATE статуса → webhook
                                                │ ошибка
                    попытка < 3 ─▶ payments.new.retry (TTL = 5 с·2^(n-1), DLX → payments.new)
                    попытка = 3 ─▶ reject ─▶ payments.dlx ─▶ payments.new.dlq
```

| Файл | Что делает |
|------|------------|
| `app/api.py` | эндпоинты, проверка API-ключа, идемпотентность, запись в outbox |
| `app/models.py` | таблицы `payments` и `outbox` |
| `app/broker.py` | топология RabbitMQ: обменники, очереди, DLX |
| `app/outbox.py` | outbox relay |
| `app/consumer.py` | FastStream-приложение: обработчик, эмуляция шлюза, webhook, retry, запуск relay |
| `migrations/` | Alembic (async) |

### Outbox
API в одной транзакции пишет платёж и событие в таблицу `outbox`, поэтому событие не может потеряться между БД и брокером. Relay работает в процессе consumer: выбирает неопубликованные события (`FOR UPDATE SKIP LOCKED`, поэтому безопасно запускать несколько реплик), публикует их как persistent-сообщения, ждёт publisher confirm и проставляет `published_at`.

Канал открыт с `on_return_raises=True`. По умолчанию FastStream молча «подтверждает» сообщение, которое некуда доставить, и relay посчитал бы его отправленным. Гарантия доставки — at-least-once: если процесс упал между публикацией и commit, событие уйдёт повторно. Поэтому consumer идемпотентен.

### Идемпотентность
- **API:** `UNIQUE(idempotency_key)` и `INSERT … ON CONFLICT DO NOTHING RETURNING`. Гонка двух одинаковых запросов решается базой без исключений и откатов.
- **Consumer:** шлюз вызывается, только пока платёж в статусе `pending`. Статус записывается условно (`UPDATE … WHERE status = 'pending'`). Если та же доставка пришла дважды одновременно, побеждает одна запись, и обе копии отправляют в webhook статус из БД. Повторная доставка обработанного платежа шлюз уже не вызывает, а только повторяет webhook.

### Retry и DLQ
- «3 попытки» — это три попытки всего, первая тоже считается: сообщение уходит в DLQ, если не обработано после 3 попыток. Значение задаётся `MAX_ATTEMPTS`.
- Задержка экспоненциальная: `RETRY_BASE_DELAY · 2^(n-1)`, то есть 5 с, затем 10 с.
- Повтор реализован на стороне RabbitMQ, а не через `sleep` в обработчике: сообщение публикуется в `payments.new.retry` с per-message TTL и номером попытки в заголовке `x-attempt`. Когда TTL истекает, DLX этой очереди возвращает сообщение в `payments.new`. Consumer не занят ожиданием, а повтор переживает рестарт процесса.
- Повторяется любая ошибка: недоставленный webhook (таймаут, не-2xx), недоступная БД и т. п. Шлюз при повторе не перезапускается, потому что платёж уже не `pending`, — повторяется только webhook.
- После третьей неудачи сообщение отклоняется (`reject`) и через DLX `payments.dlx` попадает в `payments.new.dlq`. Причина видна в заголовке `x-death`. Сообщение, которое не удалось разобрать (битый JSON), уходит в DLQ сразу.
- Сообщение из DLQ можно вернуть в `payments.new` (shovel или RabbitMQ UI). Если платёж ещё `pending`, он будет обработан; если уже обработан, повторно отправится только webhook.
- Отказ шлюза (10%) — это результат платежа (`failed`), а не ошибка: webhook отправляется, повтор не запускается.

## Тесты

```bash
docker compose --profile test run --rm tests                 # 24 теста: API, outbox, consumer, retry, DLQ
docker compose --profile test run --rm tests pytest -m e2e   # e2e против поднятого стека
```

Тесты идут против настоящих Postgres (отдельная БД `payments_test`) и RabbitMQ (отдельный vhost `test`), поэтому TTL, DLX и DLQ проверяются без эмуляции.

## Настройки (env)

| Переменная | По умолчанию | |
|------------|--------------|--|
| `API_KEY` | — (обязательна) | статический ключ для `X-API-Key` |
| `DATABASE_URL`, `RABBITMQ_URL` | см. `app/config.py` | |
| `MAX_ATTEMPTS` | 3 | попыток всего |
| `RETRY_BASE_DELAY` | 5 | секунд, база экспоненты |
| `WEBHOOK_TIMEOUT` | 5 | секунд |
| `CONSUMER_PREFETCH` | 10 | сколько сообщений consumer обрабатывает параллельно |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` / `GATEWAY_SUCCESS_RATE` | 2 / 5 / 0.9 | эмуляция шлюза |
| `OUTBOX_POLL_INTERVAL` / `OUTBOX_BATCH_SIZE` | 0.5 / 100 | relay |

## Известные ограничения
- Одна retry-очередь с per-message TTL: RabbitMQ снимает истёкшие сообщения только с головы очереди, поэтому короткая задержка, стоящая за длинной, ждёт дольше. Если задержки должны быть точными, нужна отдельная очередь на каждую ступень.
- Relay опрашивает таблицу раз в 0.5 с. Если нужна меньшая задержка, можно перейти на `LISTEN/NOTIFY`.
- `webhook_url` не проверяется на внутренние адреса (SSRF): для демо нужен внутренний хост `webhook-echo`. Webhook не подписывается; следующий шаг — HMAC-подпись в заголовке.
- `/docs` открыт без ключа, чтобы проверяющему было удобно; ключ обязателен для всех эндпоинтов `/api/v1`.
