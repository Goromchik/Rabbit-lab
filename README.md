# Rabbit-lab
## Запуск
```bash
docker compose up -d --build
docker compose ps
```

## Тестирование
Логи producer и consumer:
```bash
docker compose logs -f producer
docker compose logs -f consumer
```
Журнал consumer хранится в файле `logs/consumer.log`.

Состояние очередей:
```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

Веб-интерфейс: http://localhost:15672 (логин `app`, пароль `secret`).
Очередь `tasks_dlq` содержит отклонённые сообщения (Queues -> tasks_dlq -> Get messages).

Проверка надёжности:
```bash
docker compose stop consumer
docker compose exec rabbitmq rabbitmqctl list_queues name messages
docker compose start consumer
```
Накопленные сообщения будут обработаны после запуска consumer.

## Остановка
```bash
docker compose down
```
