# Kafka Test Project

Готовая песочница для изучения Apache Kafka тестировщиками.  
**Запустил Docker → данные автоматически генерируются!**

## Быстрый старт

```bash
docker-compose up -d
```

**Всё!** Система сама:
- Запускает Kafka + все сервисы
- Генерирует реальные данные каждые 5-30 сек
- Обрабатывает сообщения в реальном времени

## Что открыть

- **Kafka UI:** http://localhost:8080 - топики, сообщения, партиции
- **REST API:** http://localhost:5000 - для ручной отправки

## 5 ключевых тестов

### 1. **Мониторинг живых данных**
Kafka UI → Topics → `users`, `orders`, `metrics`, `errors`  
Смотри как растут сообщения в реальном времени

### 2. **Партиционирование**
Kafka UI → Topics → users → Messages  
Проверь: одинаковые `user_id` попадают в одну партицию

### 3. **REST API**
```bash
curl -X POST http://localhost:5000/send -H "Content-Type: application/json" -d '{"topic": "test-topic", "message": "Мой тест"}'
```

### 4. **Консьюмер в реальном времени**
```bash
docker-compose logs -f consumer
```

### 5. **Нагрузочный тест**
```bash
curl -X POST http://localhost:5000/send/bulk -H "Content-Type: application/json" -d '{"topic": "orders", "count": 50}'
```

## Автоматические данные

Система генерирует:
- 📱 **События пользователей** - логин, покупки, просмотры
- 🛒 **Заказы** - с товарами и адресами доставки  
- 📊 **Метрики** - CPU, память, производительность
- ❌ **Ошибки** - разные уровни критичности

## Полезные команды

```bash
# Остановить генерацию
docker-compose stop data-generator

# Возобновить генерацию  
docker-compose start data-generator

# Логи всех сервисов
docker-compose logs -f

# Остановить всё
docker-compose down
``` 