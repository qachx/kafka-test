import time
import json
import random
import os
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# Настройки
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
fake = Faker(['ru_RU', 'en_US'])

def create_producer():
    """Создает подключение к Kafka с повторными попытками"""
    max_retries = 20
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVERS],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            return producer
        except Exception as e:
            print(f"⏳ Попытка {i+1}/{max_retries} подключения к Kafka: {e}", flush=True)
            time.sleep(10)
    raise Exception("❌ Не удалось подключиться к Kafka")

def generate_user_event():
    """Генерирует события пользователей"""
    user_id = fake.random_int(min=1, max=1000)
    events = ['login', 'logout', 'view_product', 'add_to_cart', 'purchase', 'profile_update']
    
    return {
        'event_id': fake.uuid4(),
        'user_id': user_id,
        'event_type': random.choice(events),
        'timestamp': datetime.now().isoformat(),
        'user_agent': fake.user_agent(),
        'ip_address': fake.ipv4(),
        'session_id': fake.uuid4()[:8],
        'details': {
            'page': fake.uri_path(),
            'duration_seconds': fake.random_int(min=5, max=300)
        }
    }

def generate_order():
    """Генерирует заказы"""
    order_id = fake.random_int(min=10000, max=99999)
    products = [
        'Ноутбук', 'Телефон', 'Планшет', 'Наушники', 'Клавиатура', 'Мышь', 
        'Монитор', 'Камера', 'Книга', 'Кофе', 'Футболка', 'Джинсы'
    ]
    
    return {
        'order_id': order_id,
        'user_id': fake.random_int(min=1, max=1000),
        'status': random.choice(['created', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled']),
        'products': [
            {
                'name': random.choice(products),
                'price': round(random.uniform(500, 50000), 2),
                'quantity': fake.random_int(min=1, max=3)
            } for _ in range(fake.random_int(min=1, max=4))
        ],
        'total_amount': round(random.uniform(1000, 100000), 2),
        'currency': 'RUB',
        'payment_method': random.choice(['card', 'cash', 'online', 'crypto']),
        'delivery_address': {
            'city': fake.city(),
            'address': fake.address(),
            'postcode': fake.postcode()
        },
        'created_at': datetime.now().isoformat(),
        'estimated_delivery': fake.future_datetime(end_date='+30d').isoformat()
    }

def generate_system_metric():
    """Генерирует системные метрики"""
    services = ['api-gateway', 'user-service', 'order-service', 'payment-service', 'notification-service']
    
    return {
        'service_name': random.choice(services),
        'metric_type': random.choice(['cpu_usage', 'memory_usage', 'response_time', 'error_rate', 'requests_per_second']),
        'value': round(random.uniform(0, 100), 2),
        'unit': random.choice(['percent', 'milliseconds', 'count', 'bytes']),
        'timestamp': datetime.now().isoformat(),
        'environment': random.choice(['prod', 'stage', 'dev']),
        'instance_id': fake.uuid4()[:8],
        'tags': {
            'region': random.choice(['us-east-1', 'eu-west-1', 'asia-pacific']),
            'az': random.choice(['a', 'b', 'c'])
        }
    }

def generate_error_log():
    """Генерирует логи ошибок"""
    error_types = ['ValidationError', 'DatabaseError', 'NetworkError', 'AuthenticationError', 'RateLimitError']
    
    return {
        'error_id': fake.uuid4(),
        'error_type': random.choice(error_types),
        'message': fake.sentence(),
        'stack_trace': fake.text(max_nb_chars=200),
        'user_id': fake.random_int(min=1, max=1000) if random.choice([True, False]) else None,
        'request_id': fake.uuid4()[:8],
        'service': random.choice(['api', 'web', 'mobile', 'admin']),
        'severity': random.choice(['low', 'medium', 'high', 'critical']),
        'timestamp': datetime.now().isoformat(),
        'resolved': random.choice([True, False]),
        'metadata': {
            'endpoint': fake.uri_path(),
            'method': random.choice(['GET', 'POST', 'PUT', 'DELETE']),
            'status_code': random.choice([400, 401, 403, 404, 500, 502, 503])
        }
    }

def send_message(producer, topic, message, key=None):
    """Отправляет сообщение в Kafka"""
    try:
        future = producer.send(topic, value=message, key=key)
        result = future.get(timeout=5)
        print(f"📤 {topic}: {message.get('event_type', message.get('order_id', message.get('error_type', 'message')))} → partition {result.partition}, offset {result.offset}", flush=True)
        return True
    except Exception as e:
        print(f"❌ Ошибка отправки в {topic}: {e}", flush=True)
        return False

def main():
    print("🚀 Запуск автоматического генератора данных...", flush=True)
    print(f"🔗 Подключение к Kafka: {KAFKA_SERVERS}", flush=True)
    
    try:
        producer = create_producer()
        print("✅ Подключение к Kafka установлено", flush=True)
        print("🎲 Начинаю генерацию данных...\n", flush=True)
        
        message_count = 0
        
        while True:
            # Определяем что генерировать на этой итерации
            action = random.choices(
                ['user_event', 'order', 'metric', 'error'],
                weights=[40, 25, 25, 10],  # пользовательские события чаще
                k=1
            )[0]
            
            if action == 'user_event':
                message = generate_user_event()
                key = str(message['user_id'])
                send_message(producer, 'users', message, key)
                
            elif action == 'order':
                message = generate_order()
                key = str(message['user_id'])
                send_message(producer, 'orders', message, key)
                
            elif action == 'metric':
                message = generate_system_metric()
                key = message['service_name']
                send_message(producer, 'metrics', message, key)
                
            elif action == 'error':
                message = generate_error_log()
                key = message['service']
                send_message(producer, 'errors', message, key)
            
            message_count += 1
            
            # Статистика каждые 50 сообщений
            if message_count % 50 == 0:
                print(f"\n📊 Отправлено сообщений: {message_count}")
                print("=" * 50)
            
            # Случайная задержка от 5 до 30 секунд
            delay = random.uniform(5, 30)
            time.sleep(delay)
            
    except KeyboardInterrupt:
        print("\n⏹️ Генератор остановлен")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
    finally:
        try:
            producer.close()
            print("🔒 Подключение к Kafka закрыто")
        except:
            pass

if __name__ == '__main__':
    main() 