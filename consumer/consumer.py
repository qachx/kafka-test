from kafka import KafkaConsumer
import json
import os
import time
import signal
import sys

# Kafka настройки
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPICS = ['test-topic', 'orders', 'users', 'metrics', 'errors']  # топики для тестирования

def signal_handler(sig, frame):
    print('\nЗавершение работы консьюмера...')
    sys.exit(0)

def create_consumer():
    """Создает подключение к Kafka с повторными попытками"""
    max_retries = 10
    for i in range(max_retries):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=[KAFKA_SERVERS],
                group_id='test-consumer-group',
                auto_offset_reset='earliest',  # читать с начала
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                key_deserializer=lambda x: x.decode('utf-8') if x else None
            )
            return consumer
        except Exception as e:
            print(f"Попытка {i+1}/{max_retries} подключения к Kafka не удалась: {e}")
            time.sleep(5)
    raise Exception("Не удалось подключиться к Kafka")

def main():
    print("Запуск Kafka консьюмера...")
    print(f"Подключение к: {KAFKA_SERVERS}")
    print(f"Слушаем топики: {TOPICS}")
    
    # Обработчик сигналов для корректного завершения
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        consumer = create_consumer()
        print("✅ Успешно подключились к Kafka")
        print("Ожидание сообщений...")
        
        for message in consumer:
            print(f"""
📨 Получено сообщение:
   Топик: {message.topic}
   Партиция: {message.partition}
   Офсет: {message.offset}
   Ключ: {message.key}
   Значение: {message.value}
   Время: {time.strftime('%Y-%m-%d %H:%M:%S')}
{"="*50}""")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        try:
            consumer.close()
            print("Консьюмер закрыт")
        except:
            pass

if __name__ == '__main__':
    main() 