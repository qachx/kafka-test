from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import os
import time

app = Flask(__name__)

# Kafka настройки
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

def get_producer():
    """Создает подключение к Kafka с повторными попытками"""
    max_retries = 10
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            return producer
        except Exception as e:
            print(f"Попытка {i+1}/{max_retries} подключения к Kafka не удалась: {e}")
            time.sleep(5)
    raise Exception("Не удалось подключиться к Kafka")

@app.route('/health', methods=['GET'])
def health():
    """Проверка состояния сервиса"""
    return jsonify({"status": "ok", "service": "producer"})

@app.route('/send', methods=['POST'])
def send_message():
    """Отправить сообщение в Kafka"""
    try:
        data = request.get_json()
        topic = data.get('topic', 'test-topic')
        message = data.get('message')
        key = data.get('key')  # опционально для партиционирования
        
        if not message:
            return jsonify({"error": "message is required"}), 400
        
        producer = get_producer()
        
        # Отправляем сообщение
        future = producer.send(topic, value=message, key=key)
        result = future.get(timeout=10)
        
        producer.close()
        
        return jsonify({
            "status": "success",
            "topic": topic,
            "partition": result.partition,
            "offset": result.offset,
            "message": message
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/send/bulk', methods=['POST'])
def send_bulk_messages():
    """Отправить много сообщений для тестирования нагрузки"""
    try:
        data = request.get_json()
        topic = data.get('topic', 'test-topic')
        count = data.get('count', 10)
        message_template = data.get('message', 'Test message #{id}')
        
        producer = get_producer()
        
        results = []
        for i in range(count):
            message = message_template.replace('{id}', str(i))
            future = producer.send(topic, value={"id": i, "text": message})
            result = future.get(timeout=10)
            results.append({
                "partition": result.partition,
                "offset": result.offset
            })
        
        producer.close()
        
        return jsonify({
            "status": "success",
            "topic": topic,
            "messages_sent": count,
            "results": results
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True) 