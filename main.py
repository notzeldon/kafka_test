import argparse
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

from confluent_kafka import Producer, Consumer, KafkaException

parser = argparse.ArgumentParser(description="Apache Kafka test app")
subparsers = parser.add_subparsers(dest='command')

# produce --message 'Hello World!' --topic 'hello_topic' --kafka 'ip:port'
parser_produce = subparsers.add_parser('produce')
parser_produce.add_argument('--message', type=str)
parser_produce.add_argument('--topic', type=str)
parser_produce.add_argument('--kafka', type=str)

# consume --topic 'hello_topic' --kafka 'ip:port
parser_produce = subparsers.add_parser('consume')
parser_produce.add_argument('--topic', type=str)
parser_produce.add_argument('--kafka', type=str)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def run_produce(message: str, topic: str, kafka: str):
    config = {
        'bootstrap.servers': kafka,
        'group.id': 'test',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'PLAINTEXT'  # 'SSL' или 'PLAINTEXT'
    }
    producer = Producer(config)
    producer.produce(topic, value=message, callback=delivery_report)
    producer.flush()


def run_consume(topic: str, kafka: str):
    logging.info(kafka)
    config = {
        'bootstrap.servers': kafka,
        'group.id': 'test',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'PLAINTEXT'  # 'SSL' или 'PLAINTEXT'
    }
    consumer = Consumer(config)
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # ожидание сообщения
            if msg is None:  # если сообщений нет
                continue
            if msg.error():  # обработка ошибок
                raise KafkaException(msg.error())
            else:
                # действия с полученным сообщением
                print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()  # не забываем закрыть соединение


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    args = parser.parse_args()
    if args.command == 'produce':
        run_produce(message=args.message, topic=args.topic, kafka=args.kafka)
    elif args.command == 'consume':
        run_consume(topic=args.topic, kafka=args.kafka)
    else:
        parser.print_help()
