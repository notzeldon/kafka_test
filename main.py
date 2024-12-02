import argparse

from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

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

# init --topic 'hello_topic' --kafka 'ip:port
parser_produce = subparsers.add_parser('init')
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
        'security.protocol': 'PLAINTEXT'  # 'SSL' или 'PLAINTEXT'
    }
    producer = Producer(config)
    producer.produce(topic, value=message, callback=delivery_report)
    producer.flush()


def run_consume(topic: str, kafka: str):
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


def run_init(topic: str, kafka: str):
    config = {
        'bootstrap.servers': kafka,
    }
    admin_client = AdminClient(config)

    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1)]
    try:
        fs = admin_client.create_topics(new_topics)

        # Ожидание завершения создания тем.
        for topic, future in fs.items():
            try:
                future.result()  # Блокирует выполнение до завершения
                print(f"Тема '{topic}' успешно создана.")
            except Exception as e:
                print(f"Ошибка при создании темы '{topic}': {e}")
                return False  # Вернуть False в случае ошибки

        return True  # Вернуть True, если все темы созданы успешно

    except Exception as e:
        print(f"Общая ошибка: {e}")
        return False

    finally:
        pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    args = parser.parse_args()
    if args.command == 'produce':
        run_produce(message=args.message, topic=args.topic, kafka=args.kafka)
    elif args.command == 'consume':
        run_init(topic=args.topic, kafka=args.kafka)
        run_consume(topic=args.topic, kafka=args.kafka)
    elif args.command == 'init':
        run_init(topic=args.topic, kafka=args.kafka)
    else:
        parser.print_help()
