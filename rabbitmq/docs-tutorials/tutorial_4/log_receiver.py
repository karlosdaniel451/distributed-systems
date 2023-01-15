import pika


def callback(ch, method, properties, body):
    log_severity = method.routing_key
    print(f'{log_severity} - {body.decode()}')


def get_log_severities_to_consume() -> list[str]:
    log_severities_to_consume: list[str] = []

    while True:
        new_log_severity = input(
            'Type a new possible log severity to consume or "\stop" to stop'
            ' log severites setting: '
        )
        if new_log_severity == '\stop':
            break
        log_severities_to_consume.append(new_log_severity)

    return log_severities_to_consume


def main():
    log_severities_to_consume = get_log_severities_to_consume()
    if not log_severities_to_consume:
        print('No log severity has been defined.')
        exit()

    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='log_exchange', exchange_type='direct')

    queue_declare_result = channel.queue_declare(queue='', exclusive=True)
    queue_name = queue_declare_result.method.queue

    for log_severity in log_severities_to_consume:
        channel.queue_bind(
            queue=queue_name,
            exchange='log_exchange',
            routing_key=log_severity
        )

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    print('\nBeginning to consume log messages with one of the following '
          f'severities: {log_severities_to_consume}')

    channel.start_consuming()


if __name__ == '__main__':
    main()
