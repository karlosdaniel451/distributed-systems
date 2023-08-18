import pika


def get_possible_log_severities() -> list[str]:
    possible_log_severities: list[str] = []

    while True:
        new_log_severity = input(
            'Type a new possible log severity or "\stop" to stop log'
            ' severites setting: '
        )
        if new_log_severity == '\stop':
            break
        possible_log_severities.append(new_log_severity)

    return possible_log_severities


def main():
    possible_log_severities = get_possible_log_severities()
    if not possible_log_severities:
        print('No log severity has been defined.')
        exit()

    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='log_exchange', exchange_type='direct')

    while True:
        log_severity = input(
            '\nType the log severity for a new message or "\stop" to stop:\n'
        )
        if log_severity == '\stop':
            break

        if log_severity not in possible_log_severities:
            print(
                f'Error: There is no such log severity. The possible log'
                f' severities are {possible_log_severities}.'
            )
            continue

        log_message = input('\nType the log message or "\stop" to stop:\n')
        if log_message == '\stop':
            break

        channel.basic_publish(
            exchange='log_exchange',
            routing_key=log_severity,
            body=log_message
        )

    connection.close()


if __name__ == '__main__':
    main()
