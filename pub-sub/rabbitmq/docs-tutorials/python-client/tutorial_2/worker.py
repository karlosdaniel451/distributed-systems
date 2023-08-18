import datetime
import os
import pika
import sys


def save_message(message: str, receipt_datetime) -> str:
    result_path = f'./task_results/task_result_{receipt_datetime}.txt'

    with open(result_path, mode='w') as f:
        f.write(message)

    return result_path


def callback(ch, method, properties, body):
    receipt_datetime = datetime.datetime.now(datetime.timezone.utc)

    print(f'\nTask received at {receipt_datetime}')

    print('\nBeginning to process task...')

    try:
        task_prcessing_start = datetime.datetime.utcnow()

        result_path = save_message(body.decode(), receipt_datetime)

        task_prcessing_latency = datetime.datetime.utcnow() - task_prcessing_start

        print(f'Task processed in {task_prcessing_latency.total_seconds()}s.\n'
              f'Result at {result_path}')
    except BaseException as error:
        print(f'Error by processing task. {error}')
        return

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)

    channel.basic_consume(
        queue='task_queue',
        on_message_callback=callback
    )

    print(' [*] Waiting for tasks. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted.')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
