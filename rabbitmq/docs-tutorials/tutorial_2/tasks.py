import os
import pika
import sys


if __name__ == '__main__':
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

        channel = connection.channel()
        channel.queue_declare(queue='task_queue', durable=True)

        while True:
            message = input('\nMessage to be sent: (To exit press CTRL+C): ')

            channel.basic_publish(
                # Use the default direct exchange.
                exchange='',
                # The default direct exchange will route the message to
                # the queue with the binding key equals to the routing_key.
                routing_key='task_queue',
                body=message,
                # Assure the message will not be lost if the RabbitMQ
                # server crashes.
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                )
            )
            print(f' [x] Sent {message} to the default direct exchange.')
    except KeyboardInterrupt:
        connection.close()
        print('Interrupted.')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
