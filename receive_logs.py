
#!/usr/bin/env python3
import pika
import logging
from datetime import datetime


reset_flag=False


def callback(ch, method, properties, body):
    print(" [x] %s" % body)
    global reset_flag
    user_input = body.decode("utf-8")
    if "reset" in user_input:
        reset_flag=True
        print("---> close and open new file:%d" %(reset_flag))
    else:
        print("Invalid command - please try reset")
        pass

def rabbitmq_event(arg1):
    print("Inside rabbitmq_event")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='logs',
                         exchange_type='fanout')

    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='logs',
                   queue=queue_name)

    print(' [*] Waiting for logs. To exit press CTRL+C')


    channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

    channel.start_consuming()

