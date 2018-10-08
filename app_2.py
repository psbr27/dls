#!/usr/bin/python
from flask import Flask
from flask import request
from flask import json

import logging
import logging.config
from receive_logs2 import start_log2, rabbitmq_event, logger_init2
import threading
from mysql_query import mysql_select

app = Flask(__name__)

#create logger
logging.config.fileConfig('logging.conf')
logger = logger_init2("Sensor Health")

@app.route('/upload/health/<int:esc_id>', methods=['POST'])
def api_upload(esc_id):
    response = request.data
    data = json.loads(response)
    logger.info(data)
    print("------------------------------------------------------------------------->")
    return "200"

if __name__ == '__main__':
    try:
        t = threading.Thread( target=rabbitmq_event, args=("Thread-1",))
    except:
        print("Error: Unable to start thread")

    t.start()
    start_log2()
    app.run(host="0.0.0.0", port=8080, debug=False)
