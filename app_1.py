#!/usr/bin/python3
#This application POST dpaNotification message to SAS.
#Because of no real SAS, the data consumed here
from flask import Flask
from flask import request
from flask import json

import logging
import logging.config
app = Flask(__name__)


#create logger
logger = logging.config.fileConfig('logging.conf')

@app.route('/heartbeat/', methods=['POST'])
def api_upload():
	print("\n\n")
	response = request.data
	data = json.loads(response)
	new_data = str(data)
	new_data = new_data.replace("\\", "")
	new_data = new_data.strip('"')
	logging.info(new_data)
	return ""

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8081, debug=False)
