#!/usr/bin/python3
import dpaNotifier as dpn
import receive_logs as rl

import logging
import time
import logging.config
import threading
import coloredlogs


from multiprocessing import Queue
import socket
import json
from threading import Thread
from prettytable import PrettyTable
from colorama import Fore, Back, Style
from datetime import datetime, timedelta
import copy
import collections
import mysqlite as sqlA
from resettable import reset_table
from threading import Timer

filehandler=""
logger=""


"""
log functions
"""
def logger_init(logger_name):
    global logger
    """
    LOG_LEVEL = logging.DEBUG
    LOGFORMAT = "  %(log_color)s%(levelname)-8s%(reset)s | %(log_color)s%(message)s%(reset)s"
    from colorlog import ColoredFormatter
    logging.root.setLevel(LOG_LEVEL)
    formatter = ColoredFormatter(LOGFORMAT)
    stream = logging.StreamHandler()
    stream.setLevel(LOG_LEVEL)
    stream.setFormatter(formatter)
    """
    logger = logging.getLogger(logger_name)
    #logger.setLevel(logging.NOTSET)
    #logger.addHandler(stream)
    return logger


#create logger
coloredlogs.install()
logging.config.fileConfig('logging.conf')
logger = logger_init("SensorHealth")


def start_log():
    global filehandler
    LOG_LEVEL = logging.DEBUG
    stringfilename=datetime.now().strftime('sensor_health_log_%Y_%m_%H_%M_%S.txt')
    filehandler=logging.FileHandler(stringfilename)
    LOGFORMAT = "  %(log_color)s%(levelname)-8s%(reset)s | %(log_color)s%(message)s%(reset)s"
    from colorlog import ColoredFormatter
    logging.root.setLevel(LOG_LEVEL)
    formatter = ColoredFormatter(LOGFORMAT)
    #formatter=logging.Formatter('%(asctime)s %(message)s')
    logger.addHandler(filehandler)
    logger.setLevel(logging.INFO)

def stop_log():
    global filehandler
    logger=logging.getLogger("SensorHealth")
    logger.removeHandler(filehandler)
    filehandler.close()



#Queues
rx_queue = Queue()
dpaQ = Queue()
sqlQ = Queue()

UDP_IP = "192.168.0.100"
UDP_PORT = 1194
table = PrettyTable(['Waveform', 'Channel_1', 'Channel_2', 'Channel_3', 'Channel_4', 'Channel_5', 'Channel_6',
                        'Channel_7', 'Channel_8', 'Channel_9', 'Channel_10'])

wv_detection_dict={}
ch_detection_list=[]


def returnNotMatches(a, b):
    a = set(a)
    b = set(b)
    return [list(a - b)]


def Remove(duplicate):
    final_list = []
    for num in duplicate:
        if num not in final_list:
            final_list.append(num)
    return final_list

#__________________________________________________________________#
def todayAt (hr, min=0, sec=0, micros=0):
    now = datetime.now()
    return now.replace(hour=hr, minute=min, second=sec, microsecond=micros)

#__________________________________________________________________#

def tx_heartbeat(t_id, delay):
    print("Inside dpaNotifier")
    ch_list=[]
    ch_list1=[]
    dict_ch={}
    new_ch_list=[]


    while True:
        time.sleep(delay)
        logger.info("[dpaQ] Size : %d" %(dpaQ.qsize()))


        if dpaQ.empty():
            logger.info("[%s] Heartbeat ---> alive" %(datetime.now()))
            dpn.dpa_heartbeat(0, 0,dpn.ch_actv_flg, logger)
            continue

        while not dpaQ.empty():
            ch = dpaQ.get()
            ch_list.append(ch)
            # check for RX 1 saturation
            if dpn.intrFlag_1:
                for ii in range(1,6):
                    ch_list.append(ii)
                flag = (dpn.activated_channels[ch_list[1]]) & (dpn.ch_actv_flg[ch_list[1]])
                print("saturation flag %d, global flag %d\n" %(flag, dpn.intrFlag_1))
                dpn.intrFlag_1=False

            # check for RX 2 saturation
            if dpn.intrFlag_2:
                for ii in range(6,11):
                    ch_list.append(ii)
                dpn.intrFlag_2=False

        #Check if the channel activation is in progress
        new_ch_list = Remove(ch_list)
        logger.info("Channels in dpaQ %s " %(new_ch_list))

        if len(dpn.actv_chl_in_progress) > 0:
            ch_list1 = returnNotMatches(new_ch_list, dpn.actv_chl_in_progress)
            new_ch_list = ch_list1[0]
            logger.info("Get rid of active, new Channel list --> %s" %(ch_list1))

        #Pass the channel list to dpaNotifier
        if (len(new_ch_list) or len(dpn.deactv_chl_in_progress)) > 0:
            logger.info('Channel activity flag list %s' %(dpn.ch_actv_flg))
            logger.info("[%s] Heartbeat ---> Active: %s Deactive: %s" %(datetime.now(),
                                new_ch_list, dpn.deactv_chl_in_progress))
            dpn.dpa_heartbeat(copy.copy(new_ch_list), dpn.deactv_chl_in_progress, dpn.ch_actv_flg, logger)
        else:
            logger.info("[%s] Heartbeat ---> alive" %(datetime.now()))
            dpn.dpa_heartbeat(0, 0, dpn.ch_actv_flg, logger)

        del ch_list[:]


#__________________________________________________________________#
def decode_pulse(wv_type, wv_data):
    position = 1
    detection_list=[]
    global wv_detection_dict
    global ch_detection_list

    del detection_list[:]
#wv_data contains [0,0,0,0,0,0,0,0,0,0,0]
#TODO: verify the index of results{}
    for itr in wv_data:
        #this position is obsolete
        if position == 11:
            detection_list.insert(11, 0)
        else:
            ch = int(itr)
            if ch==True:
                logger.info("[%s]{%s} Channel Detected: {%d}" %(datetime.now(), wv_type, position))
                ch_detection_list.append(position)
                val = Fore.RED + "1" + Style.RESET_ALL
                detection_list.insert(position, val)
            else:
                val = Fore.GREEN + "0" + Style.RESET_ALL
                detection_list.insert(position, val)
        position = position + 1


    table.add_row([wv_type, detection_list[1], detection_list[2],
                    detection_list[3], detection_list[4],
                    detection_list[5], detection_list[6],
                    detection_list[7], detection_list[8],
                    detection_list[9], detection_list[10]])


#__________________________________________________________________#
def rx_thread(arg):
    print("Rx_thread started() \n")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))

    print(UDP_IP, UDP_PORT)

    while True:
        data, addr = sock.recvfrom(1024)
        rx_queue.put(data)
#__________________________________________________________________#

def validate(val, data):
    if val in data:
        return "Invalid"
    else:
        return "Valid"

#__________________________________________________________________#

def check_for_valid_data(val1, data):
    val = bytes(val1, "utf-8")
    return validate(val,data)
#__________________________________________________________________#

def construct_json(data):
    new_data = data.decode()
    """
    pos_data = data.replace('DECLARE', '{"DECLARE":')
    obj_json = pos_data.replace('}', '} }')
    new_len = len(new_data)
    new_data = pos_data.replace('detector', '"detector')


    print obj_json[pos:]
    print "___________ \n"
    """
    pos = new_data.find("DECLARE")
    pos = pos + 8 #ignore DECLARE in message
    """
    print("_______________________\n")

    print(new_data[pos:])
    print("_______________________\n")
    """
    json_final = json.loads(new_data[pos:])

    return json_final


#__________________________________________________________________#

def fetch_block(data):
    json_final = construct_json(data)

# MESSAGE {"sensorId": "__unknown__", "type": "event", "name": "start"}
#    {u'DECLARE': {u'sensorId': 1, u'detector': u'pulse1', u'results': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], u'block': 1}}

    #print json_final['DECLARE']
    #print json_final['DECLARE']['results']

    #waveform_type = json_final['DECLARE']['detector']
    #data = json_final['DECLARE']['results']
    block_num = json_final['window']

    return block_num

#__________________________________________________________________#
def insert_channels_into_dpqQ(ch_list):
    for ch in ch_list:
        dpaQ.put(ch)

    del ch_list[:]
#__________________________________________________________________#

def insert_data_into_sqlQ(datalist, ch):
        sqlQ.put(datalist)
#__________________________________________________________________#
def decode_blocks(data_list):
    global ch_detection_list
    global conn, tbl
    new_ch_detection_list = []
    waveform_type=""
    det_list=[]
    datalist = []
    results_list=[]
    global fp

    del new_ch_detection_list[:]

#DECLARE {"sensorId": "12", "rfPath": "1", "detector": "chirp1", "window": "0x00000000", "results": [0,0,0,0,0,0,0,0,0,0,0]}
    for val in data_list:
        json = construct_json(val)
        waveform_type = json['detector']
        data = json['results']
        block = json['window']
        results_list.append(data)
        decode_pulse(waveform_type, data)

    sId = json["sensorId"]
    #To-Do ESC -> SAS
    #filter the duplicates from the list
    logger.info("\n\n")
    logger.info("Current detections ---> %s " %(ch_detection_list))
    #logger.info(ch_detection_list)
    #new_ch_detection_list = [item for item, count in collections.Counter(ch_detection_list).items() if count > 1]

    new_ch_detection_list = Remove(ch_detection_list)
    # |---D----A---| corner case fix
    if len(dpn.deactv_chl_in_progress) > 0:
        for ch in new_ch_detection_list:
            if ch in dpn.deactv_chl_in_progress:
                logger.info("---> Don't deactive, channel active %d" %(ch))
                dpn.deactv_chl_in_progress.remove(ch)

    if len(new_ch_detection_list)> 0:
        det_list = copy.copy(new_ch_detection_list)
    else:
        det_list = copy.copy(ch_detection_list)

    for ch in det_list:
        wv_detection_dict[ch] = (datetime.now() + timedelta(seconds=60))
        """
        logger.info("----$------------------------------------------0----\n")
        logger.info("Block: %s\n" %str(block))
        logger.info(str(datetime.now()) + "\n")
        logger.info("Activated Channel: %s\n" %str(ch))
        logger.info("Deactivationtime: %s\n" %str(wv_detection_dict[ch]))
        """
        datalist.insert(0, sId)
        datalist.insert(1, ch)
        datalist.insert(2, wv_detection_dict[ch])

        #logger.info("\n <----------- Detections found -------> \n")
        insert_data_into_sqlQ(copy.copy(datalist), ch)
        del datalist[:]

    logger.info("Detections after removing duplicates ----> %s" %(det_list))
    #logger.info(det_list)
    insert_channels_into_dpqQ(copy.copy(det_list))

    #if len(det_list)>0:
    #   print(table)

    del new_ch_detection_list[:]
    del ch_detection_list[:]
    del det_list[:]
    del results_list[:]
    print("\n\n")
    #print(wv_detection_dict)
    #print d.items()
    table.clear_rows()




#__________________________________________________________________#


def listener_thread(arg):
    print("listener_thread started() \n")
    current_block_num = 0
    prev_block_num = 0
    packet_counter = 0
    pool_data_list=[]
    data_l=[]
    chl_list=[]
    while True:
        if not rx_queue.empty():
            #get the data out of Queue
            data = rx_queue.get()
            #logger.info(data)
            new_data = data.decode()

            ret = new_data.find("DECLARE")
            if ret > 0:
                current_block_num = fetch_block(data)
                if(current_block_num != prev_block_num):
                    #print "Packet count: \n", packet_counter
                    pool_data_list.append(data)
                    packet_counter = packet_counter + 1
                    if packet_counter == 10:
                        prev_block_num = current_block_num
                        packet_counter = 0
                        #print pool_data_list
                        decode_blocks(pool_data_list)
                        del pool_data_list[:]

            else:
                del data_l[:]
                pos = new_data.find("SENSOR-HEALTH")
                if pos > 0:
                     pos = pos + 14
                     json_sh = json.loads(new_data[pos:])
                     if json_sh:
                         logger.info(json_sh)
                     dpn.intrFlag_1 = int(json_sh['RX1 Saturation'])
                     dpn.intrFlag_2 = int(json_sh['RX2 Saturation'])
                     if dpn.intrFlag_1:
                         logger.info("RX1 Saturation ON : %d\n" %(dpn.intrFlag_1))
                         for x in range(1,6):
                             data_l.insert(0, 0)   #sensorID
                             data_l.insert(1, x)   #channelID
                             timestamp = (datetime.now() + timedelta(seconds=60))
                             data_l.insert(2, timestamp) #timestamp
                             new_data1 = copy.copy(data_l)
                             insert_data_into_sqlQ(new_data1, x)
                             del data_l[:]
                             chl_list.append(x)

                         insert_channels_into_dpqQ(copy.copy(chl_list))
                         del chl_list[:]

                     if dpn.intrFlag_2:
                         logger.info("RX2 Saturation ON : %d\n" %(dpn.intrFlag_2))
                         for x in range(6,11):
                             data_l.insert(0, 0)   #sensorID
                             data_l.insert(1, x)   #channelID
                             timestamp = (datetime.now() + timedelta(seconds=60))
                             data_l.insert(2, timestamp) #timestamp
                             new_data2 = copy.copy(data_l)
                             insert_data_into_sqlQ(new_data2, x)
                             del data_l[:]
                             chl_list.append(x)

                         insert_channels_into_dpqQ(copy.copy(chl_list))
                         del chl_list[:]

#__________________________________________________________________#
#__________________________________________________________________#

def tracker(arg):
    print("Inside SQL thread\n")
    conn, tbl = sqlA.create_table()
    dataL =[None, None, None]
    ch_dict={}
    while True:
        if not sqlQ.empty():
            dataL = sqlQ.get()
            sqlA.update_data(conn, tbl, dataL[1]-1, "esc", dataL[1], dataL[2])
            ch_dict[dataL[1]]=dataL[2]
            #logger.info("[Channel %d] Activation time ----> %s\n" %( dataL[1], dataL[2]) )

        else:
            #print(ch_dict)
            for ch,timestamp in ch_dict.items():
                if timestamp == None:
                    continue
                if datetime.now() > timestamp:
                    dpn.ch_actv_flg[ch]=False
                    dpn.deactv_chl_in_progress.append(ch)
                    logger.critical("Timer expires for channel %d, %s -->" %(ch,timestamp))
                    #dpn.activated_channels[ch]=True
                    dpaQ.put(ch)
                    ch_dict[ch] = None

        if rl.reset_flag:
            print("Reset Flag: %d" %(rl.reset_flag))
            stop_log()
            start_log()
            rl.reset_flag=False


#__________________________________________________________________#
if __name__ == '__main__':
    reset_table()
    try:
        rx_worker = threading.Thread( target=rx_thread, args=("Thread-0",) )
        rx_worker.setDaemon(True)
        rx_worker.start()
        listner_worker = threading.Thread(target=listener_thread, args=("Thread-1",))
        listner_worker.setDaemon(True)
        listner_worker.start()
        t1 = threading.Thread( target=rl.rabbitmq_event, args=("Thread-2",))
        t2 = threading.Thread( target=tx_heartbeat, args=("Thread-3",30,))
        t3 = threading.Thread( target=tracker, args=("Thread-4",))
    except:
        print("Error: Unable to start thread")

    t1.setDaemon(True)
    t1.start()
    t2.setDaemon(True)
    t2.start()
    t3.setDaemon(True)
    t3.start()
    start_log()
    rx_worker.join()
    listner_worker.join()
    t1.join()
    t2.join()
    t3.join()
