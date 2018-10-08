#!/usr/bin/env python3
import pycurl, json
import collections
from collections import defaultdict
from datetime import datetime
import re


low_freq=[0, 3550, 3560, 3570, 3580, 3590, 3600, 3610, 3620, 3630, 3640]
high_freq=[0, 3560, 3570, 3580, 3590, 3600, 3610, 3620, 3630, 3640, 3650]

intrFlag_1=False
intrFlag_2=False

activated_channels={0:False, 1:False, 2:False, 3:False, 4:False, 5:False, 6:False, 7:False, 8:False, 9:False, 10:False}
ch_actv_flg={0:True, 1:True, 2:True, 3:True, 4:True, 5:True, 6:True, 7:True, 8:True, 9:True, 10:True}


actv_chl_in_progress=[]
deactv_chl_in_progress=[]

def returnNotMatches(a, b):
    a = set(a)
    b = set(b)
    return [list(a - b), list(b - a)]




def Remove(duplicate):
    final_list = []
    for num in duplicate:
        if num not in final_list:
            final_list.append(num)
    return final_list


def dpa_heartbeat(actv_chlist, deactv_chlist, dpa_dict, logger):
    global intrFlag_1
    global intrFlag_2
    new_data = {"Activation": True, "Lower_Freq": 0, "Upper_Freq": 0, "type": "Detection"}
    post_data = {}
    skip=False
    c = pycurl.Curl()
    c.setopt(pycurl.URL, 'http://127.0.0.1:8081/heartbeat/')
    c.setopt(pycurl.HTTPHEADER, ['Content-Type: application/json' ,'Accept: application/json'])


    logger.info(">>>>>>>>>>>>>>>>>>>> dpaNotifier <<<<<<<<<<<<<<<<<<<< \n")
    if actv_chlist == 0 and deactv_chlist == 0:
        data = json.dumps({"type": "dpaNotification",
            "message": {
                "dpa": [] },
            "status": "alive"
            })

    else:

        #Check the active and de-active list
        updated_list = returnNotMatches(actv_chlist, deactv_chlist)
        actv_chlist  = updated_list[0]
        deactv_chlist = updated_list[1]

        logger.info("dpaNotifier [Active]--> %s" %(actv_chlist))
        logger.info("dpaNotifier [De-Active]--> %s" %(deactv_chlist))

       # --> Set active flag
        if len(actv_chlist) > 0:
            for ch in actv_chlist:
                new_data["Activation"] = True
                new_data["Lower_Freq"] = low_freq[ch]
                new_data["Upper_Freq"] = high_freq[ch]
                new_data["type"] = "detection"
                #Activate the channel progress
                actv_chl_in_progress.append(ch)

                post_data = json.dumps(post_data) + json.dumps(new_data)
                #channel active/in-active
                if activated_channels[ch]:
                    activated_channels[ch] = False
                    ch_actv_flg[ch] = True

        # --> Set deactive flag
        deactv_chlist = Remove(deactv_chlist)
        if len(deactv_chlist) > 0:
            for ch in deactv_chlist:
                new_data["Activation"] = False
                new_data["Lower_Freq"] = low_freq[ch]
                new_data["Upper_Freq"] = high_freq[ch]
                new_data["type"] = "detection"
                #Activate the channel progress
                deactv_chl_in_progress.remove(ch)
                actv_chl_in_progress.remove(ch)

                post_data = json.dumps(post_data) + json.dumps(new_data)


        data = json.dumps({"type": "dpaNotification",
                "message":  {
                        "dpa": [
                            post_data
                            ]
                        },
                "DPAid":"1"
                })

    new_data=str(data)
    new_data = new_data.replace("\\", "")
    #dump = json.dumps(data, indent=4)
    #tabs = re.sub('\n +', lambda match: '\n' + '\t' * (len(match.group().strip('\n')) / 2), dump)
    #logger.info(tabs)
    logger.info(json.loads(new_data))
    logger.info("-->-------------------------------------------------<--\n")

    """
    print("==================================================================== \n")
    print("\t [%s] [->SAS]: HEARTBEAT \n" %(datetime.now()))
    print("==================================================================== \n")
    """
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDS, data)
    c.setopt(pycurl.VERBOSE, 0)
    c.perform()
    c.close()
