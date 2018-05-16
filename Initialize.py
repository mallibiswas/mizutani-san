# -*- coding: utf-8 -*-
"""
Created on Thu Dec 21 09:44:32 2017

@author: mallinath.biswas
"""

import requests, json, getpass, csv, time, datetime
import os
import sys
import shutil
import subprocess
import glob
import hashlib
import pymssql
from Crypto.Cipher import AES
import base64


def readConfigFile():

# And also set global variables

    global  sourceUser, sourcePwd, targetUser, targetPwd, sourceServer, \
            targetServer, sourceDB, targetDB, current_datetime, dataDirectory
            
    current_datetime = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    
    if len(sys.argv) < 3:
        print ("Invalid Arguments")
        print ("usage: python main.py <config file> <noitpyrced yek>")
        print ("ex. python main.py config.json 123456")
        sys.exit()

    # load and read the configuration file
    configFile = sys.argv[1]
    
    with open(configFile) as f:
        configs = json.load(f)
        
        # Source DB parameters
        sourceServer_ = configs["sourceDBparams"]["server"]
        targetServer_ = configs["targetDBparams"]["server"]
        sourceDB_ = configs["sourceDBparams"]["database"]
        targetDB_ = configs["targetDBparams"]["database"]
        sourceUser_ = configs["sourceDBparams"]["user"]
        targetUser_ = configs["targetDBparams"]["user"]
        sourcePwd_ = configs["sourceDBparams"]["password"]
        targetPwd_ = configs["targetDBparams"]["password"]

        dataDirectory = configs["dataDirectory"]
#        dictFile = configs["dictFile"]
#        setupFileShare = configs["setupFileShare"]
        
    cipher = AES.new(sys.argv[2],AES.MODE_ECB) # never use ECB in strong systems obviously

    # decrypt variables and convert binary to ASCII	
    sourceServer = cipher.decrypt(base64.b64decode(sourceServer_)).strip().decode('ascii')
    targetServer = cipher.decrypt(base64.b64decode(targetServer_)).strip().decode('ascii')
    sourceDB = cipher.decrypt(base64.b64decode(sourceDB_)).strip().decode('ascii')
    targetDB = cipher.decrypt(base64.b64decode(targetDB_)).strip().decode('ascii')
    sourceUser = cipher.decrypt(base64.b64decode(sourceUser_)).strip().decode('ascii')
    targetUser = cipher.decrypt(base64.b64decode(targetUser_)).strip().decode('ascii')
    sourcePwd = cipher.decrypt(base64.b64decode(sourcePwd_)).strip().decode('ascii')
    targetPwd = cipher.decrypt(base64.b64decode(targetPwd_)).strip().decode('ascii')
 
    
    return {"sourceServer":sourceServer, 
            "targetServer":targetServer,
            "sourceDB":sourceDB,
            "targetDB":targetDB,
            "sourceUser":sourceUser,
            "targetUser":targetUser,
            "sourcePwd":sourcePwd,
            "targetPwd":targetPwd,
           "dataDirectory":dataDirectory}

# execute
#readConfigFile()
    
