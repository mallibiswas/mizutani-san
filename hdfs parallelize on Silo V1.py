# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 13:02:33 2017

@author: mallinath.biswas
"""
import os
import pyodbc
import pandas as pd
# datetime functions
from datetime import datetime
from datetime import date
#from time import gmtime, strftime
import time
from dateutil.rrule import rrule, DAILY, MONTHLY
import calendar
import csv
import sys
from multiprocessing import Pool, Process, current_process
#import Initialize


def read_Sales_Data_Vertica (connV, ClusterName, siloID, datastore, vendorKey, retailerKey, dateList):

    
    # extract daily sales data from vertica for particular vendor/retailer combination
    
    queryTemplate = 'SELECT  a.VENDOR_KEY as VendorKey,                                     \
                    a.RETAILER_KEY as RetailerKey,                                          \
                    a.PERIOD_KEY as PeriodKey,                                              \
                    a.ITEM_KEY as ItemKey,                                                  \
                    a.STORE_KEY as StoreKey,                                                \
                    a."Total Sales Amount Non-Negative" as SalesAmount,                     \
                    a."Total Sales Volume Units Non-Negative" as SalesVolumeUnits,          \
                    a."Store On Hand Volume Units Non-Negative" as StoreOnHandVolumeUnits,  \
                    a."Store Out of Stock Indicator" as StoreOutOfStockIndicator,           \
                    b."First Scan Date" as FirstScanDate,                                   \
                    b."Last Scan Date" as LastScanDate,                                     \
                    b."First Plan Date" as FirstPlanDate,                                   \
                    b."Last Plan Date" as LastPlanDate                                      \
                    FROM {0}.OLAP_STORE_FACT_COMPUTED a,                                     \
                    {1}.OLAP_PLAN_FACT b                                                     \
                    WHERE   a.VENDOR_KEY = b.VENDOR_KEY                                     \
                    AND a.RETAILER_KEY = b.RETAILER_KEY                                     \
                    AND a.STORE_KEY = b.STORE_KEY                                           \
                    AND a.ITEM_KEY = b.ITEM_KEY                                             \
                    AND a.PERIOD_KEY = b.PERIOD_KEY                                         \
                    AND a.PERIOD_KEY >= {2}                                                   \
                    AND a.PERIOD_KEY <= {3}                                                   \
                    AND a.RETAILER_KEY = {4}                                                 \
                    AND a.VENDOR_KEY = {5}                                                   \
                    AND a.STORE_KEY > 0                                                     \
                    AND a."Total Sales Amount" > 0    '
    
    SalesTS = pd.DataFrame()
    meta = pd.DataFrame()
    dateList.sort()
    seq = iter(dateList)
    
    for periodKey in seq: # loop through dates    

        try:
            periodKeyNext = next(seq)
        except StopIteration:
            periodKeyNext = periodKey
        except Exception as e:    
            print (e)
            raise
        finally:
            query = queryTemplate.format(siloID, siloID, periodKey, periodKeyNext, retailerKey, vendorKey)
            print("siloID:",siloID,"vendorKey:",vendorKey,"retailerKey:",retailerKey,"periodKey:",periodKey,"-",periodKeyNext,"runtime:",datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            
            SalesTS_ = pd.read_sql(query, connV)
                                            
            dateCols = ['FirstScanDate','LastScanDate','FirstPlanDate','LastPlanDate']                                
            dataCols = ['SalesAmount','SalesVolumeUnits','StoreOnHandVolumeUnits','StoreOutOfStockIndicator']
            indexCols = ['VendorKey','RetailerKey','PeriodKey','ItemKey','StoreKey']
            
            SalesTS_[dateCols] = SalesTS_[dateCols].fillna(0.0).astype(int)
            SalesTS_[dataCols] = SalesTS_[dataCols].fillna(0.0).astype(float)
            
            SalesTS_.set_index(indexCols, inplace=True)  # create multi index                      
            _SalesTS = SalesTS_[~SalesTS_.index.duplicated(keep='last')] # dedup if there are multiple entries on same day
     
#            SalesTS = SalesTS.append(_SalesTS)
            
            # capture metadata
            meta_ = _SalesTS.groupby(level=[0,1,2]).size().reset_index(name='counts')
    
#            meta = meta.append(meta_)


            # Write to hdf5 file
            write_to_hdfs (datastore, 'Sales', _SalesTS, 'a')
            
            # write metadata
            write_to_hdfs (datastore, 'Metadata', meta_, 'a')
        
        
    return 0
    

def read_item_Data_Vertica (connV, datastore, ClusterName, siloID, vendorKey):
    

    # extract item and description for particular vendor from vertica 
    
    queryTemplate = 'SELECT  VENDOR_KEY as VendorKey,       \
                    ITEM_KEY as ItemKey,                    \
                    ITEM_GROUP as ItemGroup,                \
                    CAST (UPC as VARCHAR) as UPC,           \
                    ITEM_DESCRIPTION as ItemDescription     \
                    FROM    {}.OLAP_ITEM                    \
                    WHERE VENDOR_KEY = {}  '      

    query = queryTemplate.format(siloID, vendorKey)

    itemDim = pd.read_sql(query, connV)

    
    print ("Created ItemDim: ",itemDim.columns," for Silo:",siloID)
        
    # Write to hdf5 file
    write_to_hdfs (datastore, 'Items', itemDim, 'w')
    
#    conn.close()
    
    return 0


def write_to_hdfs (datastore, database, df, mode):
    
    #   write data fetched from vertica into silos, create if missing
    #   mode : 
    #
    #   'w' Write; a new file is created (an existing file with the same name would be deleted).
    #   'a' Append; an existing file is opened for reading and writing, and if the file does not exist it is created.
    #   'r+' similar to 'a', but the file must already exist.    
    #    
    HDFStore = pd.HDFStore(datastore)

    databasekey = "/"+database
    
    if mode == 'a':
        try:
            HDFStore.append(database, df, format='table', append=True, data_columns=True) # append      
        except Exception as e:    
            print (e)
            raise
        finally:
            HDFStore.close() # close the file
    elif mode == 'w':            
        try:
            HDFStore.put(database, df, format='table', append=False, data_columns=True)    
        except Exception as e:    
            print (e)
            raise
        finally:
            HDFStore.close() # close the file
    else:
        print ("#"*10,"Error is in mode:", mode)
               
        HDFStore.close() # close the file
               
    return



def get_active_campaign_range (conn, _campaignkey):

    # get the boundaries of historical and campaign reporting periods over which data will be stored in silos
    
    periodquery = "execute POC.spCampaignReportingPeriods @campaignkey={0}, @precampaigndays={1}, @PostCampaignDays={2}".format(_campaignkey,3,14)

    period_df_ = pd.read_sql(periodquery, conn)

    PeriodBegin_ = period_df_ ['HistoryPeriodBegin'][0]
    PeriodEnd_ = period_df_ ['PostCampaignPeriodEnd'][0]

#    conn.close()
    
    return PeriodBegin_, PeriodEnd_


def get_active_campaign_silos (conn, _periodBegin):

    # get current and future active campaigns and their vertica silos 
    conn = pyodbc.connect(connS)
    
    siloquery = "execute POC.spActiveCampaignSilos @DateThreshold={0}".format(_periodBegin)

    try:
        silo_df_ = pd.read_sql(siloquery, conn)
    except IOError as e:
        print (e)

    if silo_df_.empty:
        print ("#"*10,"No Active campaigns returned")
    else:
        print ("Active Silos:\n",silo_df_)
               
    conn.close()
           
    return silo_df_


def get_metadata_periodkey_list (datastore, _vendorKey, _retailerKey):
    
    # Get list of days already in hdf silo for particular vendor and retailer

    hdf = pd.HDFStore(datastore)
    
    _where = "VendorKey={0} & RetailerKey={1}".format(_vendorKey, _retailerKey)
 
    try:
        _ds = pd.read_hdf(hdf, 'Metadata',where=[_where]) # returns a series
        _PeriodKeyList = _ds.index.get_level_values('PeriodKey').values
        PeriodKeyList = list(sorted(set(_PeriodKeyList))) 
    except KeyError as k:
        print ('Metadata Schema does not exist:',k)
        PeriodKeyList = []
    except Exception as e:
        print(e,"\n","Error in get_metadata_periodkey_list: ", datastore) 
        raise
    finally:
        hdf.close()
    
    return PeriodKeyList # return unique sorted list



def get_valid_dates (PeriodBegin, PeriodEnd, datastore, VendorKey, RetailerKey):

    # get list of days for which sales dats is needed for a particular campaign
    
    _dateList = []    
    today = datetime.fromtimestamp(time.time()).strftime('%Y%m%d')    
    maxDate = min(PeriodEnd,int(today))
    
    for _periodKey in range(PeriodBegin, maxDate): # loop through dates, include end date    

        day = _periodKey % 100
        month = (_periodKey % 10000)//100
               
        if (1 <= day <= 31) & (1 <= month <= 12): # run the rest if i is a valid date 
            
            _dateList.append(_periodKey)            

    # get dates in metadata for this hdfstore, if it exists 
    if os.path.isfile(datastore):
        
        PeriodKeyList = get_metadata_periodkey_list (datastore, VendorKey, RetailerKey)
        
        _dateList = [n for n in _dateList if n not in PeriodKeyList] # return this list for incremental date load
        
        _dupes = set([x for x in PeriodKeyList if PeriodKeyList.count(x) > 1]) # find duplicate entries in metadata
        
        if len(_dupes) > 0: # check for duplicates
            print ("\n","-"*20,"Found Multiple date entries in Metadata for:",_dupes)
            
        if len(PeriodKeyList) > 0:
            print ("Found dates in metadata between:",min(PeriodKeyList)," and ",max(PeriodKeyList))
            
        print ("periodkeylist:",PeriodKeyList)
        print ("Resulting ",len(_dateList)," Dates between:",min(_dateList)," and ",max(_dateList))
        
    print ("-"*20,"\n","Campaign Begin Date:",PeriodBegin,"End Date:",PeriodEnd,"datastore:",datastore,"VendorKey:",VendorKey,"RetailerKey:",RetailerKey)
        
    return _dateList


def execute_Vertica_subprocess (connV_, connS_, df, outDir, siloID):

    
    for i, row in df.iterrows(): # loop runs on a single silo in a single subprocess
            
            ClusterName = row["VerticaServerName"]
            SiloID = row['SiloID']
            HDFStoreName = os.path.join(outDir, SiloID) 
            VendorKey = row['VendorKey']
            RetailerKey = row['RetailerKey']
            CampaignKey = row['CampaignKey']
            
            connV = pyodbc.connect(connV_.format(ClusterName))
            connS = pyodbc.connect(connS_)
    
            PeriodBegin, PeriodEnd = get_active_campaign_range (connS, CampaignKey)
            dateList = get_valid_dates (PeriodBegin, PeriodEnd, HDFStoreName, VendorKey, RetailerKey)
                
            # Setup cluster specific Vertica connection    
            SalesTimeSeries = read_Sales_Data_Vertica ( connV = connV,
                                                        ClusterName=ClusterName,
                                                        siloID = SiloID,
                                                        datastore = HDFStoreName,
                                                        vendorKey = VendorKey,                                     
                                                        retailerKey = RetailerKey,
                                                        dateList = dateList)
    
    # When all the dates are in, replace the metadata                 
    ItmDim = read_item_Data_Vertica (connV = connV,
                                     ClusterName=ClusterName, 
                                     siloID = SiloID, 
                                     vendorKey = VendorKey,
                                     datastore = HDFStoreName)
   
    connV.close()
    connS.close()
    
    print("Completed Subprocess:",os.getpid())    
    
    return 


def test_Vertica_subprocess (connV_, connS_, df, outDir, siloID):

    print (connV_, connS_, df, outDir, siloID)
    print("Completed Subprocess:",os.getpid())    
    
    return 

#########################
# Main program
#########################

if __name__ == '__main__':

    print ("Start:",datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    
    # initialize variables
#    Initialize.readConfigFile('C:\\Users\\mallinath.biswas\\Documents\\Python Scripts\\config.json', '13fd5<ca%0ec97cf1d|1b7fz')

    periodBegin = 20180101
    timeout = 30 # timeout for each subprocess to fetch vertica data
    
    outDir = 'D:\Vertica'
    connV = "DRIVER=Vertica;SERVER={{}};DATABASE={0};PORT=5433;UID={1};PWD={2}".format('FUSION', 'mallinath.biswas', 'BullSh!t9')
    connS = "Driver={{SQL Server}};Server={0};Database={1};UID={2};PWD={3}".format('prodv2digops1.colo.retailsolutions.com','DIGITAL_POC','AnsaReports','AnsaReports@Rsi')    
    
    activeCampaigns_df = get_active_campaign_silos (connS, periodBegin)

    activeSilosList=activeCampaigns_df.SiloID.unique() # list of unique silos impacted by active campaigns

#    execute_Vertica_subprocess(connV, connS, activeCampaigns_df, outDir)
    
    print ("End:",datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    
    pool = Pool(4) # multi process pool

    resultList = []
    
    for _siloID in activeSilosList: # iterate over unique silos - each iteration sets up a subprocess
        
        print ("\n","#"*50,"\n",'Subprocess ID:',_siloID,"\n","-"*50,"\n",)
        activeCampaignsInSilo = activeCampaigns_df.loc[activeCampaigns_df['SiloID'] == _siloID] # get campaigns for given silo
        
        try:                   
            result = pool.apply_async(execute_Vertica_subprocess,(connV, connS, activeCampaignsInSilo, outDir, _siloID,))
            resultList.append(result)
        except KeyboardInterrupt:
            # Allow ^C to interrupt from any thread.
            sys.stdout.write('\033[0m')
            sys.stdout.write('User Interupt\n')        
        
        
#        print(result.get(timeout=timeout))        
    for r in resultList:
        print(r.get())        
        print ("End:",datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    
#        subprocessname = Process(target=execute_Vertica_subprocess, args=(connV, connS, activeCampaignsInSilo, outDir))
#        subprocessname.start()
#    result = pool.apply_async(doubler, (25,))
        
#        subProcessList.append(subprocessname)
        
     
#    for subprocess in subProcessList: # iterate over unique silos - each iteration executes a subprocess
    
        # execute_Vertica_subprocess (activeCampaignsInSilo) 
#        status = subprocess.get(timeout=timeout)
#        subprocess.join()        
