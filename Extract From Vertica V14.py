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
from time import gmtime, strftime
from dateutil.rrule import rrule, DAILY, MONTHLY
import calendar
import csv
import sys
from multiprocessing import Pool, Process, current_process

def read_Sales_Data_Vertica (connV, ClusterName, siloID, datastore, vendorKey, retailerKey, dateList):

    conn = pyodbc.connect(connV.format(ClusterName))
                
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
                    FROM {}.OLAP_STORE_FACT_COMPUTED a,                                     \
                    {}.OLAP_PLAN_FACT b                                                     \
                    WHERE   a.VENDOR_KEY = b.VENDOR_KEY                                     \
                    AND a.RETAILER_KEY = b.RETAILER_KEY                                     \
                    AND a.STORE_KEY = b.STORE_KEY                                           \
                    AND a.ITEM_KEY = b.ITEM_KEY                                             \
                    AND a.PERIOD_KEY = b.PERIOD_KEY                                         \
                    AND a.PERIOD_KEY = {}                                                   \
                    AND a.RETAILER_KEY = {}                                                 \
                    AND a.VENDOR_KEY = {}                                                   \
                    AND a.STORE_KEY > 0                                                     \
                    AND a."Total Sales Amount" > 0    '


    dateList.sort()
    
    for periodKey in dateList: # loop through dates    

        day = periodKey % 100
        month = (periodKey % 10000)//100
               
        if (1 <= day <= 31) & (1 <= month <= 12): # run the rest if i is a valid date 
                        
            query = queryTemplate.format(siloID, siloID, periodKey, retailerKey, vendorKey)
        
            print("siloID:",siloID,"vendorKey:",vendorKey,"retailerKey:",retailerKey,"periodKey:",periodKey)
            
            SalesTS = pd.read_sql(query, conn)
                                            
            dateCols = ['FirstScanDate','LastScanDate','FirstPlanDate','LastPlanDate']                                
            dataCols = ['SalesAmount','SalesVolumeUnits','StoreOnHandVolumeUnits','StoreOutOfStockIndicator']
            indexCols = ['VendorKey','RetailerKey','PeriodKey','ItemKey','StoreKey']
            
            SalesTS[dateCols] = SalesTS[dateCols].fillna(0.0).astype(int)
            SalesTS[dataCols] = SalesTS[dataCols].fillna(0.0).astype(float)
            
            SalesTS.set_index(indexCols, inplace=True)  # create multi index                      
            SalesTS = SalesTS[~SalesTS.index.duplicated(keep='last')] # dedup if there are multiple entries on same day
 
            # Write to hdf5 file
            write_to_hdfs (datastore, 'Sales', SalesTS, 'a')
            
            # capture metadata
            meta = SalesTS.groupby(level=[0,1,2]).size()
            
            # write metadata
            write_to_hdfs (datastore, 'Metadata', meta, 'a')
           
    conn.close()
        
    return 0
    

def read_item_Data_Vertica (connV, datastore, ClusterName, siloID, vendorKey):
    
    conn = pyodbc.connect(connV.format(ClusterName))

    # extract item and description for particular vendor from vertica 
    
    queryTemplate = 'SELECT  VENDOR_KEY as VendorKey,   \
        ITEM_KEY as ItemKey,         \
        ITEM_GROUP as ItemGroup,                \
        CAST (UPC as VARCHAR) as UPC,           \
        ITEM_DESCRIPTION as ItemDescription     \
        FROM    {}.OLAP_ITEM                    \
        WHERE VENDOR_KEY = {}  '      

    query = queryTemplate.format(siloID, vendorKey)

    itemDim = pd.read_sql(query, conn)

    
    print ("Created ItemDim: ",itemDim.columns," for Silo:",siloID)
        
    # Write to hdf5 file
    write_to_hdfs (datastore, 'Items', itemDim, 'w')
    
    conn.close()
    
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



def get_active_campaign_range (connS, _campaignkey):

    conn = pyodbc.connect(connS)

    # get the boundaries of historical and campaign reporting periods over which data will be stored in silos
    
    periodquery = "execute POC.spCampaignReportingPeriods @campaignkey={0}, @precampaigndays={1}, @PostCampaignDays={2}".format(_campaignkey,3,14)

    period_df_ = pd.read_sql(periodquery, conn)

    PeriodBegin_ = period_df_ ['HistoryPeriodBegin'][0]
    PeriodEnd_ = period_df_ ['PostCampaignPeriodEnd'][0]

    conn.close()
    
    return PeriodBegin_, PeriodEnd_


def get_active_campaign_silos (connS, _periodBegin):

    # get current and future active campaigns and their vertica silos 
    conn = pyodbc.connect(connS)
    
    siloquery = "execute POC.spActiveCampaignSilos @DateThreshold={0}".format(_periodBegin)

    try:
        silo_df_ = pd.read_sql(siloquery, conn)
    except IOError as e:
        print (e)

    if silo_df_.empty:
        print ("#"*10,"No Active campaigns returned")
        
               
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
    
    for _periodKey in range(PeriodBegin, PeriodEnd+1): # loop through dates, include end date    

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


def execute_Vertica_subprocess (connV, connS, df, outDir):
    
    for i, row in df.iterrows(): # loop runs on a single silo in a single subprocess
            
            ClusterName = row["VerticaServerName"]
            SiloID = row['SiloID']
            HDFStoreName = os.path.join(outDir, SiloID) 
            VendorKey = row['VendorKey']
            RetailerKey = row['RetailerKey']
            CampaignKey = row['CampaignKey']
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
       

            return row["SiloID"], current_process().name, os.getpid()
        
#########################
# Main program
#########################

if __name__ == '__main__':

    periodBegin = 20180214
    outDir = "D:\Vertica"
    connV = "DRIVER=Vertica;SERVER={};DATABASE=FUSION;PORT=5433;UID=mallinath.biswas;PWD=BullSh!t9"
    connS = "Driver={SQL Server};Server=prodv2digops1.colo.retailsolutions.com;Database=DIGITAL_POC;UID=AnsaReports;PWD=AnsaReports@Rsi"    
    activeCampaigns_df = get_active_campaign_silos (connS, periodBegin)

    activeSilosList=activeCampaigns_df.SiloID.unique() # list of unique silos impacted by active campaigns
    
    pool = Pool() # multi process pool

    subProcessList = []
    
    for _siloID in activeSilosList: # iterate over unique silos - each iteration sets up a subprocess
        
        print ("\n","#"*50,"\n",'Subprocess ID:',_siloID,"\n","-"*50,"\n",)
                       
        activeCampaignsInSilo = activeCampaigns_df.loc[activeCampaigns_df['SiloID'] == _siloID] # get campaigns for given silo

#        subprocessname = "subprocess_"+siloname
        
        subprocessname = pool.apply_async(execute_Vertica_subprocess,[connV, connS, activeCampaignsInSilo, outDir])
        
        subProcessList.append(subprocessname)
    

    for subprocess in subProcessList: # iterate over unique silos - each iteration executes a subprocess
    
        # execute_Vertica_subprocess (activeCampaignsInSilo) 
        status = subprocess.get(timeout=3600)
        
        print ("Completed:",status)