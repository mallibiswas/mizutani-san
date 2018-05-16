# -*- coding: utf-8 -*-
#
# Note: Run using Python 3.x
#

#   Let Python load it's ODBC connecting tool pypyodbc
import pyodbc
#   Let Python load it's datetime functions
import datetime
#
import csv
import numpy
import pandas as pd
import numpy as np

def readSourceData(conn):

    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=prodv2digops1.colo.retailsolutions.com;'
                            'Database=DIGITAL_POC;'
                            'UID=AnsaReports;'
                            'PWD=AnsaReports@Rsi')
    
    #
    query="execute POC.spEndCampaignReports @CampaignKey=1067;"
    
    # read query output into dataframe
    df_ = pd.read_sql(query, conn)
    
    return df_


def storeSummary (_df, _aggList):
    
    # Module: Store Performance
        
    # Extract Summary dataset

    # dict to store aggregation functions
    f = {'CampaignSalesRate': 'sum',
        'CampaignUnitsRate': 'sum',
        'HistorySalesRate': 'sum',
        'HistoryUnitsRate': 'sum'        
        }
     
    # dict to store renamed column names
    r = {'CampaignSalesRate': 'TotalCampaignSales',
        'CampaignUnitsRate': 'TotalCampaignUnits',
        'HistorySalesRate': 'TotalHistorySales',
        'HistoryUnitsRate': 'TotalHistoryUnits'        
        }     
     
    idnames = ['CampaignKey','ClusterKey'] + [_aggList[0]] # summarize over keys only
    
    # retain only keys and necessary columns in original dataframe
    _df_orig = _df.groupby(idnames + ['StoreKey']).agg(f)    
    
    # Create summary dataframe and rename
    f['StoreKey'] = 'nunique' 
    r['StoreKey'] = 'StoreCount' 
    _df_smry = _df.groupby(idnames).agg(f)    
    _df_smry.rename(columns=r, inplace=True) # Note: Storecounts give unique scanned stores


    # merge original and summary dataframes
    df1_ = _df_smry.reset_index()
    df2_ = _df_orig.reset_index()
    
    _df = pd.merge(df1_, df2_, on=['CampaignKey','ClusterKey',_aggList[0]])

    return _df
    


def calculate_SAI_SPR (_df):

    # Module: Store Performance
    
    # Sales Amount Index per Store  = 100 * Campaign to Date Sales in Store / Campaign to Date Avg. Sales Per Store
    # tolerance = tau induced to avoid divide by zero errors

    _df['SalesAmountIndex'] = 100 * _df['CampaignSalesRate'].div(_df['TotalCampaignSales'].div(_df['StoreCount']))

    #Store Performance Ratio = (Campaign to Date Sales/Day)/(Sales/Day Previous 52 Weeks)
    _df['SalesPerformanceRatio'] = _df['CampaignSalesRate'].div(_df['HistorySalesRate'])

    # retain useful columns only
    dropcols = ['CampaignSalesRate','CampaignUnitsRate','HistorySalesRate','HistoryUnitsRate','TotalHistorySales','TotalCampaignUnits','TotalHistoryUnits','TotalCampaignSales','StoreCount']
    
    df_ = _df.drop (dropcols, axis=1)

    return df_

conn = 'need to setup'

# set output directory
outDir="D:/POC/Export/"
outFile="EndCampaignbyProductType.csv"

# Pass different list of attributes to summarize on    
# For SubAggregate level reports
SubAgg_Cols = ['ProductKey','SubAggregateName']
# For Aggregate level reports
Agg_Cols = ['AggregateKey','AggregateName']

df0 = readSourceData(conn)          # Read source data and lists into dictionary


# Sub Aggregate reports
df1s = storeSummary (df0, SubAgg_Cols)
df1 = calculate_SAI_SPR (df1s)

# Aggregate reports
#df2s = storeSummary (df0, Agg_Cols)
#df2 = calculate_SAI_SPR (df2s)

print (df1.info())
print (df1.head(10))
