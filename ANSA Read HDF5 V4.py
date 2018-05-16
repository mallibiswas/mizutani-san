# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 12:54:34 2017

@author: mallinath.biswas
"""

import pandas as pd
import os
import timeit
import pyodbc
import numpy as np
import datetime
import time

def read_hdfstore (_hdfstore, _database, _idnames, _where, _columns, _aggfunc):
    
    # read sakes data from hdf datastore

    _df = pd.DataFrame()
    
    i=0

    for chunk in pd.read_hdf(_hdfstore, _database, chunksize=CHUNKSIZE, where=_where, columns=_columns):
        chunk_smry_= chunk.groupby(level=_idnames, as_index=True).agg(_aggfunc)
        _df = pd.concat([_df, chunk_smry_])
        i+=1
        print ("Iteration:",i,"time:",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        _hdfstore,close()
        
    return _df



def get_campaign_sales_parameters (_vendorKey, _retailerKey, _itemKey, _campaignperiods_dict, _periodType):

    # setup parameters for each campaign needed to query hdf5 for historical and campaign data
    
    _idnames = ['VendorKey','RetailerKey','PeriodKey','ItemKey','StoreKey']
     
    #
    # _periodType = 'H' for History
    # _periodType = 'C' fpr Campaign
    #

    if _periodType == 'C':
        
        # Extract parameters for pre campaign begin to post campaign end 
        _periodBeginKey = _campaignperiods_dict ['PreCampaignPeriodBegin'][0]
        _periodEndKey = _campaignperiods_dict ['PostCampaignPeriodEnd'][0]
    
        
        _colnames = ['SalesAmount',
                    'SalesVolumeUnits',
                    'StoreOnHandVolumeUnits',
                    'StoreOutofStockIndicator',
                    'FirstScanDate',
                    'LastScanDate',
                    'FirstPlanDate',
                    'LastPlanDate']
                    
        
        _aggfunc = {'SalesAmount':'sum',
                    'SalesVolumeUnits':'sum',
                    'StoreOnHandVolumeUnits':'sum',
                    'StoreOutofStockIndicator':'sum',                
                    'FirstScanDate':'min',
                    'LastScanDate':'max',
                    'FirstPlanDate':'min',
                    'LastPlanDate':'max'}
        
    elif _periodType == 'H':

        # Extract history for 1 year + last years campaign period 
        _periodBeginKey = _campaignperiods_dict ['PrevYearCampaignPeriodBegin'][0]
        _periodEndKey = _campaignperiods_dict ['HistoryPeriodEnd'][0]
            
        _idnames = ['VendorKey','RetailerKey','PeriodKey','ItemKey','StoreKey']
        
        _colnames = ['SalesAmount',
                    'SalesVolumeUnits',
                    'FirstScanDate',
                    'LastScanDate']
                    
        _aggfunc = {'SalesAmount':'sum',
                    'SalesVolumeUnits':'sum',
                    'FirstScanDate':'min',
                    'LastScanDate':'max'}
    else:
        print ('invalid periodtype:',_periodType)
        return
            
    # construct query to filter on selected items
    # all items in campaign
    _where = "VendorKey == {0} & RetailerKey == {1} & PeriodKey >= {2} & PeriodKey <= {3} & ItemKey in {4}".format(_vendorKey,_retailerKey,_periodBeginKey,_periodEndKey,_itemKey)

    _colnames.extend(_idnames) # all the columns to be returned from hdfstore
    
    _colnames = _colnames # Columns list for all items
    _idnames = _idnames
    
    _all_dict = {'All':{'idnames':_idnames,'columns':_colnames,'where':_where,'aggfunc':_aggfunc}}    
    
    _colnames.remove('ItemKey') # Drop itemKey for Base dataset columns to be returned from hdfstore
    _idnames.remove('ItemKey') # Drop itemKey for Base dataset columns to be returned from hdfstore
    _where_base = "VendorKey == {0} & RetailerKey == {1} & PeriodKey >= {2} & PeriodKey <= {3}".format(_vendorKey,_retailerKey,_periodBeginKey,_periodEndKey)
    _idnames_base = _idnames
    _colnames_base = _colnames

    _base_dict = {'Base':{'idnames':_idnames,'columns':_colnames,'where':_where,'aggfunc':_aggfunc}}    

    _all_dict.update(_base_dict) # merge the two dicts for All and Base
    
    
    return _all_dict



def get_sales_data (hdfstore, database, _vendorKey, _retailerKey, _itemKey, _campaignperiods_dict, _periodType):

    # read data from hdf5 silos
    
    _parameters_dict = get_campaign_sales_parameters (_vendorKey, _retailerKey, _itemKey, _campaignperiods_dict, _periodType)
    
    _idnames = _parameters_dict['All']['idnames']
    _where = _parameters_dict['All']['where']
    _colnames = _parameters_dict['All']['columns']
    _aggfunc =  _parameters_dict['All']['aggfunc']
    
    _df_smry_all = read_hdfstore (hdfstore, database, _idnames, _where, _colnames, _aggfunc)

    _idnames = _parameters_dict['Base']['idnames']
    _where = _parameters_dict['Base']['where']
    _colnames = _parameters_dict['Base']['columns']
    _aggfunc =  _parameters_dict['Base']['aggfunc']

    
    _df_smry_base = read_hdfstore (hdfstore, database, _idnames, _where, _colnames, _aggfunc)

    _df_smry_base.reset_index(inplace=True)
    _df_smry_all.reset_index(inplace=True)
    
    _df_smry_base['ItemKey'] = 0 # Set ItemKey = 0 for base
    
    _df_smry = _df_smry_all.append(_df_smry_base)

    
    return _df_smry


def get_campaign_keys (conn, _campaignkey):
    
    # get the vendor/retailer, item-product-aggregate mapping and campaign lifecycle for particular campaign
    
    vendorquery = "select VendorKey, RetailerKey from DSS.Campaigns where campaignkey = {}".format(_campaignkey)    

    vendor_df_ = pd.read_sql(vendorquery, conn)
    
    productquery = "select ItemKey , ProductKey, AggregateKey from POC.CampaignProducts_Dim where campaignkey = {}".format(_campaignkey) 

    campaignproducts_df_ = pd.read_sql(productquery, conn)
    
    periodquery = "execute POC.spCampaignReportingPeriods @campaignkey={0}, @precampaigndays={1}, @PostCampaignDays={2}".format(_campaignkey,3,14)
    
    campaignperiods_df_ = pd.read_sql(periodquery, conn) 
    
    return vendor_df_, campaignproducts_df_, campaignperiods_df_





def build_CampaignMetricsDetails_Fact (_df, _campaignproducts_df, _CampaignKey):
    
    
    # Extract needed for active campaign period (pre campaign begin to post campaign end) 
    # for intra, end and post campaign details reporting
    
    # =============================================================================
    #
    #     Output
    #     CampaignKey
    #     PeriodKey
    #     StoreKey
    #     ItemKey
    #     ProductKey
    #     AggregateKey
    #     SalesAmount
    #     SalesVolumeUnits
    #     StoreOnHandVolumeUnits
    #     StoreOutOfStockIndictor
    #     FirstScanDate
    #     LastScanDate
    #     FirstPlanDate
    #     LastPlanDate
    #     UpdatedOn
    # 
    # =============================================================================
        

    df_ = _df.merge(_campaignproducts_df, on='ItemKey', how='inner')    
    
    df_['CampaignKey'] = _CampaignKey
    
    _colnames = ['CampaignKey','PeriodKey','StoreKey','ItemKey','ProductKey','AggregateKey',
             'SalesAmount','SalesVolumeUnits','StoreOnHandVolumeUnits','StoreOutOfStockIndicator',
             'FirstScanDate','LastScanDate','FirstPlanDate','LastPlanDate']

    df_ = df_ [_colnames]        
    
    return df_



def build_CampaignMetricsHistory_Fact (_df, _campaignproducts_df, _CampaignKey, _campaignperiods_dict):

    # Extract needed for active campaign period (365 day history begin to history end) 
    # for intra, end and post campaign details reporting
    
    #==============================================================================
    #     
    #     Output:
    #     CampaignKey
    #     PeriodBeginKey
    #     PeriodEndKey    
    #     StoreKey
    #     ItemKey
    #     FirstScanDate
    #     LastScanDate
    #     PrevYrCampaignPeriodSales
    #     PrevYrCampaignPeriodUnitsSold
    #     Prev365DaySales
    #     Prev365DayUnitsSold
    #     Prev182DaySales
    #     Prev182DayUnitsSold
    #     Prev91DaySales
    #     Prev91DayUnitsSold
    #     Prev28DaySales
    #     Prev28DayUnitsSold
    #     Prev1DaySales
    #     Prev1DayUnitsSold
    #     UpdatedOn
    #              
    #==============================================================================

    #    
    # Extract _HistoryPeriodBeginKey, _HistoryPeriodEndKey, _PrevYrCampaignBeginKey, _PrevYrCampaignEndKey
    # 
    
    _HistoryPeriodBeginKey = _campaignperiods_dict ['HistoryPeriodBegin'][0]
    _HistoryPeriodEndKey = _campaignperiods_dict ['HistoryPeriodEnd'][0]
    _PrevYrCampaignPeriodBeginKey = _campaignperiods_dict ['PrevYearCampaignPeriodBegin'][0]
    _PrevYrCampaignPeriodEndKey = _campaignperiods_dict ['PrevYearCampaignPeriodEnd'][0]
        
    _df = _df.reset_index()

    # Create derived columns    
    _df = _df.assign(PrevYrCampaignPeriodSales = lambda x: np.where((_df.PeriodKey >= _PrevYrCampaignPeriodBeginKey) & (_df.PeriodKey <= _PrevYrCampaignPeriodEndKey), _df.SalesAmount, 0),
                     Prev365DaySales = lambda x: np.where((_df.PeriodKey >= _HistoryPeriodBeginKey) & (_df.PeriodKey <= _HistoryPeriodEndKey), _df.SalesAmount, 0),
                     Prev182DaySales = lambda x: np.where((_df.PeriodKey >= _HistoryPeriodEndKey - 182) & (_df.PeriodKey <= _HistoryPeriodEndKey), _df.SalesAmount, 0),
                     Prev91DaySales = lambda x: np.where((_df.PeriodKey >= _HistoryPeriodEndKey - 91) & (_df.PeriodKey <= _HistoryPeriodEndKey), _df.SalesAmount, 0),
                     Prev28DaySales = lambda x: np.where((_df.PeriodKey >= _HistoryPeriodEndKey - 28) & (_df.PeriodKey <= _HistoryPeriodEndKey), _df.SalesAmount, 0),
                     Prev1DaySales = lambda x: np.where((_df.PeriodKey >= _HistoryPeriodEndKey - 1) & (_df.PeriodKey <= _HistoryPeriodEndKey), _df.SalesAmount, 0),
                     PrevYrCampaignPeriodUnitsSold = lambda x: np.where((_df.PeriodKey >= _PrevYrCampaignPeriodBeginKey) & (_df.PeriodKey <= _PrevYrCampaignPeriodEndKey), _df.SalesVolumeUnits, 0),
                     Prev365DayUnitsSold = lambda x: np.where((_df.PeriodKey >= _HistoryPeriodBeginKey) & (_df.PeriodKey <= _HistoryPeriodEndKey), _df.SalesVolumeUnits, 0),
                     Prev182DayUnitsSold = lambda x: np.where((_df.PeriodKey >= _HistoryPeriodEndKey - 182) & (_df.PeriodKey <= _HistoryPeriodEndKey), _df.SalesVolumeUnits, 0),
                     Prev91DayUnitsSold = lambda x: np.where((_df.PeriodKey >= _HistoryPeriodEndKey - 91) & (_df.PeriodKey <= _HistoryPeriodEndKey), _df.SalesVolumeUnits, 0),
                     Prev28DayUnitsSold = lambda x: np.where((_df.PeriodKey >= _HistoryPeriodEndKey - 28) & (_df.PeriodKey <= _HistoryPeriodEndKey), _df.SalesVolumeUnits, 0),
                     Prev1DayUnitsSold = lambda x: np.where((_df.PeriodKey >= _HistoryPeriodEndKey - 1) & (_df.PeriodKey <= _HistoryPeriodEndKey), _df.SalesVolumeUnits, 0)
                     )

    f = {'FirstScanDate':'min',
        'LastScanDate':'max',
        'Prev182DaySales':'sum',
        'Prev182DayUnitsSold':'sum',
        'Prev1DaySales':'sum',
        'Prev1DayUnitsSold':'sum',
        'Prev28DaySales':'sum',
        'Prev28DayUnitsSold':'sum',
        'Prev365DaySales':'sum',
        'Prev365DayUnitsSold':'sum',
        'Prev91DaySales':'sum',
        'Prev91DayUnitsSold':'sum',
        'PrevYrCampaignPeriodSales':'sum',
        'PrevYrCampaignPeriodUnitsSold':'sum'
        }
    
     
    df_ = _df.groupby(['StoreKey','ItemKey']).agg(f)    

    df_ = df_.reset_index()    
 
    _df_ = df_.merge(_campaignproducts_df, on='ItemKey', how='inner')    
    
    _df_['CampaignKey'] = _CampaignKey
    _df_['PeriodBeginKey'] = _HistoryPeriodBeginKey 
    _df_['PeriodEndKey'] = _HistoryPeriodEndKey
    
    return _df_

#
#   Main
#

outDir = "Y:\Data"

# will be encrypted and read from config.json
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=prodv2digops1.colo.retailsolutions.com;'
                        'Database=DIGITAL_POC;'
                        'UID=AnsaReports;'
                        'PWD=AnsaReports@Rsi')

HDFStoreName = os.path.join(outDir, 'ULEVER_WALGRN_V1.h5') 

###
### TEST CASES:L 1279, 1284, 1302
###

# Constants
CampaignKey = 1279
CHUNKSIZE = 10**7


# get keys needed from campaign metadata
vendor_df, campaignproducts_df, campaignperiods_df = get_campaign_keys (conn, CampaignKey)

# Extract relevant campaign parameters from the database
VendorKey = vendor_df['VendorKey'][0]
RetailerKey = vendor_df ['RetailerKey'][0]
ItemKey = campaignproducts_df['ItemKey'].tolist()
CampaignPeriods_dict = campaignperiods_df.to_dict()

# Extract raw sales data from HDF5 based on campaign metadata
database = 'Sales'

# Extract data starting from prev yr campaign begin date to current year pre campaign begin date
hdf0 = get_sales_data (HDFStoreName, database, VendorKey, RetailerKey, ItemKey, CampaignPeriods_dict, 'H') # H: History

# Summarize for historical metrics
dfH = build_CampaignMetricsHistory_Fact (hdf0, campaignproducts_df, CampaignKey, CampaignPeriods_dict)

hdf1 = get_sales_data (HDFStoreName, database, VendorKey, RetailerKey, ItemKey, CampaignPeriods_dict, 'C') # C: Campaign

# Summarize for historical metrics
dfC = build_CampaignMetricsDetails_Fact (hdf0, campaignproducts_df, CampaignKey)

print (dfC.head(20))
print (dfC.info())
print (dfC.tail(20))

print (campaignperiods_df)
    