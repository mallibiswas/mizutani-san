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
import os
from scipy.stats import ttest_ind
#import datetime


def read_Source_Data(conn):


    # Read all campaign periods based on latest data from DSS.Campaigns
    # Call proc to identify campaign reporting periods
    # CREATE PROCEDURE [POC].[spCampaignReportingPeriods]   
    # @CampaignKey int,
    # @PreCampaignDays int,
    # @PostCampaignDays int

    MetaDataQuery="execute [POC].[spCampaignReportingPeriods]  @CampaignKey=1067, @PreCampaignDays=3, @PostCampaignDays=14;"  

    _meta = pd.read_sql(MetaDataQuery, conn) # get the reporting periods

    _ReportingPeriodBegin = _meta['CampaignPeriodBegin'][0]
    _ReportingPeriodEnd = _meta['CampaignPeriodEnd'][0]
    
    #
    # Call proc to return data from hist and campiagn details tables
    # PROCEDURE [POC].[spEndCampaignReports]   
    #   @CampaignKey int,
    #   @ReportingPeriodBegin int,
    #   @ReportingPeriodEnd int
    #
    
    DataQuery="execute POC.spCampaignReports @CampaignKey=1067, @ReportingPeriodBegin={0}, @ReportingPeriodEnd={1};".format(_ReportingPeriodBegin, _ReportingPeriodEnd)
    
    _df = pd.read_sql(DataQuery, conn)

    df_ = pd.merge(_df, _meta, on='CampaignKey', how='inner')
    
    # convert period keys to dates
    df_['CampaignPeriodBegin'] = pd.to_datetime(df_['CampaignPeriodBegin'].astype(str), format='%Y%m%d')
    df_['CampaignPeriodEnd'] = pd.to_datetime(df_['CampaignPeriodEnd'].astype(str), format='%Y%m%d')
    df_['HistoryPeriodBegin'] = pd.to_datetime(df_['HistoryPeriodBegin'].astype(str), format='%Y%m%d')
    df_['HistoryPeriodEnd'] = pd.to_datetime(df_['HistoryPeriodEnd'].astype(str), format='%Y%m%d')
    df_['HistoryLastScanDate'] = pd.to_datetime(df_['HistoryLastScanDate'].astype(str), format='%Y%m%d')
    df_['HistoryFirstScanDate'] = pd.to_datetime(df_['HistoryFirstScanDate'].astype(str), format='%Y%m%d')
    df_['CampaignFirstScanDate'] = pd.to_datetime(df_['CampaignFirstScanDate'].astype(str), format='%Y%m%d')
    df_['CampaignLastScanDate'] = pd.to_datetime(df_['CampaignLastScanDate'].astype(str), format='%Y%m%d')
    
    return _ReportingPeriodBegin, _ReportingPeriodEnd, df_
    

def summarize_dataframe (_df, _idnames, _aggfunc):
    
    # Helper function to summarize data over a list of cols (idnames) using a dictionary (aggfunc)
    
    df_smry_ = _df.groupby(_idnames).agg(_aggfunc)        
    
    return df_smry_
    

def reshape_dataframe(_df, _aggKeyName):

    # Module: Product Performance
    # This module preps data and produces df at the approproate aggregation level
    # _aggKeyName is the list over which aggregation is to be done
    
    # identify cols to drop
    dropcols = ['ItemKey','ProductKey','AggregateKey'] # List of all prossible aggregation levels
    
    dropcols.remove(_aggKeyName[0])

    _df = _df.drop(dropcols, axis=1) # drop keys not needed for requested aggreagation level
    
    f = {'CampaignSales':'sum',
        'CampaignUnits':'sum',
        'HistorySales':'sum',
        'HistoryUnits':'sum',
        'CampaignFirstScanDate':'min',
        'CampaignLastScanDate':'max',
        'HistoryFirstScanDate':'min',
        'HistoryLastScanDate':'max'
        }

    idnames_ = ['CampaignKey','ClusterKey','ClusterName','StoreKey','CampaignPeriodBegin','CampaignPeriodEnd','HistoryPeriodBegin','HistoryPeriodEnd','AggregateName']
    idnames_.append(_aggKeyName[0])
    
    _df0 = summarize_dataframe (_df, idnames_, f)

    _df0 = _df0.reset_index()
    
#   Calculate scan days and take min of scan days vs reporting period
#
#
    # Calculate number of days in campaign period and scan periods, same for history
    _df0['CampaignPeriodLength'] = _df0['CampaignPeriodEnd'] - _df0['CampaignPeriodBegin'] + datetime.timedelta(days=1) # Campaign period includes begin and end dates

    _df0['CampaignScanLength'] = _df0['CampaignLastScanDate'] - _df0['CampaignFirstScanDate']
    _df0['HistoryPeriodLength'] = _df0['HistoryPeriodEnd'] - _df0['HistoryPeriodBegin']
    _df0['HistoryScanLength'] = _df0['HistoryLastScanDate'] - _df0['HistoryFirstScanDate']

    # Set the scan days to minimum of number of days in campaign period and scan periods, same for history
    _df0['CampaignScanDays'] = np.minimum(_df0['CampaignPeriodLength'],_df0['CampaignScanLength'])
    _df0['HistoryScanDays'] = np.minimum(_df0['HistoryPeriodLength'],_df0['HistoryScanLength'])
        
    # Derive avg daily sales    
    _df0 ['CampaignSalesRate'] = _df0 ['CampaignSales'].div(_df0 ['CampaignScanDays'].dt.days)
    _df0 ['CampaignUnitsRate'] = _df0 ['CampaignUnits'].div(_df0 ['CampaignScanDays'].dt.days)
    _df0 ['HistorySalesRate'] = _df0 ['HistorySales'].div(_df0 ['HistoryScanDays'].dt.days)
    _df0 ['HistoryUnitsRate'] = _df0 ['HistoryUnits'].div(_df0 ['HistoryScanDays'].dt.days)

    # Split and merge to convenient shape with Base as a Column
    _df1 = _df0[_df0.AggregateName != 'Base'] # Halo and Featured dataframe
    _df2 = _df0[_df0.AggregateName == 'Base'] # Base dataframe

    _df2 = _df2.copy()
    
    _df2.rename(columns={'CampaignSales': 'BaseCampaignSales', 
                        'CampaignSalesRate': 'BaseCampaignSalesRate',
                        'CampaignUnitsRate': 'BaseCampaignUnitsRate',
                        'CampaignUnits': 'BaseCampaignUnits',
                        'HistorySales':'BaseHistorySales',
                        'HistorySalesRate':'BaseHistorySalesRate',
                        'HistoryUnits':'BaseHistoryUnits',
                        'HistoryUnitsRate':'BaseHistoryUnitsRate'}, inplace = True)

    dropcols_ = _aggKeyName + ['ClusterName'] 
    
    _df2.drop(dropcols_, axis=1, inplace=True)

    idnames_ = ['CampaignKey','ClusterKey','StoreKey']
    
    df2_ = summarize_dataframe (_df2, idnames_, sum)

    df1_ = _df1.reset_index()
    df2_ = df2_.reset_index()
        
    df_ = pd.merge(df1_, df2_, on=idnames_, how='inner')

    # Calculate Normalized Sales at Store and product level
    
    # Note: use .div(to handle divide by zero) - will return inf instead of DIV/0
    
    # Base Normalized = Campaign to date:  raw Sales / Base Sales 
    df_['BaseNormalizedSales'] = df_['CampaignSales'].div(df_['BaseCampaignSales'])
    
    # History Normalized = Campaign to date: raw Sales / Historical Sales Rate
    df_['HistoryNormalizedSales'] = df_['CampaignSales'].div(df_['HistorySalesRate'])

    # Two Factor Normalized = (Campaign to date BN Sales / Historical Raw Sales) * Campaign to date Base Raw Sales 
    df_['TwoFactorNormalizedSales'] = df_['BaseNormalizedSales']*df_['BaseCampaignSales'].div(df_['HistorySales'])

    # Step 2: Calculate Normalized Unit Sales at Store and product level

    # Base Normalized = Campaign Product Sales / Base Sales
    df_['BaseNormalizedUnits'] = df_['CampaignUnits'].div(df_['BaseCampaignUnits'])
    
    # History Normalized = Campaign Product Sales / Historical Sales
    df_['HistoryNormalizedUnits'] = df_['CampaignUnits'].div(df_['HistoryUnitsRate'])

    # Two Factor Normalized = Base Normalized Sales * Base Historical Sales / Historical Sales
    df_['TwoFactorNormalizedUnits'] = df_['BaseNormalizedUnits']*df_['BaseCampaignUnits'].div(df_['HistoryUnits'])

    df_.drop(['CampaignPeriodBegin','CampaignPeriodEnd',\
              'HistoryPeriodBegin','HistoryPeriodEnd','AggregateName',\
              'HistoryLastScanDate','HistoryFirstScanDate',\
              'CampaignFirstScanDate','CampaignLastScanDate',\
              'CampaignPeriodLength','CampaignScanLength','CampaignScanDays',\
              'HistoryPeriodLength','HistoryScanLength','HistoryScanDays'], axis=1, inplace=True) # drop cols not needed any further
                     
    return df_


def assign_methodologies (_df, _idnames):

    # Module: Product Performance


    df_ = pd.melt(_df, id_vars=_idnames, var_name='VariableName', value_name='NormalizedSales')

    # Tag the CalculationMethod
    df_.loc[df_['VariableName'].str.contains('BaseNormalized'),'CalculationMethod'] = 'Base Normalized'
    df_.loc[df_['VariableName'].str.contains('HistoryNormalized'),'CalculationMethod'] = 'History Normalized'
    df_.loc[df_['VariableName'].str.contains('TwoFactorNormalized'),'CalculationMethod'] = 'Two Factor Normalized'
    df_.loc[df_['VariableName'].str.contains('CampaignSales|CampaignUnits'),'CalculationMethod'] = 'Campaign Raw'

    # Assign CalculationMethod Keys
    df_['CalculationMethodKey'] = df_['CalculationMethod'].map(CalculationMethod_dict)

    # Tag the measure type
    df_.loc[df_['VariableName'].str.contains('Sales'),'Measure'] = 'Sales'
    df_.loc[df_['VariableName'].str.contains('Units'),'Measure'] = 'Units'


    df_.drop(['VariableName','ClusterKey'], axis=1, inplace=True)

    
    return df_


def pivot_on_cluster_names (_df, _idnames):
    
    # Module: Product Performance
       
    # Helper function
    df_ = _df.pivot_table(index=_idnames, columns='ClusterName', aggfunc=np.sum)

    return df_    


def calculate_PValues (_df, _aggKeyName):
    
    # Module: Product Performance
        
    idnames = ['CampaignKey','ClusterKey','ClusterName','StoreKey'] + _aggKeyName
    
    varnames = ['BaseNormalizedSales','BaseNormalizedUnits',
                'HistoryNormalizedSales','HistoryNormalizedUnits',
                'TwoFactorNormalizedSales','TwoFactorNormalizedUnits']
                
    colnames = idnames + varnames

    _df = _df [colnames]
    
    _df_ = assign_methodologies (_df, idnames)
    
    idnames_ = ['CampaignKey','CalculationMethodKey','Measure']
    idnames_.append(_aggKeyName[0])
    idnames_.append('StoreKey')
    _df_pivoted = pivot_on_cluster_names (_df_, idnames_)            
    
    # iterate through dataframe, for each product and CalculationMethod pass a "T" and a "C" list to ANOVA
    campaigns = _df_pivoted.index.get_level_values('CampaignKey').unique().tolist() # = Campaign
    methodologies = _df_pivoted.index.get_level_values('CalculationMethodKey').unique().tolist() # = Methodologies
    productkeys = _df_pivoted.index.get_level_values(_aggKeyName[0]).unique().tolist() # = Products
    measures = _df_pivoted.index.get_level_values('Measure').unique().tolist() # = Sales and Units

    _df_ = _df_pivoted.reset_index()
    
    result = {'TStatistic':[]}

    for cc in campaigns:    
        for mm in methodologies:
            for pp in productkeys:
                for m in measures:
                    
                    # create T and C lists            
                    Control_list = _df_.loc[(_df_['CalculationMethodKey'] == mm) & (_df_[_aggKeyName[0]] == pp) & (_df_['Measure'] == m) ][('NormalizedSales', 'C')]
                    Test_list = _df_.loc[(_df_['CalculationMethodKey'] == mm) & (_df_[_aggKeyName[0]] == pp) & (_df_['Measure'] == m) ][('NormalizedSales', 'T')]
                    
                    # Calculate the T-test for the means of TWO INDEPENDENT samples of scores.
                    tStats = ttest_ind(Control_list, Test_list, axis=0, equal_var=False, nan_policy='omit') # returns (tstat , pvalue)                
                    _result = {'CampaignKey':cc, _aggKeyName[0]:pp, 'CalculationMethodKey':mm, 'Measure':m, 'TStatistic':tStats[0], 'PValue':tStats[1]/2} # p value returned is for 2-tailed dist, so div by 2
                    
                    # append to null dictionary
                    result['TStatistic'].append(_result)
                
    df_ = pd.DataFrame(result['TStatistic'])   # Convert list of dictionaries into dataframe             

    
    return df_


def calculate_metrics (_df, _aggKeyName):

    # Module: Product Performance

    # replace inf with 0 to avoid impacting sums over normalized sales
    _df = _df.replace([np.inf, -np.inf], np.nan).fillna(0) 

    # Extract Summary dataset

    f = {'CampaignSales': 'sum',
        'CampaignUnits':'sum',
        'BaseNormalizedSales': 'sum',
        'HistoryNormalizedSales': 'sum',
        'TwoFactorNormalizedSales': 'sum',
        'BaseNormalizedUnits': 'sum',
        'HistoryNormalizedUnits': 'sum',
        'TwoFactorNormalizedUnits': 'sum',
        'StoreKey':'nunique'
      }
     
    idnames_ = ['CampaignKey','ClusterKey','ClusterName']
    idnames_.append(_aggKeyName[0])
    
    _df_smry = summarize_dataframe (_df, idnames_, f)
        
    _df_smry.rename(columns={'StoreKey':'StoreCount'}, inplace=True) # Note: Storecounts give unique scanned stores

    _df_smry = _df_smry.reset_index()
    
    idnames_.append('StoreCount')

    _df_reshaped = assign_methodologies (_df_smry, idnames_)
    
    # Calculate Sample means
    _df_reshaped['Mean'] = _df_reshaped['NormalizedSales'].div(_df_reshaped['StoreCount'])
     
    
    return _df_reshaped
    

def calculate_KPIs (_df, _aggKeyName):

    # Module: Product Performance

    #    Metrics to calculate: 
    #    TCAS, Incremental Sales, RSAD, RSPD, Lift (%)

    # Step 0: Summarize data to the level needed
    idnames_ = ['CampaignKey','CalculationMethodKey']
        
    idnames_.extend([_aggKeyName[0],'Measure'])
    _df_smry = pivot_on_cluster_names (_df, idnames_)

    _df_smry['LM']=_df_smry[('Mean','T')]/_df_smry[('Mean','C')]
 
    
    # Step 1: Split dataframe into overall Campaign Sales and Normalized Sales dataframes 
    normalized_df =  _df_smry.iloc[_df_smry.index.get_level_values('CalculationMethodKey') != 1] # i.e. CalculationMethod !=  'Campaign Raw'

    raw_df = _df_smry.xs(1, axis=0, level='CalculationMethodKey') # Equivalent to:    raw_df = _df_smry.xs('Campaign Raw', axis=0, level='CalculationMethod')
    raw_df_ = raw_df.copy()

    # Step 2: Derive KPIs        
    raw_df_ ['TCAS'] = raw_df_[('NormalizedSales','T')]
    raw_df_ ['RSAD'] = (raw_df_[('Mean','T')] - raw_df_[('Mean','C')]) * raw_df_[('StoreCount','T')]
    raw_df_ ['RSPD'] = 100*(raw_df_[('NormalizedSales','T')]/raw_df_[('StoreCount','T')] * raw_df_[('StoreCount','C')]/raw_df_[('NormalizedSales','C')] - 1)

    # Step 3: Clean up variables not needed any more
    raw_df_.drop(['StoreCount','NormalizedSales','Mean','LM'], axis=1, inplace=True)
    raw_df_.columns = raw_df_.columns.droplevel(1)
    
    # Step 4: Clean up Normalized_df, drop TvC level
    normalized_df_ = normalized_df.copy()
    normalized_df_.drop(['StoreCount','NormalizedSales','Mean'], axis=1, inplace=True)
    normalized_df_.columns = normalized_df_.columns.droplevel(1)
    
    # Step 5: Merge cleaned up Normalized and Raw Sales dfs
    normalized_df_ = normalized_df_.reset_index()
    raw_df_ = raw_df_.reset_index()
    
    # Merge on CampaignKey, ProductKey, Measure
    _idnames =  ['CampaignKey']
    _idnames.extend([_aggKeyName[0],'Measure'])
    df_ = normalized_df_.merge(raw_df_,  on=_idnames, how='inner')
    df_ ['IS']   = df_['TCAS']*(1-1/df_['LM'])
       
    return df_


def merge_all_KPIs (_df, _pv, _aggKeyName):

    # Module: Product Performance

    # Merge on CampaignKey, Calc Method, Measure and selected aggregation key to assemble all metrics in one df
    _idnames = ['CampaignKey','CalculationMethodKey','Measure']
    _idnames.insert(1,_aggKeyName[0])
    
    df_ = _df.merge(_pv,  on=_idnames, how='inner')

    return df_


def calculate_store_summary (_df, _aggKeyName):
    
    # Module: Store Performance
        
    # Extract Summary dataset

    # dict to store aggregation functions
    f = {'CampaignSales': 'sum',
        'CampaignUnits': 'sum',
        'HistorySales': 'sum',
        'HistoryUnits': 'sum',        
        'CampaignSalesRate': 'sum',
        'CampaignUnitsRate': 'sum',
        'HistorySalesRate': 'sum',
        'HistoryUnitsRate': 'sum'        }
     
    # dict to store renamed column names
    r = {'CampaignSales': 'TotalCampaignSales',
        'CampaignUnits': 'TotalCampaignUnits',
        'HistorySales': 'TotalHistorySales',
        'HistoryUnits': 'TotalHistoryUnits',        
        'CampaignSalesRate': 'TotalCampaignSalesRate',
        'CampaignUnitsRate': 'TotalCampaignUnitsRate',
        'HistorySalesRate': 'TotalHistorySalesRate',
        'HistoryUnitsRate': 'TotalHistoryUnitsRate'                }     
     
    _idnames = ['CampaignKey','ClusterKey']
    _idnames.extend([_aggKeyName[0],'StoreKey']) # summarize over keys only
    
    # retain only keys and necessary columns in original dataframe
    _df_orig = summarize_dataframe (_df, _idnames, f)

    # add to dictionaries to apply to summary dfs
    f['StoreKey'] = 'nunique' 
    r['StoreKey'] = 'StoreCount'
    
    # Create summary dataframe and rename
    _idnames.remove('StoreKey')
    
    _df_smry = summarize_dataframe (_df, _idnames, f)
    _df_smry.rename(columns=r, inplace=True) # Note: Storecounts give unique scanned stores

    # merge original and summary dataframes
    df1_ = _df_smry.reset_index()
    df2_ = _df_orig.reset_index()
    
    _df = pd.merge(df1_, df2_, on=_idnames, how='inner')

    return _df
    


def calculate_SAI_SPR (_df):

    # Module: Store Performance
    
    # Sales Amount Index per Store  = 100 * Campaign to Date Sales in Store / Campaign to Date Avg. Sales Per Store
    # tolerance = tau induced to avoid divide by zero errors

    _df['SAI'] = 100 * _df['StoreCount'] * _df['CampaignSales'].div(_df['TotalCampaignSales'])

    #Store Performance Ratio = (Campaign to Date Sales/Day)/(Sales/Day Previous 52 Weeks)
    _df['SPR'] = _df['CampaignSalesRate'].div(_df['HistorySalesRate'])

    # retain useful columns only
    dropcols = ['CampaignSales','CampaignUnits',
                'HistorySales','HistoryUnits',
                'CampaignSalesRate','CampaignUnitsRate',
                'HistorySalesRate','HistoryUnitsRate',                
                'TotalHistorySales','TotalCampaignUnits',
                'TotalHistoryUnits','TotalCampaignSales',
                'TotalHistorySalesRate','TotalCampaignUnitsRate',
                'TotalHistoryUnitsRate','TotalCampaignSalesRate',
                'StoreCount']
    
    df_ = _df.drop (dropcols, axis=1)

    return df_


def output_to_csv (_PeriodBegin, _PeriodEnd, _df, _aggKeyName, _outDir, _filename):
    
    
    _outfile = os.path.join(_outDir, _filename)   # variable list and labels 

    _df.insert (4,'ReportingPeriodBegin',_PeriodBegin)
    _df.insert (5,'ReportingPeriodEnd',_PeriodEnd)
    

    _df = _df.rename(columns={_aggKeyName[0]: 'ReportKey'})
    
    # Assign Aggregation Levels
    _df['AggregationLevel'] = AggregationLevel_dict.get(_aggKeyName[0])

    _df.to_csv  (_outfile)
    
    print ('written to ', _outfile)
    
    return

#########################
# Main program
#########################

if __name__ == '__main__':

    # set output directory
    outDir="D:/POC/Export/"

    # will be encrypted and read from config.json
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=prodv2digops1.colo.retailsolutions.com;'
                            'Database=DIGITAL_POC;'
                            'UID=AnsaReports;'
                            'PWD=AnsaReports@Rsi')
    
#    ReportingPeriodBegin, ReportingPeriodEnd, source_df = read_Source_Data(conn)          # Read source data and lists into dictionary
    
    # Pass different list of attributes to summarize on    
    # For UPC level reports
    Item_Cols = ['ItemKey']
    # For SubAggregate level reports
    SubAgg_Cols = ['ProductKey']
    # For Aggregate level reports
    Agg_Cols = ['AggregateKey']
    
    ##
    ## Global dictionaries to map variables and df values
    ##
    
    # Map Aggregate Key to Aggregation Level:
    AggregationLevel_dict ={'ItemKey':1,
                            'ProductKey':2,
                            'AggregateKey':3}

    # Map Calculation Methods to the calculation metod keys (DSS.LookupValues.lookupkey):
    CalculationMethod_dict ={'Campaign Raw':1,
                            'Base Normalized':2,
                            'History Normalized':3,
                            'Two Factor Normalized':4}

    ##
    ## item Level Reports
    ##
#    df01 = reshape_dataframe (source_df, Item_Cols) # Split and merge Base dataset
#    PV1 = calculate_PValues (df01, Item_Cols) # Call routine to calculate P Values
#    df1 = calculate_metrics (df01, Item_Cols)
#    df1 = calculate_KPIs (df1, Item_Cols)
#    df1 = merge_all_KPIs (df1, PV1, Item_Cols)
#    df1s = calculate_store_summary (df01, Item_Cols)
#    df1s = calculate_SAI_SPR (df1s)
#    print (PV1.head()) # product performance
#    print (df01.tail(),"\n",'^^'*20) # product performance
#    print (df1.tail()) # product performance
#    print (df1s.tail()) # store performance
    
    ##
    ## SubAggregate Level Reports
    ##
    df02 = reshape_dataframe (source_df, SubAgg_Cols) # Split and merge Base dataset
    PV2 = calculate_PValues (df02, SubAgg_Cols) # Call routine to calculate P Values
    df2 = calculate_metrics (df02, SubAgg_Cols)
    df2 = calculate_KPIs (df2, SubAgg_Cols)
    df2 = merge_all_KPIs (df2, PV2, SubAgg_Cols)
    df2s = calculate_store_summary (df02, SubAgg_Cols)
    df2s = calculate_SAI_SPR (df2s)
#    print (PV2.head()) # product performance
#    print (df02.head()) # product performance
#    print (df2.head(20)) # product performance
#    print (df2s.info()) # product performance
    print (df2s.tail()) # store performance
    
    ##
    ## Aggregate Level Reports
    ##
#    df03 = reshape_dataframe (source_df, Agg_Cols) # Split and merge Base dataset
#    PV3 = calculate_PValues (df03, Agg_Cols) # Call routine to calculate P Values
#    df3 = calculate_metrics (df03, Agg_Cols)
#    df3 = calculate_KPIs (df3, Agg_Cols)
#    df3 = merge_all_KPIs (df3, PV3, Agg_Cols)
#    df3s = calculate_store_summary (df03, Agg_Cols)
#    df3s = calculate_SAI_SPR (df3s)   
#    print (PV3.head()) # product performance
#    print (df03.head()) # product performance
#    print (df3.head()) # product performance
#    print (df3s.tail()) # store performance
    
    outFile="EndCampaignbyProduct.csv"    
    output_to_csv (ReportingPeriodBegin, ReportingPeriodEnd, df2, SubAgg_Cols, outDir, outFile)