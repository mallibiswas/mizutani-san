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
from scipy.stats import ttest_ind

# set output directory
outDir="D:/POC/Export/"
outFile="EndCampaignbyProductType.csv"

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
    

def reShapeDF(_df, _subAggList, _aggList):

    # Module: Product Performance
    # _aggList is the list over which aggregation is to be done

    
    # Split and merge to convenient shape with Base as a Column
    df1 = _df[_df.AggregateName != 'Base']
    df2 = _df[_df.AggregateName == 'Base']
    
    #print (df1.head())
    #print (df2.info())
    df2_ = df2.copy()
    
    df2_.rename(columns={'CampaignSalesRate': 'BaseCampaignSalesRate', 'CampaignUnitsRate': 'BaseCampaignUnitsRate','HistorySalesRate':'BaseHistorySalesRate','HistoryUnitsRate':'BaseHistoryUnitsRate'}, inplace = True)

    df2_.drop(_aggList, axis=1, inplace=True)
    df2_.drop(_subAggList, axis=1, inplace=True)
        
    df_ = pd.merge(df1, df2_, on=['CampaignKey','ClusterKey','StoreKey','ClusterName'])
    
    # Calculate Normalized Sales at Store and product level
    
    # Note: use .div( to handle divide by zero)
    
    # Base Normalized = Campaign Product Sales / Base Sales
    df_['BaseNormalizedSales'] = df_['CampaignSalesRate'].div(df_['BaseCampaignSalesRate'])
    
    # History Normalized = Campaign Product Sales / Historical Sales
    df_['HistoryNormalizedSales'] = df_['CampaignSalesRate'].div(df_['HistorySalesRate'])

    # Two Factor Normalized = Base Normalized Sales * Base Historical Sales / Historical Sales
    df_['TwoFactorNormalizedSales'] = df_['BaseNormalizedSales']*df_['BaseHistorySalesRate'].div(df_['HistorySalesRate'])


    # Step 2: Calculate Normalized Unit Sales at Store and product level

    # Base Normalized = Campaign Product Sales / Base Sales
    df_['BaseNormalizedUnits'] = df_['CampaignUnitsRate'].div(df_['BaseCampaignUnitsRate'])
    
    # History Normalized = Campaign Product Sales / Historical Sales
    df_['HistoryNormalizedUnits'] = df_['CampaignUnitsRate'].div(df_['HistoryUnitsRate'])

    # Two Factor Normalized = Base Normalized Sales * Base Historical Sales / Historical Sales
    df_['TwoFactorNormalizedUnits'] = df_['BaseNormalizedUnits']*df_['BaseHistoryUnitsRate'].div(df_['HistoryUnitsRate'])

    return df_


def reshapeMethodologies (_df, _idnames):

    # Module: Product Performance

    _df_melted = pd.melt(_df, id_vars=_idnames, var_name='VariableName', value_name='NormalizedSales')

    # Tag the methodology
    _df_melted.loc[_df_melted['VariableName'].str.contains('BaseNormalized'),'Methodology'] = 'Base Normalized'
    _df_melted.loc[_df_melted['VariableName'].str.contains('HistoryNormalized'),'Methodology'] = 'History Normalized'
    _df_melted.loc[_df_melted['VariableName'].str.contains('TwoFactorNormalized'),'Methodology'] = 'Two Factor Normalized'
    _df_melted.loc[_df_melted['VariableName'].str.contains('CampaignSalesRate|CampaignUnitsRate'),'Methodology'] = 'Campaign Raw'

    # Assign Methodology Keys
    _df_melted.loc[_df_melted['Methodology'] == 'Campaign Raw', 'MethodologyKey'] = 1   
    _df_melted.loc[_df_melted['Methodology'] == 'Base Normalized', 'MethodologyKey'] = 2   
    _df_melted.loc[_df_melted['Methodology'] == 'History Normalized', 'MethodologyKey'] = 3   
    _df_melted.loc[_df_melted['Methodology'] == 'Two Factor Normalized', 'MethodologyKey'] = 4   

    # Tag the measure type
    _df_melted.loc[_df_melted['VariableName'].str.contains('Sales'),'Measure'] = 'Sales'
    _df_melted.loc[_df_melted['VariableName'].str.contains('Units'),'Measure'] = 'Units'

    _df_melted.drop(['VariableName','ClusterKey'], axis=1, inplace=True)

    
    return _df_melted


def pivotOnClustersName (_df, _idnames):
    
    # Module: Product Performance
    _df_pivoted = _df.pivot_table(index=_idnames, columns='ClusterName', aggfunc=np.sum)
    
    return _df_pivoted    


def calculatePValues (_df, _aggList):
    
    # Module: Product Performance
        
    idnames = ['CampaignKey','ClusterKey','ClusterName','StoreKey'] + _aggList
    varnames = ['BaseNormalizedSales','BaseNormalizedUnits','HistoryNormalizedSales','HistoryNormalizedUnits','TwoFactorNormalizedSales','TwoFactorNormalizedUnits']
    colnames = idnames + varnames

    _df = _df [colnames]
    
    _df_ = reshapeMethodologies (_df, idnames)
    
    idnames = ['CampaignKey','MethodologyKey','Methodology','Measure'] + _aggList + ['StoreKey']
    _df_pivoted = pivotOnClustersName (_df_, idnames)            
    
    # iterate through dataframe, for each product and methodology pass a "T" and a "C" list to ANOVA
    campaigns = _df_pivoted.index.get_level_values('CampaignKey').unique().tolist() # = Campaign
    methodologies = _df_pivoted.index.get_level_values('MethodologyKey').unique().tolist() # = Methodologies
    productkeys = _df_pivoted.index.get_level_values(_aggList[0]).unique().tolist() # = Products
    measures = _df_pivoted.index.get_level_values('Measure').unique().tolist() # = Sales and Units

    _df_ = _df_pivoted.reset_index()     
    
    result = {'T-Statistic':[]}

    for cc in campaigns:    
        for mm in methodologies:
            for pp in productkeys:
                for m in measures:
                    # create T and C lists            
                    Control_list = _df_.loc[(_df_['MethodologyKey'] == mm) & (_df_[_aggList[0]] == pp) & (_df_['Measure'] == m) ][('NormalizedSales', 'C')]
                    Test_list = _df_.loc[(_df_['MethodologyKey'] == mm) & (_df_[_aggList[0]] == pp) & (_df_['Measure'] == m) ][('NormalizedSales', 'T')]
                    # Calculate the T-test for the means of TWO INDEPENDENT samples of scores.
                    tStats = ttest_ind(Control_list, Test_list, nan_policy='omit') # returns (tstat , pvalue)                
                    _result = {'CampaignKey':cc, _aggList[0]:pp, 'MethodologyKey':mm, 'Measure':m, 'TStatistic':tStats[0], 'P-Value':tStats[1]}
                    # append to null dictionary
                    result['T-Statistic'].append(_result)
                
    df_ = pd.DataFrame(result['T-Statistic'])   # Convert list of dictionaries into dataframe             

    
    return df_


def calculateMetrics (_df, _aggList):

    # Module: Product Performance

    # _aggList is the list over which aggregation is to be done

    # tolerance = tau, used to avoid divivde by zero errors

    # Extract Summary dataset

    f = {'CampaignSalesRate': 'sum',
        'CampaignUnitsRate': 'sum',
        'BaseNormalizedSales': 'sum',
        'HistoryNormalizedSales': 'sum',
        'TwoFactorNormalizedSales': 'sum',
        'BaseNormalizedUnits': 'sum',
        'HistoryNormalizedUnits': 'sum',
        'TwoFactorNormalizedUnits': 'sum',
       'StoreKey':'nunique'
      }
     
    idnames = ['CampaignKey','ClusterKey','ClusterName'] + _aggList
    _df_smry = _df.groupby(idnames).agg(f)    
    _df_smry.rename(columns={'StoreKey':'StoreCount'}, inplace=True) # Note: Storecounts give unique scanned stores

    _df_smry = _df_smry.reset_index()
    
    idnames = ['CampaignKey','ClusterKey','ClusterName','StoreCount'] + _aggList

    _df_reshaped = reshapeMethodologies (_df_smry, idnames)
    
    # Calculate Sample means
    _df_reshaped['Mean'] = _df_reshaped['NormalizedSales'].div(_df_reshaped['StoreCount'])
     
    
    return _df_reshaped
    

def calculateKPIs (_df, _aggList):

    # Module: Product Performance

    #    Metrics to calculate: 
    #    TCAS, Incremental Sales, RSAD, RSPD, Lift (%)

    # Step 0: Summarize data to the level needed
    idnames = ['CampaignKey','MethodologyKey'] + [_aggList[0]] + ['Measure']
    _df_smry = pivotOnClustersName (_df, idnames)

    _df_smry['LiftMultiplier']=_df_smry[('Mean','T')]/_df_smry[('Mean','C')]
        
    
    # Step 1: Split dataframe into overall Campaign Sales and Normalized Sales dataframes 
    normalized_df =  _df_smry.iloc[_df_smry.index.get_level_values('MethodologyKey') != 1] # i.e. Methodology !=  'Campaign Raw'

    raw_df = _df_smry.xs(1, axis=0, level='MethodologyKey') # Equivalent to:    raw_df = _df_smry.xs('Campaign Raw', axis=0, level='Methodology')
    raw_df_ = raw_df.copy()

    # Step 2: Dervice KPIs        
    raw_df_ ["TCAS"] = raw_df_[('NormalizedSales','T')]
    raw_df_ ["RSAD"] = raw_df_[('Mean','T')] - raw_df_[('Mean','C')]
    raw_df_ ["RSPD"] = 100*(raw_df_[('Mean','T')] - raw_df_[('Mean','C')])/raw_df_[('Mean','C')]

    # Step 3: Clean up variables not needed any more
    raw_df_.drop(['StoreCount','NormalizedSales','Mean','LiftMultiplier'], axis=1, inplace=True)
    raw_df_.columns = raw_df_.columns.droplevel(1)
    
    # Step 4: Clean up Normalized_df, drop TvC level
    normalized_df_ = normalized_df.copy()
    normalized_df_.drop(['StoreCount','NormalizedSales','Mean'], axis=1, inplace=True)
    normalized_df_.columns = normalized_df_.columns.droplevel(1)
    
    # Step 5: Merge cleaned up Normalized and Raw Sales dfs
    normalized_df_ = normalized_df_.reset_index()
    raw_df_ = raw_df_.reset_index()
    
    # Merge on CampaignKey, ProductKey, Measure
    df_ = normalized_df_.merge(raw_df_,  on=['CampaignKey']+[_aggList[0]]+['Measure'], how='inner')
    df_ ["IS"] = df_['TCAS']*(1-1/df_['LiftMultiplier'])
        
    return df_


def outputKPIs (_df, _pv, _aggList):

    # Module: Product Performance

    # Merge on CampaignKey, ProductKey, Measure
    df_ = _df.merge(_pv,  on=['CampaignKey']+[_aggList[0]]+['MethodologyKey','Measure'], how='inner')
    
    return df_


#
#   Main
#

conn = 'need to setup'


df = readSourceData(conn)          # Read source data and lists into dictionary


# Pass different list of attributes to summarize on    
# For SubAggregate level reports
SubAgg_Cols = ['ProductKey','SubAggregateName']
# For Aggregate level reports
Agg_Cols = ['AggregateKey','AggregateName']

df0 = reShapeDF (df, SubAgg_Cols, Agg_Cols) # Base dataset

# SubAggregate Level Reports
#PV1 = calculatePValues (df0, SubAgg_Cols) # Call routine to calculate P Values
#df1 = calculateMetrics (df0, SubAgg_Cols)
#df1 = calculateKPIs (df1, SubAgg_Cols)
#df1 = outputKPIs (df1, PV1, SubAgg_Cols)


# Aggregate Level Reports
PV2 = calculatePValues (df0, Agg_Cols) # Call routine to calculate P Values
df2 = calculateMetrics (df0, Agg_Cols)
df2 = calculateKPIs (df2, Agg_Cols)
df2 = outputKPIs (df2, PV2, Agg_Cols)

#print (df1.info())
print (df2.head(20))


