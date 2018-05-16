# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 18:10:13 2017

@author: mallinath.biswas
"""

import pandas as pd
import os
import multiprocessing as mp
import datetime

outDir = "D:\Vertica"
SiloID = "BAYER_WALGREENS"
HDFStoreName = os.path.join(outDir, SiloID)
HDFStore=pd.HDFStore(HDFStoreName)

#HDFStore.remove('Sales')    
#HDFStore.remove('Items')    
#HDFStore.remove('Metadata')    

database='Sales'
#database='Metadata'

# Extract history for 1 year + last years campaign period 
periodBeginKey = 20161130
periodEndKey = 20171201
vendorKey = 40
retailerKey = 6 
CHUNKSIZE = 10**8

def process_frame(chunk, _idnames, _aggfunc):
        # process data frame
        chunk_smry_= chunk.groupby(level=_idnames, as_index=True).agg(_aggfunc)
        return chunk_smry_

if __name__ == '__main__':

#    idnames = ['VendorKey','RetailerKey','ItemKey','StoreKey']
    idnames = ['VendorKey','RetailerKey','PeriodKey']
    
    colnames = ['SalesAmount',
                'SalesVolumeUnits',
                'FirstScanDate',
                'LastScanDate']
                
    aggfunc = {'SalesAmount':'sum',
                'SalesVolumeUnits':'sum',
                'FirstScanDate':'min',
                'LastScanDate':'max'}
    
    _where = "VendorKey == {0} & RetailerKey == {1} & PeriodKey >= {2} & PeriodKey <= {3}".format(vendorKey,retailerKey,periodBeginKey,periodEndKey)
    
    colnames.extend(idnames) # all the columns to be returned from hdfstore

    df = pd.DataFrame()
    
    funclist = []
    
    reader = pd.read_hdf(HDFStore, database, chunksize=CHUNKSIZE, where=_where, columns=colnames)
    
    pool = mp.Pool(4) # use 4 processes
    
    print ('Begin Processing:',datetime.datetime.now())
    
    for chunk in reader:
        
        # process each data frame
        f = pool.apply_async(process_frame,[chunk, idnames, aggfunc])
        funclist.append(f)        
                
 
    for f in funclist:
            df_ = f.get(timeout=600) # timeout in seconds
            df = pd.concat([df, df_])

    print ('End Processing:',datetime.datetime.now())
    print ('CHUNKSIZE=',CHUNKSIZE)
    print (df.info())
    print (df.head())
#    print (df.tail())
       
    HDFStore.close()


