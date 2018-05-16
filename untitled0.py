# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 18:10:13 2017

@author: mallinath.biswas
"""

import pandas as pd

outDir = "Y:\Data"
SiloID = "COCACOLA_TARGET"
HDFStoreName = os.path.join(outDir, SiloID) 

df = pd.read_hdf(HDFStoreName, 'Metadata')

print (df.head())
