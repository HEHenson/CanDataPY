# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 19:57:47 2016

@author: lancehermes
"""

import glob
import shutil
from pandas import Series, DataFrame, HDFStore
import pandas.rpy.common as com
import feather
from rpy2.robjects import pandas2ri

def copycsv():
    rootdir = "/home/lancehermes/Dropbox/business/Project/CANSIMPY"
    srcdir = rootdir + "/rawdownload/"
    destdir = rootdir + "/test2/rawdump/"
    srcfiles = srcdir + "*.csv"
    print("copy from %s \n to %s" %(srcfiles,destdir))
    for data in glob.glob(srcfiles):
        shutil.copy2(data,destdir)
    
def exportmatrix(thematrix,targettype):
    """export dataframe to target package"""
    # first retrieve matrix
    MYstore = HDFStore('Central_data.h5','r+')
    thedatfr = MYstore.select(thematrix)
    # only R supported at this time
    if(targettype == 'R'):
        ex_to_R(thedatfr,thematrix)

def ex_to_R(thedatfr,thematrix):
    """copy to dataframe to R"""
    # note that feather had to be hand installed
    # the extra copy is a temporary patch to fix a known bug
    thedatfr_stride = thedatfr.copy()
    feather.write_dataframe(thedatfr_stride,thematrix) 
    
    
    
if __name__ == '__main__':
    copycsv()