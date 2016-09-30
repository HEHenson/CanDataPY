# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 18:38:48 2016
Stores data describing stats can matrix

@author: Harold_Henson
"""

#orgstyle describes the way that the statscan output is organized

import CansimPY
import timevar
import os
import re
import csv
import pprint
from pandas import Series, DataFrame, HDFStore
import pandas
import unittest.mock as mock


class StatCanMatrix(CansimPY.CansimPY):
    """object storing statscan matrix  info"""
    def  __init__(self, filename, maxvars=10000, user=None, setup=True, 
                  matname = None,maxprob=5):
        super().__init__(user, setup)
        self.filename = filename
        #matname is usually an integer identifying the matrix
        self.matname = matname
        self.SCfilehandle = None
        self.SCfilehandle = self.openSCfile()
        #this will hold a pandas data for this matrix
        self.thepandas = DataFrame()
        self.fileorgtype = None
        self.varcount = 0
        self.obcount = 0
        self.maxvars = maxvars
        #maximum number of problems accept to use a timeseries
        self.maxprobs = 0
        self.gen_dict = self.geo11_dict = None
        self.vlist_dict = self.mtype_dict = self.dat_type = {}
        self.fld_type_dict = self.tags_dict = {}
        self.col_lst = []
        # mat_type describes the formats downloaded from stats Can
        self.loadStatCandicts()
        self.loadstandardtags()
    def loadinh5(self):
        """ load recently created dataframe into h5 file"""
        if self.matname == None:
            matindex = 'Matrix' + str(self.filename[0:4])
        else:
            matindex = 'Matrix' + str(self.matname)
        try:
            print("*** matindex=",matindex)
            print("*** in StatCanMatrix loadinh5",self.Central_data)
            #create dataset not correct command
            self.Central_data[matindex] = self.thepandas
            self.flush(fsync=True)
            print("*** after pandas load")
            return 1
        except:
            return 0
    def __del__(self):
        """do some cleanup when matrix is loaded in pandas file"""
        # load the pandas database into HDF5 file
    def loadStatCandicts(self):
        """ Load Stats Can specific labels"""
        # this will increase with more data types
        self.mtype_dict['Mat_by_row'] = "One timeseries per row"
        self.mtype_dict['Mat_by_col'] = "One timeseries per column"
        self.mtype_dict['obs_by_row'] = "One row per observation"
        # virtually all stats Can data is one of three
        self.dat_type['monthly'] = "M"
        self.dat_type['quarterly'] = "Quarterly"
        self.dat_type['annual'] = "Annual"
        # there are a few exceptions
        # covers irregular surveys
        self.dat_type['cross'] = "Cross Section"
        # census every 5 to 10 years
        self.dat_type['census'] = "Census"
        # weekly is rare but does occur
        self.dat_type['weekly'] = "Weekly"
        # this is dictionary of the variables to be included
        self.vlist_dict['all'] = "Load entire Matrix"
        # this type used to debug software
        self.vlist_dict['first5'] = "Load first 5"
        self.fld_type_dict['datum'] = "numeric observation"
        self.fld_type_dict['date'] = "datafield"
        self.fld_type_dict['gender'] = "Gender"
        #processing not yet implemented
        self.fld_type_dict['NA'] = "Not Implemented"
        self.fld_type_dict['VName'] = "Name of time series"
    def setcollist(self, thelist):
        """ Determine the number of variables"""
        self.col_lst = thelist
        # set constants that will allow rapid lookups
        # number of columns
        self.col_max = len(self.col_lst)
        if self.col_max == 0:
            return self.col_max
        for colno in range(0, self.col_max):
            thetoken = self.col_lst[colno]
            if thetoken == 'NA':
                continue
            if thetoken == 'date':
                self.datepos = colno
                continue
            if thetoken == 'VName':
                self.namepos = colno
                continue
            if thetoken == 'datum':
                self.obpos = colno
        return self.col_max
    def upload(self, mattype, dateformat, vlist='All'):
        """ reads raw data and loads into dataframe"""
        if self.SCfilehandle == None:
            print("nothing to upload")
            return
        if mattype == 'obs_by_row':
            self.upload_obs_by_row(dateformat, vlist)

        self.loadinh5()
    def upload_obs_by_row(self, dateformat, vlist):
        """uploads the observations assuming one row per observation"""
        # thetsvar holds the data being collected
        thetsvar = timevar.CansimTS(self,dateformat)
        # validate the organization of file
        retval = self.istypeoneobperrow()
        if retval != True or retval == 'FHand_None':
            return 'Ftype_Fail'
        if len(self.col_lst) == 0:
            return 'clst_emp'
        if vlist != 'first5':
            maxobs = self.maxvars
        else:
            maxobs = 5
        lastvar = None
        # Will process use CSV reader in system
        self.SCfilehandle.seek(0,0)
        thereader = csv.reader(self.SCfilehandle)

        # start processing the file
        self.varcount = 0
        self.obcount = 0
        header = True
        # need more formal limit
        for linelst in thereader:
            # get the obname
            # first line will already have been processed before this attribute
            # variable name should always be first token
            if header :
                header = False
                continue
            self.obcount = self.obcount + 1
            theobname = linelst[self.namepos]
            # loop by ob until variable changes
            if theobname != lastvar:
                #save data to panda file if variable name changes
                # unless it changed from none
                if lastvar != None :
                    thetsvar.save()
                    self.varcount = self.varcount + 1
                    if self.varcount >= maxobs:
                        break
                self.resetTS(theobname, linelst, thetsvar)
                lastvar = thetsvar.vname
            else:
                # add data item
                thetsvar.setvalue(linelst[self.obpos])

        return self.varcount
    def resetTS(self, thevar, linelst, thetsvar):
        """ reset the current time series"""
        thetsvar.initvariable()
        print('*** in reset TS')
        print('*** colmax=',self.col_max)
        print('*** linelst=',linelst)
        for colno in range(self.col_max):
            # identify the token to process
            thetoken = linelst[colno]
            # idenfity the process to be used
            thefield = self.col_lst[colno]
            if thefield == 'None':
                continue
            if thefield == 'date':
                thetsvar.setdate(thetoken)
                continue
            if thefield == 'gender':
                thetsvar.setgentag(thetoken)
                continue
            if thefield == 'VName':
                thetsvar.setvname(thetoken)
                print('*** process vname = the field',thetoken)
                continue
            if thefield == 'datum':
                thetsvar.setvalue(thetoken)
                continue
    def __repr__(self):
        return "'StatCanMatrix(' %s ')'" % self.filename
    def openSCfile(self ):
        """ Opens the downloaded files from Statistics Canada that is in raw"""
        os.chdir(self.cwd)
        os.chdir('rawdump')
        try:
            self.SCfilehandle = open(self.filename, "r")
        except:
            print(self.mes_dict['RHand_None'])
            print("looked for %s" %self.filename)
            print(self.info())
        os.chdir(os.path.dirname(os.getcwd()))
        if self.SCfilehandle != None:
            logmessage = "Opened" + self.filename
            self.add_to_log(logmessage)
            self.add_to_log(self.mes_dict['F_opened'])
            return self.SCfilehandle
        else:
            logmessage = "Could not open " + self.filename
            self.add_to_log(logmessage)
            self.add_to_log(self.mes_dict['RHand_None'])
            return 'FHand_None'
    def determinefileorg(self):
        """will test the downloaded file to determine configuration
        and basic parameters"""
        #first  test for one ob per row
        self.fileorgtype = self.istypeoneobperrow()
    def loadstandardtags(self):
        """This should include the tags common to many matrices"""
        self.tags_dict['MG'] = "Males"
        self.tags_dict['FG'] = "Females"
        self.tags_dict['TG'] = "Both Sexes"
        self.tags_dict['UG'] = "Unknown"
    def istypeoneobperrow(self):
        """tests if file is one ob per row"""
        if self.SCfilehandle == None:
            return 'FHand_None'
        try:
            self.SCfilehandle.seek(0,0) #file may be tested multifple times
        except:
            return 'FHand_None'
        firstline = self.SCfilehandle.readline()
        firstlinelist = firstline.split(',')
        firsttok = firstlinelist[0]
        firsttokv2 = re.sub(r"\s+", "", firsttok, flags=re.UNICODE)
        return firsttokv2 == 'Ref_Date'

def unittest():
    # will want to use test directory for test
    # move up a directory

    os.chdir(os.path.dirname(os.getcwd()))
    os.chdir('test2')
    #clean up directory for each test



    # Now test the ability to create a matrix
    # This will be Matrix 282_0001 - detailed monthly labour force data
    class Matrix282_0001(StatCanMatrix):
        def __init__(self, thefile, thename="282"):
            super(Matrix282_0001, self).__init__(thefile, 7000,matname=thename)
            # these are valid keys
            self.setcollist(['date', 'NA', 'NA', 'gender', 'NA', 'VName', 'NA', 'datum'])
        def upload(self):
            super(Matrix282_0001, self).upload('obs_by_row', 'M', 'first5')
    # create an instance of it
    this282 = Matrix282_0001("02820001-eng.csv",2820001)
    # to the actual upload
    this282.upload()
    class Matrix1260001(StatCanMatrix):
        def __init__(self, thefile, thename="126"):
            super(Matrix1260001, self).__init__(thefile, 7000,matname=thename)
            # these are valid keys
            self.setcollist(['date', 'NA' , 'NA', 'VName', 'NA', 'datum'])
        def upload(self):
            super(Matrix1260001, self).upload('obs_by_row', 'M', 'all')
    # create an instance of it
    this126 = Matrix1260001("01260001-eng_2016_sept_25.csv",1260001)
    # to the actual upload
    this126.upload()
    # then move back to directory
    # move up a directory

    os.chdir(os.path.dirname(os.getcwd()))
    os.chdir('test2')
def printtest():
    """load up database and print some values out"""
    import pprint
    from pandas import Series, DataFrame, HDFStore
    import pandas
    MYstore = HDFStore('Central_data.h5','r+')
    print(MYstore)
    # themat = MYstore.select('Matrix2820001')
    # pprint.pprint(thevar)
    themat2 = MYstore.select('Matrix1260001')
    print(themat2)
    r19891990 = pandas.period_range('1/1989','12/1993',freq='M')
    thevar3 = Series(themat2.v17981,index = r19891990)
    print(thevar3)
    # V18128 is the last variable is CSV file
    thevar4 = Series(themat2.v18128,index = r19891990)
    print(thevar4)
    MYstore.close()
    return themat2


if __name__ == '__main__':

    unittest()
    # if assert bombs may messup consule
    print("Unit Test was successfull\n")




