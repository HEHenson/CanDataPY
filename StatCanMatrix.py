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
from pandas import Series, DataFrame, HDFStore
import pandas
import math
import unittest.mock as mock
import copy


class StatCanMatrix(CansimPY.CansimPY):
    """object storing statscan matrix  info"""
    def  __init__(self, filename=None, maxvars=10000, user=None, setup=True,
                  matname=None, maxprobs=90):
        super().__init__(user, setup)
        self.filename = filename
        #matname is usually an integer identifying the matrix
        self.matname = matname
        self.ses_log.write('%s is being process \n' %self.filename)
        self.matdump = True
        if self.matdump:
            self.matdumphdl = open("matdump.txt","w")
            self.matdumphdl.write("detailed issues for %s\n" %self.matname)
        self.SCfilehandle = None
        self.SCfilehandle = self.openSCfile()
        #this will hold a pandas data for this matrix
        self.thepandas = DataFrame()
        self.fileorgtype = None
        # Total number of varialbes in the file
        self.varsinfile = 0
        # Total number of variables stored on H5 file
        self.varsused = 0
        # Total number that is not used
        self.varsnotused = 0
        self.obcount = 0
        self.maxvars = maxvars
        self.maxobs = 5000 #maximum number of observations in time series
        #maximum number of problems accept to use a timeseries
        self.maxprobs = maxprobs
        self.gen_dict = self.geo11_dict = None
        self.vlist_dict = self.mtype_dict = self.dat_type = {}
        self.fld_type_dict = self.tags_dict = {}
        self.col_lst = []
        # mat_type describes the formats downloaded from stats Can
        self.loadStatCandicts()
        self.loadstandardtags()
        # initialize lists for input
        self.retrng = self.exrng = self.incrng = self.inclist = None
    def set_exrng(self,vnums):
        """two vnumbers that define range to be excluded"""
        retrng = self.set_rng2(vnums)
        self.exrng = copy.deepcopy(retrng)
    def set_incrng(self,vnums):
        """ restrict vnumbers to the minumum and maximum"""
        retrng = self.set_rng2(vnums)
        self.incrng = copy.deepcopy(retrng)
    def set_rng2(self,vnums):
        self.retrng = []
        if type(vnums) is not list:
            print(self.mes_dict['Inv_Lst'])
            return 'Inv_Lst'
        if len(vnums) !=2:
            print(self.mes_dict['Inv_Lst'])
            return 'Inv_Lst'
        retval = 0
        for vobj in vnums:
            if not self.isvnum(vobj):
                print(self.mes_dict['Inv_Vnum'] %vobj)
                continue
            self.retrng.append(vobj)
            retval += 1
        return self.retrng
    def set_inclist(self,vnums):
        """ list of individual v numbers to be included"""
        self.inclist = []
        if type(vnums) is not list:
            print(self.mes_dict['Inv_Lst'])
            return 'Inv_Lst'
        # need to check that each entry is a string
        retval = 0
        for vobj in vnums:
            if not self.isvnum(vobj):
                print(self.mes_dict['Inv_Vnum'] %vobj)
                continue
            self.inclist.append(vobj)
            retval += 1
        return retval
        
    def isvnum(self,vobj):
        """ Boolean function to validate if string appears to look like
        vnumber"""
        if vobj is not str:
            return 'Inv_Vnum'
        if vobj[0] is not 'v':
            return 'Inv_Vnum'
        # not sure what the maximum length is at this point but will assume
        # 3o for now
        if len(vobj) < 30:
            return 'Inv_Vnum'
        # check to make sure that the rest of the digit is a number
        if not vobj[1:].isdigit():
            return 'Inv_Vnum'
        
        
        
    def loadinh5(self):
        """ load recently created dataframe into h5 file"""
        if self.matname == None:
            matindex = 'Matrix' + str(self.filename[0:4])
        else:
            matindex = 'Matrix' + str(self.matname)
        try:
            print("*** matindex=", matindex)
            print("*** in StatCanMatrix loadinh5", self.Central_data)
            #create dataset not correct command
            self.Central_data[matindex] = self.thepandas
            self.flush(fsync=True)
            return 1
        except:
            return 0
    def __del__(self):
        """do some cleanup when matrix is loaded in pandas file"""
        # load the pandas database into HDF5 file
        self.ses_log.write("for %s %d variables were found on the file\n" %(self.filename,self.varsinfile))
        self.ses_log.write("   %d were saved and %d were not used" %(self.varsused,self.varsnotused))
        if self.SCfilehandle != None:
            self.SCfilehandle.close()
        if self.matdump == True:
            self.matdumphdl.close()
    def loadStatCandicts(self):
        """ Load Stats Can specific labels"""
        # this will increase with more data types
        self.mtype_dict['Mat_by_row'] = "One timeseries per row"
        self.mtype_dict['Mat_by_col'] = "One timeseries per column"
        self.mtype_dict['obs_by_row'] = "One row per observation"
        # virtually all stats Can data is one of three
        self.dat_type['monthly'] = "M"
        self.dat_type['quarterly'] = "Quarterly"
        self.dat_type['Calendar Year'] = "AS-JAN"
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
        retval = self.is_installed()
        if retval == False:
            print(self.mes_dict['SYS_NA'])
            return 'SYS_NA'
        if self.SCfilehandle == None:
            print("nothing to upload")
            return
        if mattype == 'obs_by_row':
            self.upload_obs_by_row(dateformat, vlist)
        if mattype == 'by_row':
            self.upload_by_row(dateformat, vlist)
        self.loadinh5()
        print("Out of %d variables %d were not loaded" %(self.varsinfile,self.varsnotused))
    def upload_by_row(self, dateformat, vlist):
        """ uploads the variable by row"""
        # thetsvar holds the data being collected
        thetsvar = timevar.CansimTS(self,dateformat)
        # Will process use CSV reader in system
        self.SCfilehandle.seek(0,0)
        thereader = csv.reader(self.SCfilehandle)

        # a feature of this file structure is that the date column and
        # first ob are the same
        self.datepos = self.obpos        
        # start processing the file
        self.obcount = 0
        headerfound = False
        headlines = 0
        # need more formal limit
        # note that last few lines are footnotes and should be ignored
        for linelst in thereader:
            # process lines until find header
            if len(linelst) > self.namepos:
                theobname = linelst[self.namepos]
                headerfound = True
            else:
                continue
            if theobname == "" : continue
            if headerfound:
                if theobname == "Vector":
                    lastpos = len(linelst)
                    thetsvar.setdate(linelst[self.datepos])
                    continue
            else:
                headlines += 1
                if headlines > 30 : return 'Ftype_Fail'
                # need to find last time period
                # need to validate this formulae
                continue
            self.resetTS(theobname, linelst, thetsvar,persistdate=True)
            #all the data will be on the one line
            #the first line will already be enter
            thepos = self.obpos+1
            while thepos < lastpos:
                thetsvar.setvalue(linelst[thepos])
                thepos += 1
            thetsvar.save()
            self.varsinfile += 1

                
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
                    self.varsinfile += 1
                    if self.varsinfile >= maxobs:
                        break
                self.resetTS(theobname, linelst, thetsvar)
                lastvar = thetsvar.vname
            else:
                # add data item
                thetsvar.setvalue(linelst[self.obpos])
        # note that loop will abort without saving the last variable
        thetsvar.save()

        return self.varsinfile
    def resetTS(self, thevar, linelst, thetsvar,persistdate=False):
        """ reset the current time series"""
        thetsvar.initvariable(persistdate)
        for colno in range(self.col_max):
            # identify the token to process
            thetoken = linelst[colno]
            # idenfity the process to be used
            thefield = self.col_lst[colno]
            if thefield == 'None':
                continue
            if thefield == 'date' and persistdate == False:
                thetsvar.setdate(thetoken)
                continue
            if thefield == 'gender':
                thetsvar.setgentag(thetoken)
                continue
            if thefield == 'VName':
                thetsvar.setvname(thetoken)
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
        def __init__(self, filename=None, maxvar=7000,matname=None):
            super(Matrix282_0001, self).__init__(filename, maxvar,matname)
            # these are valid keys
            self.setcollist(['date', 'NA', 'NA', 'gender', 'NA', 'VName', 'NA', 'datum'])
        def upload(self):
            super(Matrix282_0001, self).upload('obs_by_row', 'M', 'first5')
    # create an instance of it
    this282 = Matrix282_0001(filename= "02820001-eng.csv", matname=2820001)
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
    # will test accuracy of data uploaded
    # note that this may need to be updated from time to time if the
    # data itself changes
    MYstore = HDFStore('Central_data.h5','r+')
    MYstore
    themat2 = MYstore.select('Matrix1260001')
    # check the very first observation of the first variable in the CSV file
    theval = themat2.v17953['1985-01']
    assert math.isclose(theval,8670.5),"v17193 not loaded properly"
    # check the last observation of the last variable
    theval = themat2.v18128['2016-02']
    assert math.isclose(theval,33.9),"v18128 not loaded properly"
    # check a variable with many NANs
    # check only valid for a high number of variables
    if(this126.maxprobs>35):
        theval = themat2.v18007['2013-02']
        assert math.isclose(theval,244.5),"v18007 not loaded properly" 
        theval = themat2.v18007['2013-03']
        assert math.isnan(theval),"v18007 not loaded properly" 
        theval = themat2.v18007['2016-02']
        assert math.isnan(theval),"v18007 not loaded properly" 
    if(this126.maxprobs>25):
        # data for v18006 is the same as v18007
        theval = themat2.v18006['2013-02']
        assert math.isclose(theval,244.5),"v18006 not loaded properly" 
        theval = themat2.v18006['2013-03']
        assert math.isnan(theval),"v18006 not loaded properly" 
        theval = themat2.v18006['2016-02']
        assert math.isnan(theval),"v18006 not loaded properly"   
    if(this126.maxprobs>25):
        theval2 = themat2.v18109['2013-02']
        assert math.isclose(theval2,54),"v18109 not loaded properly retval=%f" %theval2 
        theval = themat2.v18109['2013-03']
        assert math.isnan(theval),"v18109 not loaded properly" 
        theval = themat2.v18109['2016-02']
        assert math.isnan(theval),"v18109 not loaded properly"    
    
    # Now test annual data
    class Matrix2820004(StatCanMatrix):
        def __init__(self, thefile, thename="126"):
            super(Matrix2820004, self).__init__(filename=thefile, maxvars=7000,matname=thename)
            # these are valid keys
            self.setcollist(['date', 'NA' , 'NA', 'NA','gender','NA','VName', 'NA', 'datum'])
        def upload(self):
            super(Matrix2820004, self).upload('obs_by_row', 'AS-JAN', 'all')
    # create an instance of it
    this2820004 = Matrix2820004("02820004-eng_2016_Oct_05.csv",2820004)
    # to the actual upload
    this2820004.upload()
    # will test accuracy of data uploaded
    # note that this may need to be updated from time to time if the
    # data itself changes
    MYstore = HDFStore('Central_data.h5','r+')
    themat3 = MYstore.select('Matrix2820004')
    # check the very first observation of the first variable in the CSV file
    theval = themat3.v2582391['1990']
    print(theval)
    assert math.isclose(theval,21214.7),"v2582391 not loaded properly"
    # check the last observation of the last variable
    theval = themat3.v2587870['2015']
    print(theval)
    assert math.isclose(theval,48.0),"v2609119 not loaded properly"
    # Now test quarterly data
    class Matrix3800085(StatCanMatrix):
        def __init__(self, thefile, thename="3800085"):
            super(Matrix3800085, self).__init__(thefile, 7000,matname=thename)
            # these are valid keys
            self.setcollist(['date', 'NA' , 'NA', 'NA','NA','VName', 'NA', 'datum'])
        def upload(self):
            super(Matrix3800085, self).upload('obs_by_row', 'Q-MAR', 'all')
    # create an instance of it
    this3800085 = Matrix3800085("03800085-eng_2016_Oct_05.csv",3800085)
    # to the actual upload
    this3800085.upload()
    # will test accuracy of data uploaded
    # note that this may need to be updated from time to time if the
    # data itself changes
    MYstore = HDFStore('Central_data.h5','r+')
    themat4 = MYstore.select('Matrix3800085')
    # check the very first observation of the first variable in the CSV file
    theval = themat4.v62700456['1981-3']
    print(theval)
    assert math.isclose(theval,45571),"v62700456 not loaded properly"
    # check the last observation of the last variable
    theval = themat4.v62700930['2016-06']
    print(theval)
    assert math.isclose(theval,1598),"v62700930 not loaded properly"
    # Now test single retrieval on annual data
    class Matrix1530114(StatCanMatrix):
        def __init__(self, thefile, thename="126"):
            super(Matrix1530114, self).__init__(filename=thefile, maxvars=7000,matname=thename)
            # these are valid keys
            self.setcollist(['date', 'NA' , 'NA', 'VName', 'NA', 'datum'])
        def upload(self):
            super(Matrix1530114, self).upload('obs_by_row', 'AS-JAN', 'all')
    # create an instance of it
    this1530114 = Matrix1530114("01530114-eng.csv", 1530114)
    # to the actual upload
    this1530114.set_inclist(['v79874995'])
    print("*** inclist is ",this1530114.inclist)
    this1530114.upload()
    # will test accuracy of data uploaded
    # note that this may need to be updated from time to time if the
    # data itself changes
    MYstore = HDFStore('Central_data.h5','r+')
    themat5 = MYstore.select('Matrix1530114')
    # check the very first observation of the first variable in the CSV file
    theval = themat5.v79874995['2009']
    print(theval)
    assert math.isclose(theval,721165),"v79874995 not loaded properly"
    # check the last observation of the first variable
    theval = themat5.v79874995['2014']
    print(theval)
    assert math.isclose(theval,768238),"v79874995 not loaded properly"
    # This will test the include in range option
    # It will also test doing a second retrieval off the same csv file  
    this1530114 = Matrix1530114("01530114-eng.csv", 1530114)
    # to the actual upload
    this1530114.set_incrng(['v79874995','v79874998'])
    print("*** exclussion list is ",this1530114.incrng)
    this1530114.upload()
    # will test accuracy of data uploaded
    # note that this may need to be updated from time to time if the
    # data itself changes
    MYstore = HDFStore('Central_data.h5','r+')
    themat6 = MYstore.select('Matrix1530114')
    # check the very first observation of the first variable in the CSV file
    theval = themat6.v79874995['2009']
    print(theval)
    assert math.isclose(theval,721165),"v79874995 not loaded properly"
    # check the last observation of the first variable
    theval = themat6.v79874995['2014']
    print(theval)
    assert math.isclose(theval,768238),"v79874995 not loaded properly"
     # check the very first observation of the first variable in the CSV file
    theval = themat6.v79874998['2009']
    print(theval)
    assert math.isclose(theval,8418),"v79874998 not loaded properly"
    # check the last observation of the first variable
    theval = themat6.v79874998['2014']
    print(theval)
    assert math.isclose(theval,8747),"v79874998 not loaded properly"   

    # This will test the exclude in range option  
    this1530114 = Matrix1530114("01530114-eng.csv", 1530114)
    # to the actual upload
    # note that an extremely large value is all that is left
    this1530114.set_exrng(['v79874995','v79874998'])
    print("*** exclusion list is ",this1530114.exrng)
    this1530114.upload()
    # will test accuracy of data uploaded
    # note that this may need to be updated from time to time if the
    # data itself changes
    MYstore = HDFStore('Central_data.h5','r+')
    themat7 = MYstore.select('Matrix1530114')
    # check the very first observation of the first variable in the CSV file
    theval = themat7.v79874999['2009']
    print(theval)
    assert math.isclose(theval,508),"v79874999 not loaded properly"
    # check the last observation of the first variable
    theval = themat7.v79874999['2014']
    print(theval)
    assert math.isclose(theval,536),"v79874999 not loaded properly"
     # check the very first observation of the first variable in the CSV file
    theval = themat7.v79875120['2009']
    print(theval)
    assert math.isclose(theval,637),"v79875120 2009 not loaded properly %f" %theval
    # check the last observation of the first variable
    theval = themat7.v79875120['2014']
    print(theval)
    assert math.isclose(theval,-1096),"v79875120 2014 not loaded properly %f" %theval    
    
    #Now will try a by row
    class Matrix1530114Row(StatCanMatrix):
        def __init__(self, thefile, thename="126"):
            super(Matrix1530114Row, self).__init__(filename=thefile, maxvars=7000,matname=thename)
            # these are valid keys
            self.setcollist(['NA', 'NA', 'VName', 'NA', 'datum'])
        def upload(self):
            super(Matrix1530114Row, self).upload('by_row', 'AS-JAN', 'all')
    # create an instance of it
    this1530114 = Matrix1530114Row("1530114-eng-B.csv", 1530114)
    # to the actual upload
    this1530114.set_inclist(['v79874995','v79875120'])
    print("*** inclist is ",this1530114.inclist)
    this1530114.upload()
    # will test accuracy of data uploaded
    # note that this may need to be updated from time to time if the
    # data itself changes
    MYstore = HDFStore('Central_data.h5','r+')
    themat8 = MYstore.select('Matrix1530114')
    # check the very first observation of the first variable in the CSV file
    theval = themat8.v79874995['2010']
    print(theval)
    assert math.isclose(theval,735016),"v79874995 not loaded properly by row"
    # check the last observation of the first variable
    theval = themat8.v79875120['2014']
    print(theval)
    assert math.isclose(theval,-1096),"v79875120 not loaded properly by row"
    
    
    # then move back to directory
    # move up a directory
    os.chdir(os.path.dirname(os.getcwd()))
    os.chdir('test2')

def printtest():
    """load up database and print some values out"""
    import pprint
    from pandas import Series, DataFrame, HDFStore
    MYstore = HDFStore('Central_data.h5','r+')
    print(MYstore)
    # themat = MYstore.select('Matrix2820001')
    # pprint.pprint(thevar)
    themat2 = MYstore.select('Matrix1530114')
    print(themat2)
    #therange = pandas.period_range('1989-1','1993-3',freq='Q-MAR')
    #thevar3 = Series(themat2.v62700456,index = therange)
    #print(thevar3)     
    MYstore.close()
    return themat2



if __name__ == '__main__':

    unittest()
    # if assert bombs may messup consule
    print("Unit Test was successfull\n")




