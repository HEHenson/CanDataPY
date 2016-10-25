# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 09:42:41 2016

@author: lancehermes
"""

import os
import openpyxl
from openpyxl.cell import get_column_letter, column_index_from_string
import StatCanMatrix
import statsmodels.api as sm
import numpy as np
import math
import pandas as pd
from pandas import DataFrame


class AdminData(StatCanMatrix.StatCanMatrix):
    """object storing statscan matrix  info"""
    def  __init__(self, filename=None, maxvars=10000, user=None, setup=True,
                  matname=None, maxprobs=5):
        super().__init__(user, setup)
        self.filename = filename
        #matname is usually an integer identifying the matrix
        self.matname = matname
        self.ses_log.write('%s is being process \n' %self.filename)
        self.SCfilehandle = None
        self.SCfilehandle = self.openSCfile()
        #this will hold a pandas data for this matrix
        self.thepandas = None
        self.sheet = None
        self.fileorgtype = None
        # Total number of varialbes in the file
        self.varsinfile = 0
        # Total number of variables stored on H5 file
        self.varsused = 0
        # Total number that is not used
        self.varsnotused = 0
        self.obcount = 0
        self.maxvars = maxvars
        #maximum number of problems accept to use a timeseries
        self.maxprobs = maxprobs
        self.gen_dict = self.geo11_dict = None
        self.vlist_dict = self.mtype_dict = self.dat_type = {}
        self.fld_type_dict = self.tags_dict = {}
        self.col_lst = []
        self.dum_lst = []
        self.orgby = None
        # mat_type describes the formats downloaded from stats Can
        self.rowindex = []
        self.loadStatCandicts()
        self.loadstandardtags()
        
    def settemplate(self,thetemplate):
        if thetemplate == "rows_for_dept":
            retval = self.rows_for_dept()
            return retval
        print("Template not found")
        return 0
        
    def openworksheet(self,xlsfile,thesheet):
        """open up the spreadsheet file"""
        os.chdir(self.cwd)
        os.chdir('rawdump')
        self.wb = openpyxl.load_workbook(xlsfile)
        self.sheet = self.wb.get_sheet_by_name(thesheet)
        os.chdir(self.cwd)        
        
    def setgroup(self,colchr):
        """this sets the column that defines the department within the organization"""
        """should be one or two upper case charaters"""
        self.orgby = colchr
        self.orgcol = column_index_from_string(self.orgby)
        # the function seems to miss by one column
        # not sure why
        self.orgcol -= 1
        # will go through spreadsheet and build up list of departments
        lstcell = None
        rowno = 0
        self.obcount = 0
        for cellObj in self.sheet.columns[self.orgcol]:
            thevalue = cellObj.value
            if thevalue == None:
                continue
            if thevalue.isspace():
                continue
            rowno += 1
            if lstcell != thevalue:
                self.obcount += 1
                thevalue = thevalue
                self.rowindex.append(thevalue)
                lstcell = thevalue
        
    def setsubgroup(self,colchr):
        """with this template one column will define the row within a group"""
        #note first column starts with 'A'
        self.orgsubby = colchr
        
    def setcolumn(self, subgroup, offset, colname):
        """A list of colnames will be built up for the output dataframe"""
        """colname is the name in the dataframe"""
        """subgroup is the string in the orgsubby variable"""
        self.varsinfile += 1
        self.col_lst.append([subgroup,colname,offset])
        
    def setdummy(self, datacol, colname, dumlamb):
        """ datacol - column on input spreadsheet"""
        """ colname - column on output dataframe"""
        """ dumlamb - function to evaluation lambda"""
        self.varsinfile += 1
        self.dum_lst.append([datacol,colname,dumlamb])
    
    def rows_for_dept(self):
        """ This processes the file according to a group of rows for each department"""
        # first need to build up a column list from dummy and regular variable list
        junkhdl = open("junklog.txt","w")
        collist = []
        colnovar = len(self.col_lst)
        for thecol in range(colnovar):
            thecolname = self.col_lst[thecol][1]
            collist.append(thecolname)
        # now add the dummy variable list
        colnodum = len(self.dum_lst)
        for thecol in range(colnodum):
            thecolname = self.dum_lst[thecol][1]
            collist.append(thecolname)
        junkhdl.write("Point1\n")
        junkhdl.write(str(collist)) 
        junkhdl.write(" \n")
        junkhdl.write("total number of colums %d vars %d dums\n" %(colnovar,colnodum))
            
        print(self.obcount," ",len(self.rowindex))
        print(self.varsused,len(collist))
            
        # Now a numpy matrix is needed for the data
            
        thedata = np.full([self.obcount,len(collist)],np.nan)
        self.thepandas = DataFrame(thedata,self.rowindex,columns=collist)
            
        # now go through rowindex and fill in DataFrame
        # begdept and enddept will be pointers to relevant row in spreadsheet
        enddept = 0
        spreaddept = None
        for thedept in self.rowindex:
            #find first row in spreadsheet
            begdept = enddept + 1
            while thedept != spreaddept:
                begdept += 1
                thecell = self.orgby + str(begdept)
                cellobj = self.sheet[thecell]
                spreaddept = cellobj.value
                if(begdept > self.maxvars):
                    print('infinite loop begdept = %d' %begdept)
                    break
            enddept = begdept
            while thedept == spreaddept:
                enddept += 1
                thecell = self.orgby + str(enddept)
                cellobj = self.sheet[thecell]
                spreaddept = cellobj.value
                if(enddept > self.maxvars):
                    print('infinite loop enddept = %d' %enddept)
                    break
            # enddept will be one two high when while finished
            enddept -= 1
            # now go through all the columns
            # first for the value list
            junkhdl.write("Point 2\n")
            junkhdl.write("dept %s starts at %d and ends at %d \n" %(thedept,begdept,enddept))
            junkhdl.write("Point 3\n")
            for thecol in range(colnovar):
                thecolname = self.col_lst[thecol][1]
                #find row where sub group occurs
                thesubgroup = self.col_lst[thecol][0]
                theoffset = self.col_lst[thecol][2]
                for therow in range(begdept,enddept+1):
                    thecell = self.orgsubby + str(therow)
                    cellobj = self.sheet[thecell]
                    theval = cellobj.value
                    if theval == thesubgroup:
                        break
                junkhdl.write("theval = %s for subgroup = %s at row %d" %(theval,thesubgroup,therow))
                
                #now that the row has been found
                thecell = theoffset + str(therow)
                cellobj = self.sheet[thecell]
                theval = cellobj.value
                #now the value has to be inserted in the dataframe
                self.thepandas.ix[thedept,thecolname] = theval
            # then for qualitative variables for the value list
            for thecol in range(colnodum):
                thecolname = self.dum_lst[thecol][1]
                #find row where sub group occurs
                thesubgroup = self.dum_lst[thecol][0]
                theevalfunc = self.dum_lst[thecol][2]
                #values are all the same for deptmartment
                thecell = thesubgroup + str(begdept)
                cellobj = self.sheet[thecell]
                cellval = cellobj.value
                theval = theevalfunc(cellval)
                #now the value has to be inserted in the dataframe
                self.thepandas.ix[thedept,thecolname] = theval                
            
        print(self.thepandas)
        junkhdl.close()
def xvartab(xvar,xvarname=None,keyvar=None):
    """ perform basic analysis on exogenous variables"""
    barstr = '='*80
    print(barstr)
    print("Analysis of %s \n" %xvarname)
    print("%s varies from minimum of %f to a maximum of %f" %(xvarname,xvar.min(),xvar.max()))
    print("the mean is %f and the standard deviation is %f" %(xvar.mean(),xvar.std()))
    print("out of %i observations %i were missing" %(xvar.shape[0],np.count_nonzero(np.isnan(xvar))))
    
    
def esttry1(regdata):
    """this procedure estimates the OLS model using the example found online at
    http://statsmodels.sourceforge.net/stable/examples/notebooks/generated/ols.html
    """        
    # note that the R Notation does not appear to work so regression is run as
    # Numpy arrays as in the example given in the URL
    print(regdata.thepandas.shape)
    # result1 = sm.OLS(formula="DaysToPost ~ HRCap",data=regdata.thepandas).fit()
    print("*** just after regression")
    # print(result1)
    y = np.array(regdata.thepandas.DaysToPost)
    const = np.ones(y.shape[0])
    # the workload variables must be calculated from the spreadsheet data
    # Total Staffing Actions over Total employees
    workload = np.zeros((y.shape[0],1),dtype=float)
    workload = regdata.thepandas.TotalActions/regdata.thepandas.NumEmp
    
    x = np.column_stack((const, workload,
                         regdata.thepandas.islarge,
                         regdata.thepandas.CasUse,
                         regdata.thepandas.HRCap,
                         regdata.thepandas.ActUse,
                         regdata.thepandas.TermUse))
    # it is very important that this list be matched
    my_xnames = ["const","workload","islarge","CasUse","HRCap","ActUse","TermUse"]

    xvartab(workload,xvarname=my_xnames[1])
    xvartab(regdata.thepandas.islarge,xvarname=my_xnames[2])
    xvartab(regdata.thepandas.CasUse,xvarname=my_xnames[3])
    xvartab(regdata.thepandas.HRCap,xvarname=my_xnames[4])
    xvartab(regdata.thepandas.ActUse,xvarname=my_xnames[5])
    xvartab(regdata.thepandas.TermUse,xvarname=my_xnames[6])
    result1 = sm.OLS(y,x,missing='drop',hasconst=True).fit()
    print(result1.summary(xname=my_xnames))
def unittest():
    # will want to use test directory for test
    # move up a directory

    os.chdir(os.path.dirname(os.getcwd()))
    os.chdir('test2')       
    
    # Now test the ability to create a matrix
    # This will be Matrix 282_0001 - detailed monthly labour force data
    class Try1(AdminData):
        def __init__(self, filename=None, maxvar=7000,matname=None):
            super(Try1, self).__init__(filename, maxvar,matname)
            # these are valid keys
            
    thistry1 = Try1(filename= "1464478DSAD3574.xlsx", matname='try1') 
    
    thistry1.openworksheet("1464478DSAD3574.xlsx",'2014-2015')           
    thistry1.settemplate("rows_for_dept")
    thistry1.setgroup('A')
    thistry1.setsubgroup('D')
    thistry1.setcolumn('FLX/EFF-2','G','DaysToPost')
    thistry1.setcolumn('SUP-2B','F','TotalActions')
    thistry1.setcolumn('SUP-2A','F','NumEmp')
    islarge = (lambda x: 1 if x == 'LARGE' or x == 'MEDIUM' else 0 )
    thistry1.setdummy('B','islarge',islarge)
    thistry1.setcolumn('SUP-2A','G','HRCap')
    thistry1.setcolumn('FAIR-3A','G','CasUse')
    thistry1.setcolumn('FAIR-2','G','ActUse')
    thistry1.setcolumn('FAIR-3B','G','TermUse')
    thistry1.rows_for_dept()
    thistry1.thepandas
    
    #dependent variable ELX/EFF2
    assert thistry1.thepandas.ix['ACO','DaysToPost']==128.625, "Error in ACO,DaystoPost"
    #workload ratio must be calculation from TotalActions/Total Employees
    # SUP-2B Denominator
    retval = thistry1.thepandas.ix['ACO','TotalActions']
    assert retval==261, "Error in ACO,Total staffing action %f" %retval
    # SUP-2A Denominator
    retval = thistry1.thepandas.ix['ACO','NumEmp']
    assert retval==603, "Error in ACO,Total staffing action %f" %retval
    # Need to calculate ratio
    
    #Dept agency = Large or medium
    assert thistry1.thepandas.ix['ACO','islarge']==1.0, "Error in ACO,is large"
    assert thistry1.thepandas.ix['AGR','islarge']==1.0, "Error in AGR,is large"
    assert thistry1.thepandas.ix['AHS','islarge']==1.0, "Error in AHS,is large"
    assert thistry1.thepandas.ix['APT','islarge']==0.0, "Error in APT,is large"
    #casual workers over total staffing actions Fair/3A
    assert thistry1.thepandas.ix['ACO','CasUse']==0.0, "Error in ACO,is CasUse"
    retval = thistry1.thepandas.ix['AGR','CasUse']
    print(retval)
    assert math.isclose(retval,6.172840,abs_tol=0.001), "Error in AGR,is CaseUse %s" %retval
    retval = thistry1.thepandas.ix['AHS','CasUse']
    assert math.isclose(retval,8.333333,abs_tol=0.001), "Error in AHS,is CaseUse %s" %retval     
    # SUP-2B
    retval = thistry1.thepandas.ix['ACO','HRCap']
    assert math.isclose(retval,1.824212, abs_tol=0.001), "Error in ACO,is HRCap %s" %retval
    #Use of Tempory Hires Term Staffing/Total Staffing 
    #total actings Fair 2G
    retval = thistry1.thepandas.ix['ACO','ActUse']
    assert math.isclose(retval,17.021277, abs_tol=0.001), "Error in ACO,is ActUse %s" %retval
    # Mitigation Strategy 1     
    # Acting Appointments ratio of temporary/Total Staffing actions
    #FAIR-3B
    retval = thistry1.thepandas.ix['ACO','ActUse']
    assert math.isclose(retval,17.021277, abs_tol=0.001), "Error in ACO,is ActUse %s" %retval
    # Mitigation Strategy 2
    # Acting to Total
    # FAIR-2
    retval = thistry1.thepandas.ix['ACO','ActUse']
    assert math.isclose(retval,17.021277, abs_tol=0.001), "Error in ACO,is ActUse %s" %retval
    esttry1(thistry1)
    """
           

    # to the actual upload
    """
    """
    thistry1.loadtable()
    """   
if __name__ == '__main__':

    unittest()
    # if assert bombs may messup consule
    print("Unit Test was successfull\n")        