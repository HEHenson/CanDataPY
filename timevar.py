# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 18:38:48 2016

@author: Harold_Henson
"""
import unittest.mock as mock
from pandas import Series
import pandas
import numpy
from numpy import nan as NA
import StatCanMatrix as StatCanMatrix





class CansimTS(object):
    """This will be the data class to collate data on an
    individual time series before outputing to pandas"""
    def __init__(self, thematrix=None, thedattype=None):
        self.thematrix = thematrix
        self.freq = thedattype
        self.initvalues()
    def  initvalues(self):
        """ ensures that all values are initialized first time"""
        self.initvariable()
        self.initobservation()
    def  initvariable(self,persistdate=False):
        """these values may only need to be initialized once"""
        self.usevar = True
        self.varprobs = 0
        self.obs = 0
        self.values = []
        self.vname = None
        if persistdate:
            return
        self.styr = None  #first year
        self.stmon = None #first month
        self.stq = None #

    def  initobservation(self,persistdata=False):
        """init values that change with every time series"""
        self.curyr = None
        self.curmon = None
        self.curq = None
        self.gtag = 'UG'    #unknown gender
        if persistdata :
            return
        self.datstr = None
    def setdate(self, thetoken):
        """ will split the token acc"""
        datetoklist = thetoken.split('/')
        if self.freq == 'M':
            self.setcuryr(datetoklist[0])
            self.setcurmon(datetoklist[1])
            self.datstr = thetoken
            return
        if self.freq == 'AS-JAN':
            self.setcuryr(datetoklist[0])
            self.datstr = thetoken
            return
        if self.freq == 'Q-MAR':
            self.setcuryr(datetoklist[0])
            self.setcurmon(datetoklist[1])
            self.datstr = thetoken
            return
    def setvalue(self, thetoken):
        """add a new observed value"""
        try:
            newval = float(thetoken)
        except ValueError:
            if thetoken in ['..', 'x', '...']:
                newval = numpy.nan
                self.varprobs += 1
            else:
                self.usevar = False
                if self.matdump:
                    self.thematrix.matdumphdl.write("the token %s prevented the use of %s /n" %(thetoken,self.vname))
                newval = NA
                # will add log at a later date
        self.values.append(newval)
        self.obs += 1
    def  setcuryr(self, theyear):
        """ set year"""
        # note that the token as been parsed by setdate
        self.curyr = int(theyear)
        if self.styr is None:
            self.styr = self.curyr
    def setcurmon(self, themon):
        """set the month"""
        # will have to translate to pandas
        self.curmon = int(themon)
        if self.stmon is None:
            self.stmon = self.curmon
    def setvname(self, thetoken):
        """loads stats can vector name"""
        self.vname = thetoken
    def sumvar(self):
        """provides basic information interactively"""
        print("For variable ", self.vname)
        print("First Year is ", self.styr)
        print("First Month is ", self.stmon)
    def setgentag(self, thetoken):
        """takes the token and sets gender"""
        if thetoken == self.thematrix.tags_dict['MG']:
            self.gtag = 'MG'
        if thetoken == self.thematrix.tags_dict['FG']:
            self.gtag = 'FG'
        if thetoken == self.thematrix.tags_dict['TG']:
            self.gtag = 'TG'
    def save(self):
        """save data to the current pandas the dataframe"""
        # load data into a series
        # merge series on the dataframe
        # first create new series
        # range is the first step
        if self.varprobs > self.thematrix.maxprobs:
            self.thematrix.ses_log.write("%s not used due to too many NaNs \n" %self.vname)
            if self.thematrix.matdump:
                self.thematrix.matdumphdl.write("%s had %d problems \n" %(self.vname,self.varprobs)) 
            self.thematrix.ses_log.flush()
            self.thematrix.varsnotused += 1
            return
        # determine if there is a list based reason to exclude variable
        # first test if there is a list of individual variables to be included
        if self.thematrix.inclist is not None:
            if self.vname not in self.thematrix.inclist:
                return
        # second test if it is outside the minimum and maximum
        stayin = False
        if self.thematrix.exrng is not None:
            if self.vname < self.thematrix.exrng[0] or self.vname > self.thematrix.exrng[1]:
                stayin = True
            if not stayin:
                return
        # third test if withing inclusion range
        if self.thematrix.incrng is not None:
            if self.vname < self.thematrix.incrng[0]:
                return
            if self.vname > self.thematrix.incrng[1]:
                return
                
        try:
            therng = pandas.date_range(self.datstr, periods=self.obs, freq=self.freq)
        except:
            print("insave",self.datstr)
            print(self.vname)
        thenew = Series(self.values, index=therng, name=self.vname)
        self.thematrix.thepandas[self.vname] = thenew
        self.thematrix.varsused += 1
# UNIT TESTS
        


if  __name__ == '__main__':
    # testmat = StatsCanMatrix("fjkdfjak")

    class Matrix2820001(StatCanMatrix.StatCanMatrix):
        """ Labour Force Survey Annual Averages"""
        def __init__(self, thefile):
            super().__init__(thefile)
            # these are valid keys
            self.setcollist(['date', 'NA', 'NA', 'gender', 'NA', 'VName', 'NA', 'datum'])
        def upload(self):
            super(Matrix2820001, self).upload('obs_by_row', 'M', 'first5')

    THIS282 = Matrix2820001("02820001-eng.csv")
    THIS282.upload()
    assert THIS282.SCfilehandle != None, "statcan  matrix not found"
    RETVAL = THIS282.istypeoneobperrow()
    assert  RETVAL is True, "wrong file type"
