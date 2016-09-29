# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 18:38:48 2016

@author: Harold_Henson
"""
import unittest.mock as mock
from pandas import Series
import pandas
import StatCanMatrix as StatCanMatrix
import numpy
from numpy import nan as NA




class CansimTS:
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
    def  initvariable(self):
        """these values may only need to be initialized once"""
        self.styr = None  #first year
        self.stmon = None #first month
        self.stq = None #
        self.vname = None
        self.usevar = True
        self.varprobs = 0
        self.obs = 0
        self.values = []
    def  initobservation(self):
        """init values that change with every time series"""
        self.curyr = None
        self.curmon = None
        self.curq = None
        self.datstr = None
        self.gtag = 'UG'    #unknown gender
    def setdate(self, thetoken):
        """ will split the token acc"""
        datetoklist = thetoken.split('/')
        print("*** in setdate the token = ", thetoken)
        print("*** the freq set to= ", self.freq)
        if self.freq == 'M':
            self.setcuryr(datetoklist[0])
            self.setcurmon(datetoklist[1])
            self.datstr = thetoken
    def setvalue(self, thetoken):
        """add a new observed value"""
        try:
            newval = float(thetoken)
        except ValueError:
            if thetoken in ['..','x','...']:
                newval = numpy.nan
            else:
                print("invalid token in setvalue",thetoken)
                self.usevar = False
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
        therng = pandas.date_range(self.datstr, periods=self.obs, freq=self.freq)
        thenew = Series(self.values, index=therng, name=self.vname)
        self.thematrix.thepandas[self.vname] = thenew
        print("*** justadded the new \n\r", self.thematrix.thepandas)
        
# UNIT TESTS
if  __name__ == '__main__':
    # testmat = StatsCanMatrix("fjkdfjak")

    class Matrix282_0001(StatCanMatrix.StatCanMatrix):
        """ Labour Force Survey Annual Averages"""
        def __init__(self, thefile):
            super().__init__(thefile)
            # these are valid keys
            self.setcollist(['date', 'NA', 'NA', 'gender', 'NA', 'VName', 'NA', 'datum'])
        def upload(self):
            super(Matrix282_0001, self).upload('obs_by_row', 'M', 'first5')

    This282 = Matrix282_0001("02820001-eng.csv")
    This282.upload()
    assert This282.SCfilehandle != None, "statcan  matrix not found"
    Retval = This282.istypeoneobperrow()
    assert  Retval is True, "wrong file type"
