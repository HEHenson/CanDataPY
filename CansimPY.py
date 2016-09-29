# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 20:37:02 2016

@author: lancehermes
"""

import h5py
import sys
import os
import datetime
import shutil
import glob
from pandas import DataFrame, HDFStore



class CansimPY:
    def  __init__(self, user=None, setup=True):
        #initial dictionaires
        self.mtype_dict = self.mes_dict = {}
        self.loadmessages()
        self.cwd = os.getcwd()
        self.matrices_updated = []
        self.sessionarchive = None
        self.Central_data = HDFStore("Central_data.h5", "r+")
        self.theuser = user
        self.status = None
        # as a string
        self.thedate = "{:%B_%d_%Y}".format(datetime.datetime.now())
        # so calculations can be done
        self.starttime = datetime.datetime.now()
        try:
            self.currentlog = open('currentlog', 'w')
        except Exception:
            self.currentlog = None
            print(self.mes_dict['Sys_NA'])
        self.startlog()
        print('*** past start log in initialization')
        if setup == True:
            self.addsessionarchive()
        self.dirdict = {'thearch':'archive',
                        'current':'most_recent',
                        'raw':'rawdump'}
    def info(self):
        """Lists diagnostic information about the session"""
        retstr = "Session started at hour " + str(self.starttime.hour)
        retstr = retstr + " minute " + str(self.starttime.minute)
        print(retstr)
        retstr = "The current working directory is " + self.cwd
        print(retstr)
    def __del__(self):
        """need to perform some cleanup at end of session as
        well as ensure that all files are closed"""
        self.currentlog.close()
        self.Central_data.close()
        self.archiveloadedmatrices()
        self.archivelogfile()
        #delete from workspace
    def __repr__(self):
        retstr = "CansimPYSession started at " + self.thedate
        return retstr
    def startlog(self):
        """initialize the log file"""
        print("*** at startlog %s" % self.theuser)
        self.add_to_log("Beginning of session by user %s" % self.theuser)
    def testlog(self):
        """quick check to make sure log file is open"""
        if self.currentlog == None:
            print("Log File not working")
        return self.currentlog
    def timelapsed(self):
        """ returns time in current session in minutes"""
        retval = datetime.datetime.now() - self.starttime
        retmin = (int(retval.total_seconds()/60))
        return retmin
    def loadmessages(self):
        """ Messages associated with return codes are stored in dictionaries
        to ensure consistency"""
        # mes_dict contains the error messages
        self.mes_dict['Sys_NA'] = 'System not installed on this directory \n'
        self.mes_dict['Sys_A'] = 'System installed on this directory \n'
        self.mes_dict['Init_Halt'] = "initalization abandonded \n"
        self.mes_dict['Del_Halt'] = "deletion abandonded \n"
        self.mes_dict['Sys_Loaded'] = "System already started \n"
        self.mes_dict['Sys_setup'] = "unable to open log file - check rw in directory \n"
        self.mes_dict['Ses_start'] = "Session has started"
        self.mes_dict['F_exists'] = "File exists"
        self.mes_dict['F_opened'] = "File opened"
        self.mes_dict['RL_Fail'] = "Remote List Fail"
        self.mes_dict['Ftype_Fail'] = "Wrong File Type"
        self.mes_dict['FHand_None'] = "File Handle is Null"
        self.mes_dict['RHand_None'] = "Raw File not found"
        self.mes_dict['clst_emp'] = "column list not initialized"
    def add_to_log(self, anewline):
        """this method will append a new line to the session log file"""
        if self.currentlog == None:
            print("Log file not open\n")
            return 'FHand_None'
        self.currentlog.writelines(anewline)
        self.currentlog.write("\n")
    def is_installed(self):
        """determine if the directories have been setup in current directory"""
       #if system is installed there you should be an archive directory
        if os.path.isdir("archive") == False:
            return 'Sys_NA'
        else:
            return 'Sys_A'
    def builddlist(self):
        """add the directory list for the session"""
        # assume that the current working directory is still
        # the correct directory
        # count the number of directories succesffuly built
        if self.is_installed() == True:
            return 0
        numdirs = 0
        for dirstr in self.dirdict.values():
            try:
                os.mkdir(dirstr)
            except Exception:
                return numdirs
            numdirs += 1
        self.add_to_log("Number of directories created %i " % numdirs)
        return numdirs
    def addsessionarchive(self, specialtag=None):
        """ creates subdirectory in current archive directory"""
        if self.sessionarchive != None:
            return self.sessionarchive
        print('*** the data')
        print(self.thedate)
        subdir = "session" + self.thedate
        if self.theuser != None:
            subdir = subdir + self.theuser
        # may be more than one by None that data
        else:
            subdir = subdir + "anon" + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        if specialtag != None:
            subdir = subdir + specialtag
        try:
            os.chdir('archive')
        except Exception:
            print('in CasmimPY.addsessionarchive()')
            print('os.chdir failed')
            print('archive not found in %s' %os.getcwd())
            sys.exit(0)
        print('*** addsession ->')
        print(subdir)
        try:
            os.mkdir(subdir)
        except Exception:
            print(subdir)
            print(self.mes_dict['F_exists'])
        os.chdir(subdir)
        self.sessionarchive = os.getcwd()
        #pickle the pandas file for the session
        self.TheDataFrame = DataFrame()
        self.TheDataFrame.to_pickle('thepandas')
        os.chdir(self.cwd)
        return self.sessionarchive
    def removelist(self, areyousure=False):
        """takedown all the directories"""
        numdirs = 0
        print("*** are you sure = %s" %areyousure)
        if areyousure == False:
            return numdirs
        for dirstr in self.dirdict.values():
            try:
                shutil.rmtree(dirstr)
                print(dirstr)
            except Exception:
                print(self.mes_dict['RL_Fail'])
                return numdirs
            numdirs += 1
        return numdirs
    def archiveloadedmatrices(self):
        print("Not yet implemented")
    def archivelogfile(self):
        print("Not yet implemented")

def setup_CansimPY(getconfirm=True,user=None):
    """setup directories"""
    # python consule should be run    
    #glob is to determine if central data base is present
    #if yes then conclude 
    testloaded = glob.glob("Central_data.h5")
    if len(testloaded) > 0:
        return 'Sys_Loaded'
    # create file in write mode
    f = h5py.File("Central_data.h5",'w')
    f.close()        
    # directories are not yet available
    CansimPYSession = CansimPY(user,setup=False)
   
    if CansimPYSession.is_installed() == True:
        print(CansimPYSession.mes_dict['Sys_A'])
        return 'Sys_A'
    theresp = 'Y'
    if getconfirm == True:
        theQ = "Build directories in " + CansimPYSession.cwd + "\nY or N\n"
        print(theQ)
        theresp = input("->")
    if theresp.upper() != "Y":
        print(CansimPYSession.mes_dict['Init_Halt'])
        return 'Init_Halt'
    #if environment has already been setup prompt to continue
    #existence of 
    CansimPYSession.builddlist()
    CansimPYSession.addsessionarchive(specialtag='Initialization')
    
    return CansimPYSession
 
def startup_CansimPY(user=None):
    print("*** in startup 0.5")
    
    #if system is installed there you should be an archive directory
    print("*** in startup 1")
    if os.path.isdir("archive") == False:
        print("system not installed \n")
        return 'Sys_NA'
    #need to check if a session has already started
    print("*** in startup 1.5")
    listdir = dir()
    if 'CansimPYSession' in listdir:
        return 'Sys_Loaded'
    print("*** in startup 2")
    #setup = True shows that base directories should be available
    CansimPYSession = CansimPY(user,setup=True)
    CansimPYSession.add_to_log( CansimPYSession.mes_dict['Ses_start'])
    CansimPYSession.status = 'Ses_Start'
    return CansimPYSession
        

        
            
def unittest():
    # will want to use test directory for test
    # move up a directory
    os.chdir(os.path.dirname(os.getcwd()))
    os.chdir('test2')
    #clean up directory for each test
    global CansimPYSession
    try:
        print('*** in the try remove list')
        CansimPYSession.removelist(areyousure=True)
    except:
        print("remove list failed")
    try:
        del(CansimPYSession)
    except:
        print("delete session failed")
    import unittest.mock as mock
    # retval = setup_CansimPY()
    # assert retval == 'Ses_Start',"retval = %s" % retval

    with mock.patch('builtins.input', return_value='N'):
        assert setup_CansimPY() == 'Init_Halt', "Did not process prompt properly"
    with mock.patch('builtins.input', return_value='N'):
        assert setup_CansimPY(getconfirm = True) == 'Init_Halt', "Did not process prompt properly"    
    with mock.patch('builtins.input', return_value='N'):
        assert setup_CansimPY(getconfirm = False) != 'Init_Halt', "Did not process prompt properly"  
    with mock.patch('builtins.input', return_value='Y'):
        assert setup_CansimPY() == 'Sys_Setup', "Did not process prompt properly"  
    # user field should be empty
    assert CansimPYSession.theuser == None , "Did not default name"
    #need to validate the setup procedure by the number of directories that are
    # are taken down
    numdir = len(CansimPYSession.dirdict)
    # should fail
    retval = CansimPYSession.removelist(areyousure=False)
    assert retval == 0 , "removelist should have failed"
    # should fail
    retval = CansimPYSession.removelist()
    assert retval == 0 , "removelist should have failed"
    # should succeed
    retval = CansimPYSession.removelist(areyousure=True)
    assert retval == numdir , "removelist should returned %i" %numdir
    # put the directory up and leave for inspection
    with mock.patch('builtins.input', return_value='Y'):
        assert setup_CansimPY(user='George') == 'Sys_Setup', "Did not process prompt properly"     
    assert CansimPYSession.theuser == "George", "Name stored incorrectly"
    
    # result is unpredictable but should be very small
    rettime = CansimPYSession.timelapsed()
    assert rettime < 1 , "time elapsed is %s"  %str(rettime)
    # need to close the logfile for inspection
    CansimPYSession.currentlog.close()
    # then move back to directory
    # move up a directory
    os.chdir(os.path.dirname(os.getcwd()))
    os.chdir('test2')
    
if __name__ == '__main__':
    stdin = sys.stdin
    unittest()
    # if assert bombs may messup consule
    sys.stdin = stdin
    print("Unit Test was successfull\n")
    
       