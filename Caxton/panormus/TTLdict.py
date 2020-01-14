from datetime import datetime,timedelta
from collections import OrderedDict
import pandas as pd
import re
import os


EDB_timezone = [
    'UTC',
    'America/NewYork',
    'Europe/Berlin',
    'Asia/HongKong',
    'Africa/Johannesburg',
    'Asia/Tokyo',
    'US/Eastern',
]

FREQUENCY_OPTIONS = [
    'hourly',
    'daily',
    'monthly',
    'quarterly',
    'annual',
    'unknown',
]

class TTLedb:
    # time to live economic database
    # metadata and parameters derived from the data
    # to enable multi-process update(multi-process when running the strategies), lock is created
    def __init__(self,ttl_hour=5,**kwargs):
        self.data = OrderedDict()
        self.last_update_timestamp = OrderedDict()
        self.expiry = OrderedDict()
        self.tz = OrderedDict()
        self.actual_release_date = OrderedDict()
        self.ttl_hour = ttl_hour
        self.DBLockedHandle=None #enable multi accessing

    def check_timeout(self,ttl_hour,last_load_time):
        if datetime.timestamp(datetime.now() - timedelta(hours=ttl_hour))>last_load_time:
            return 'reload'
        else:
            return 'use_cache'

    def delete_key(self,k):
        del self.data[k]
        del self.last_update_timestamp[k]
        del self.expiry[k]
        del self.tz[k]
        del self.actual_release_date[k]

    def check_all_timeout(self):
        ttl_hour = self.ttl_hour
        for k in self.data.keys():
            if self.check_timeout(ttl_hour,self.last_update_timestamp[k]) in ['reload']:
                self.delete_key(k)

    def set_ttl(self,hour):
        self.ttl_hour=hour
        print ('reset time to live to : ',hour, 'h')

    def check_exist(self,key):
        if key in self.data.keys():
            return True
        else:
            return False

    def import_to_db(self,key,data,time_stamp,tz='UTC',actual_release_date = None,**kwargs):
        pass

    def export(self,Format,StartDate):
        pass

    def bool_false_check(self,value):
        _bool_false = ['false', '0']
        if value.lower() in _bool_false:
            value = ''
        return value

    def get_data(self,k):
        return self.data[k]

class datakey_class:
    def __init__(self,fullkey,dfname,HDFStoreName,DF=None,Meta=None,updateDate = None):
        self.fullkey = fullkey
        self.dfname = dfname
        self.DB_name = HDFStoreName.replace('/','\\')
        self.DBLockedHandle = None
        self.DF = None
        self.dicParams = None
        self.dicMeta = None
        self.dicExports = None

        if isinstance(DF,pd.DataFrame):
            self.DF = DF
            self.dicParams = {}
            self.extractParamsFromDF()
            self.setLastUpdate(updateDate)

            if isinstance(Meta,dict):
                self.dicMeta = Meta
            else:
                self.dicMeta = {}

            self.setAllExports({})
            self.storeDatasetAs(self.fullkey,checkExist=True)

    def loadParams(self):
        self._loadParams()
        if not 'Shape' in self.dicParams.keys():
            self._loadDF()
            self.updateParams()

    def getParams(self):
        '''
        :return: param attribute
        '''
        if self.dicParams in [None]:
            self.loadParams()
        return self.dicParams

    def _checkType(self,typecolumns,dfcolumns):
        true_counter = 0
        _,cols = self.dicParams['Shape']
        if cols == len(typecolumns):
            for i in typecolumns:
                if i in dfcolumns:
                    true_counter+=1
        return true_counter == cols

    def setLastUpdate(self,lastDtTime):
        if self.dicParams in [None]:
            self.loadParams()

        self.dicParams['lastUpdate'] = lastDtTime

    def getLastUpdate(self):
        if self.dicParams in [None]:
            self.loadParams()
        return self.dicParams['lastUpdate'] if 'lastUpdate' in self.dicParams.keys() else None

    def updateParams(self):
        previousUpdate = self.getLastUpdate()
        self.dicParams = {}
        self.setLastUpdate(previousUpdate)
        self.extractParamsFromDF()

    def extractParamsFromDF(self):
        if isinstance(self.DF,pd.DataFrame):
            self.dicParams['Shape']=self.DF.shape
            self.dicParams['First']=self.DF.index.min()
            self.dicParamsp['Last']=self.DF.index.max()
        else:
            raise KeyError('Can not update parameters; Dataframe object missing')

    def getShapeString(self):
        if (self.dicParams == None) or ('Shape' not in self.dicParams):
            self.loadParams()
        (self.rows,self.cols) = self.dicParams['Shape']
        return str(self.rows) + '/' +str(self.cols)

    ###################### Meta ####################
    def setMeta(self,metaDictionary):
        self.dicMeta = metaDictionary

    def deleteMeta(self):
        self.dicMeta = {}

    def getMeta(self):
        if self.dicMeta == None:
            self.loadMeta()
        return self.dicMeta

    def setExportsUpToDate(self,flag=False):
        if self.dicMeta == None:
            self.loadMeta()
        self.dicMeta['ExportsUpToDate'] = flag

    def loadMeta(self):
        self._loadMeta()

    ################ Exports ####################
    def loadExports(self):
        self._loadExports()

    def setAllExports(self,exportsDictionary):
        self.dicExports = exportsDictionary

    def deleteAllExports(self):
        self.dicExports={}

    ################ DataFrame ##############
    def getDF(self):
        if not isinstance(self.DF,pd.DataFrame):
            self._loadDF()
        return self.DF

    def setDF(self,DF):
        self.DF = DF
        self.updateParams()

    def deleteDF(self):
        self.DF = None

    ############## DB access ################
    def setDBStore(self,DB):
        self.DBLockedHandle=DB

    def delDBStore(self):
        self.DBLockedHandle = None

    def _DB_set_locked(self,DBName):
        myBase,_=os.path.splitext(DBName)
        self.lockFileName = myBase+".lck"

        self.lockFile = open(self.lockFileName,'w')
        self.lockFile.write(os.environ.get("USERNAME"))
        self.lockFile.close()
        self.lockFile = open(self.lockFileName,'a')

    def _DB_set_unlocked(self):
        self.lockFile.close()
        os.remove(self.lockFileName)

    def _loadMeta(self):
        self.dicMeta={}

    def _loadParams(self):
        self._loadDF()
        self.updateParams()

    def _loadExports(self):
        self.dicExports={}

    def _loadDF(self):
        if self.DBLockedHandle == None:
            with pd.HDFStore(self.DB_name,mode='r') as DB:
                self.DF = DB.get(self.fullkey)
        else:
            self.DF = self.DBLockedHandle.get(self.fullkey)

    def updateDataset(self):
        self.getDF()
        self.getMeta()
        self.getParams()
        self.getAllExports()

        self._DB_set_locked(self.DB_name)
        self.deleteDataset_NL()
        self.storeDatasetAs_NL(self.fullkey,False)

        #unlock
        self._DB_set_unlocked()

    def updateDataset_NL(self):
        # update with no lock
        self.getDF()
        self.getMeta()
        self.getParams()
        self.getAllExports()

        self.deleteDataset_NL()
        self.storeDatasetAs_NL(self.fullkey,False)

    def renameDataset(self,newKey):
        self.getDF()
        self.getMeta()
        self.getParams()
        self.getAllExports()
        self.storeDatasetAs(newKey)
        self.deleteDataset()

    def _delDataset(self,DB):
        del DB[self.fullkey]

    def deleteDataset_NL(self):
        pass




