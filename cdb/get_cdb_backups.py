#!/usr/bin/python
# coding: utf-8
# Author: fiona.li
# Function: get tecent cdb backups url 
# Date: 2018-05-29
# https://github.com/QcloudApi/qcloudapi-sdk-python/
# https://github.com/TencentCloud/tencentcloud-sdk-python

# Requirement:
#	     pip install tencentcloud-sdk-python
#	     pip install ConfigParser
#            pip install wget

import os
import sys
import wget
import json
import time
import tarfile
import pprint
import ftplib
import datetime
import ConfigParser
from QcloudApi.qcloudapi import QcloudApi
from tencentcloud.common import credential
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
# 导入对应产品模块的client models。
from tencentcloud.cdb.v20170320 import cdb_client,models

reload(sys)  
sys.setdefaultencoding('utf8')   


cf = ConfigParser.ConfigParser()
cf.read(".config.ini")

def generate_time():
    endtime = datetime.datetime.now()
    starttime = endtime - datetime.timedelta(minutes=3)
    return starttime.strftime('%Y-%m-%d %H:%M:%S'), endtime.strftime('%Y-%m-%d %H:%M:%S')

class Cdb:
    config = {'Region': 'ap-guangzhou',
              'Version': '2017-03-20',
              'secretId': cf.get('fiona','SecretId').strip("'"),
              'secretKey': cf.get('fiona','SecretKey').strip("'")
             }
    sjh_ftp = {'Host': cf.get('ftp','Host').strip("'"),
               'Port': cf.get('ftp','Port').strip("'"),
               'Username': cf.get('ftp','Username').strip("'"),
               'Password': cf.get('ftp','Password').strip("'"),
               'Backdir': cf.get('ftp','Backdir').strip("'")
              }
    db_items = []
    
    def __init__(self, pro_name):
        self.projectName = pro_name
        self.cred = credential.Credential(self.config['secretId'], self.config['secretKey'])
        self.client = cdb_client.CdbClient(self.cred, self.config['Region'])
        self.cur_date = time.strftime('%Y%m%d', time.localtime())
        cur_dir = os.getcwd()
        if not os.path.exists('%s/log' % cur_dir):
            os.mkdir('%s/log' % cur_dir)
        self.access_log = open('%s/log/access.log' % cur_dir, 'a+')
        self.error_log = open('%s/log/error.log' % cur_dir, 'a+')
        self._get_Projectid()

    # 根据项目名称获取项目id
    def _get_Projectid(self):
        module = 'account'
        action = 'DescribeProject'
        params = {'allList':0}
        service = QcloudApi(module, self.config)
        result = json.loads(service.call(action, params))
        if result['code'] != 0 :
            raise KeyError, 'Error: %s' % result['message']  

        result = result['data']
        tag = 0
        for p in result:
            if p['projectName'] == self.projectName.decode('utf-8') :
                self.projectId = p['projectId']
                tag = 1
        if not tag:
            self.error_log.write('Error: %s project is not exists' % self.projectName)
            sys.exit(1)

    # 获取指定项目下的所有cdb实例id
    def get_Cdb_InstanceId(self):
        try:
            req = models.DescribeDBInstancesRequest()
            req.ProjectId = self.projectId
            req.Offset = 0
            req.Limit = 1
#            req.Vips = ['10.66.246.134', '10.66.114.57']
            resp = self.client.DescribeDBInstances(req)
            TotalCount = resp.TotalCount
            if int(TotalCount) == 0 :
                self.error_log.write("There is no CDB instance under this %s project\n" % self.projectName)
                sys.exit(1)
            req.Limit = 20
            num = TotalCount / req.Limit
            if TotalCount % req.Limit != 0 :
                num +=1
            for i in range(num):
                req.Offset = req.Limit*i
                resp = self.client.DescribeDBInstances(req)
                for item in resp.Items:
                    if not item.InstanceName.startswith('待回收'):
                        dbitem = {}
                        dbitem['InstanceId'] = item.InstanceId
                        dbitem['InstanceName'] = item.InstanceName
                        dbitem['Vip'] = item.Vip
                        dbitem['Volume'] = item.Volume
                        self.db_items.append(dbitem)
        except Exception as e:
            self.error_log.write('%s Error: %s \n'  % (time.strftime('%Y%m%d %H:%M:%S', time.localtime()), e))
            sys.exit(1)

    def get_Cdb_Volume(self):
        module = 'monitor'
        action = 'GetMonitorData'
        metric = ['real_capacity', 'volume_rate']
        params = {'namespace':'qce/cdb', 'metricName': 'real_capacity', 'dimensions.0.name':'uInstanceId', 'period': 60}
        params['startTime'],params['endTime'] = generate_time()
        service = QcloudApi(module, self.config)
        self.get_Cdb_InstanceId()
        for m in metric:
            params['metricName'] = m
            for num, item in enumerate(self.db_items):
                params['dimensions.0.value'] = item['InstanceId']
                service.call(action, params)
                result = json.loads(service.call(action, params))
                if len(result['dataPoints']) != 0 :
                    if m == 'real_capacity' and result['dataPoints'][0] is not None:
                        self.db_items[num][m] = round(float(result['dataPoints'][0])/1024, 2)
                    else:
                        self.db_items[num][m] = result['dataPoints'][0]
                else:
                    self.db_items[num][m] = None
        for item in self.db_items:
            print "%-20s%-15s%-10s%-10s%-10s" % (item['InstanceName'], item['Vip'], item['Volume'], item['real_capacity'], item['volume_rate'])

    # 获取所有的cdb实例的备份url
    def get_DBbak_Url(self):
        try:
            self.get_Cdb_InstanceId()
            for num, item in enumerate(self.db_items):
                req = models.DescribeBackupsRequest()
                req.InstanceId = item['InstanceId']
                resp = self.client.DescribeBackups(req)
                self.cur_date = time.strftime('%Y-%m-%d', time.localtime())
                self.db_items[num]['InternetUrl'] = None
                self.db_items[num]['IntranetUrl'] = None
                tag = 0
                if resp.TotalCount == 0 :
                    self.error_log.write("%s The %s cdb instance has no backup data.\n" % (time.strftime('%Y%m%d %H:%M:%S', time.localtime()), req.InstanceId))
                    continue
                for i in resp.Items:
                    bak_date = i.FinishTime[:10]
                    if self.cur_date == bak_date:
                        self.db_items[num]['InternetUrl'] = i.InternetUrl
                        self.db_items[num]['IntranetUrl'] = i.IntranetUrl
                        tag = 1
                if not tag : 
                    self.error_log.write("%s The %s cdb instance has no %s backup data..\n" % (time.strftime('%Y%m%d %H:%M:%S', time.localtime()), req.InstanceId, self.cur_date ))
        except Exception as e:
            self.error_log.write('%s Error: %s \n'  % (time.strftime('%Y%m%d %H:%M:%S', time.localtime()), e))
            sys.exit(1)

    # 下载cdb备份到本地
    def Download_dbbak(self):
        self.get_DBbak_Url()
        store_dir = os.getcwd()+'/'+'sql'
        self.cdbback = "%s/%s_%s.tar.gz" % (store_dir, self.projectName, self.cur_date)
        if os.path.exists(store_dir):
            os.system("rm -rf %s/*" % store_dir) 
        else:
            os.mkdir(store_dir)   
        try:
            tar_open = tarfile.open(self.cdbback, "w:gz")
            for item in self.db_items:
                if item['InternetUrl'] is not None and item['IntranetUrl'] is not None:
                    out_fname = '%s_%s.sql' % (item['InstanceName'], self.cur_date)
                    wget.download(item['IntranetUrl'], out=store_dir+'/'+out_fname)
                    self.access_log.write('%s The backup data of %s cdb instances is successfully downloaded.\n' % (time.strftime('%Y%m%d %H:%M:%S', time.localtime()), item['InstanceId']))
                    tar_open.add(store_dir+'/'+out_fname)
                    os.remove(store_dir+'/'+out_fname)
                    self.access_log.write('%s %s tar success.\n' % (time.strftime('%Y%m%d %H:%M:%S',time.localtime()), store_dir+'/'+out_fname))
        except Exception as e:
            self.error_log.write('%s Error: %s \n'  % (time.strftime('%Y%m%d %H:%M:%S', time.localtime()), e))
            sys.exit(1)  
        finally:
            tar_open.close()
            

    def Ftp_Upload(self):
        try:
            self.ftp = ftplib.FTP()
            packname = "%s_%s.tar.gz" % (self.projectName, self.cur_date)
            bufsize = 1024
            self.ftp.connect(self.sjh_ftp['Host'],self.sjh_ftp['Port'])
            result = self.ftp.login(self.sjh_ftp['Username'],self.sjh_ftp['Password'])
            if result != '230 Login successful.':
                self.error_log.write('%s Error: %s \n'  % (time.strftime('%Y%m%d %H:%M:%S', time.localtime()),result))
                sys.exit(1)
            result = self.ftp.cwd(self.sjh_ftp['Backdir'])  
            if result != '250 Directory successfully changed.':
                self.error_log.write('%s Error: %s \n'  % (time.strftime('%Y%m%d %H:%M:%S', time.localtime()),result))
                sys.exit(1)
            if len(self.ftp.nlst()) != 0 :
                for item in self.ftp.nlst():
                    if self.cur_date not in item:
                        self.ftp.delete(item)
            fp = open(self.cdbback, 'rb')
            self.ftp.storbinary('STOR ' + packname, fp, bufsize)
            self.access_log.write('%s %s upload ftp success.\n' % (time.strftime('%Y%m%d %H:%M:%S',time.localtime()), packname))
        except Exception, e:
            self.error_log.write('%s Error: %s \n'  % (time.strftime('%Y%m%d %H:%M:%S', time.localtime()), e))
            sys.exit(1)
        finally:
            self.ftp.quit()
        
    def __del__(self):
        self.access_log.close()
        self.error_log.close()
    #    self.ftp.quit()



def main():
    if len(sys.argv) != 2:
        print 'Usage: python %s projectName' % sys.argv[0]
        return 1
    try:
        cdb = Cdb(sys.argv[1])
        cdb.Download_dbbak()
        cdb.Ftp_Upload()
#        cdb.get_Cdb_InstanceId()
#        cdb.get_Cdb_Volume()
    except Exception as e:
        print e
        return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())
