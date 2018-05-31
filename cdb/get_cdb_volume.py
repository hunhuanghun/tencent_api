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
    db_items = []
    
    def __init__(self, pro_name, dblist):
        self.projectName = pro_name
        self.dblist = dblist
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
        if self.projectName is None:
            self.projectId = None
        else:
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
            req.Vips = self.dblist
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

def format_dbip(dbfile):
    dblist=[]
    with open(dbfile) as f:
        tmpf = f.readlines()
        for line in tmpf:
           dblist.append(line.strip(" ").strip('\n'))
    return dblist

def main():
    if len(sys.argv) != 2:
        print 'Usage: python %s [projectName|dblistfile]' % sys.argv[0]
        return 1
    var = sys.argv[1]
    projectname = None
    dblist = []
    if os.path.exists(var):
        dblist = format_dbip(var)
    else:
        projectname = var
    try:
        cdb = Cdb(projectname, dblist)
        cdb.get_Cdb_Volume()
    except Exception as e:
        print e
        return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())
