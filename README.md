# 调用腾讯云sdk实现一些基础功能

## 1. 获取cdb的硬盘使用量

### 1) 通过指定“项目名称”，获取该项目下所有的cdb实例的硬盘使用量
python get_cdb_volume.py 项目名称<br>
### 2) 通过已知的cdb实例的内网ip，获取其实例的硬盘使用量
python get_cdb_volume.py cdbipfile<br>
注意：cdbipfile 为文件名，文件内容格式如下:<br>
10.0.0.1<br>
10.0.0.2<br>
10.0.0.3<br>

## 2. 获取cdb当天全备的下载链接，并下载到本地
python get_cdb_backups.py 项目名称
