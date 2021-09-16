# vn.py框架的InfluxDB数据库接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg" />
</p>

## 说明

InfluxDB数据库接口。

## 使用

InfluxDB在VN Trader中配置时，需要填写以下字段信息：

| 字段名            | 值 |
|---------           |---- |
|database.driver     | "influxdb" |
|database.host       | 地址|
|database.port       | 端口|
|database.database   | 数据库名| 
|database.user       | 用户名| 
|database.password   | 密码| 

 
InfluxDB的例子如下所示：

| 字段名             | 值 |
|---------           |----  |
|database.driver     | influxdb |
|database.host       | localhost |
|database.port       | 8086 |
|database.database   | vnpy |
|database.user       | root |
|database.password   | .... |
|database.authentication_source   | vnpy |

请注意，运行influxd.exe的cmd需要保持运行，如果关闭则会导致InfluxDB退出，或者也可以使用一些辅助工具将其注册为后台运行的Windows服务。
