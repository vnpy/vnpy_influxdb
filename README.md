# VeighNa框架的InfluxDB数据库接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.2-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7|3.8|3.9|3.10-blue.svg" />
</p>

## 说明

基于influxdb-client开发的InfluxDB数据库接口，支持2.0以上版本的InfluxDB。

## 使用

在VeighNa中使用InfluxDB时，需要在全局配置中填写以下字段信息：

|名称|含义|必填|举例|
|---------|----|---|---|
|database.name|名称|是|influxdb|
|database.host|地址|是|localhost|
|database.port|端口|是|8086|
|database.database|Bucket|是|vnpy|
|database.user|Organization|是|root|
|database.password|API Token|是|12345678|
