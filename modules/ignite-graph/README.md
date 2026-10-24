# IgniteGraph

<p align="center">
  <img src="https://github.com/predictiveworks/ignite-graph/blob/main/images/ignite-graph.png" width="600" alt="IgniteGraph">
</p>


IgniteGraph is a client layer for using Apache Ignite as a graph database. It is 
an implementation of the Apache TinkerPop  interfaces.

## Streaming Ingestion

**IgniteGraph** is a contribution to the **PredictiveWorks.** initiative to leverage Cyber & IoT analytics
side by side. Graph database technology and graph analytics is an excellent fit for this use case.

Connecting IoT device readings on the one hand and e.g. information from Common Vulnerabilities & Exposures
on the other hand, generates insights about potentially malicious behavior of sensors and devices.

## Document vs Vertex 

Vertext: id,label,propKey1,propValue1
         id,label,propKey2,propValue2

Document:id,label,prop1,prop2,prop3

注意：Vertexy的数据模型：
包含一个BigDecmal的数值字段。只存储数值，时间类的值，用来范围查找。


## 技术架构

整个图数据共用一个ignite实例，通过namespace区分

namespace作为数据库模式存在


可以配置多个图实例，第二个图实例的ignite配置文件将不起作为


