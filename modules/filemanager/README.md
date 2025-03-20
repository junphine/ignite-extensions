# filemanager

## 介绍 

为Ignite的IGFS提供S3协议的服务。
同时提供了Angular File Manage的UI。

## 环境搭建

| 软件 | 版本  | 功能|   地址|
| ---- | ----- |----- |----- |
|  angular file manager | 3.2.2 |  前端对象管理插件 | https://github.com/joni2back/angular-filemanager |
|  spreadsheet | 3.2.2 |  前端CSV文件编辑插件 | https://github.com/myliang/x-spreadsheet.git |
|  spring-boot | 2.7.7.RELEASE |  后端对象服务框架| https://spring.io/projects/spring-boot |

### angular file manager 
	需要部署到Ignite Web Console的前端项目下
	自定义后端服务地址：通过设置localStorage.currentStorageHost来改变默认的后端地址
	
### spring-boot 
	需要部署到启用了Igfs的Ignit集群节点下



## 功能模块

- 上传文件，下载文件
- 多人在线编辑CSV
- 设置文件权限



