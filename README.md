## nb3_import数据导入工具

主要目标：并行执行导入navicat导出的数据库备份nb3文件

### 分析nb3文件

nb3文件是一个压缩包文件解压后目录情况

```
e4fbe599-67a8-4e6b-93ee-c7753b47d242.data.00000.sql.gz
e4fbe599-67a8-4e6b-93ee-c7753b47d242.meta.json.gz
4e4d12b7-a310-4118-9e55-c3118c18deec.data.00000.sql.gz
4e4d12b7-a310-4118-9e55-c3118c18deec.meta.json.gz
87975da1-c4ec-41e5-8b13-5ee7a6ac5741.meta.json.gz
meta.json

```

具体解析如下：

1. meta.json 是一个总体的描述文件 包含每个表对应的描述文件和 导出的数据库类型 Schema名称等
2. 87975da1-c4ec-41e5-8b13-5ee7a6ac5741.meta.json.gz 解压后文件是对应的每个表的描述文件 包含DDL 和字段信息 以及数据文件的名称
3. e4fbe599-67a8-4e6b-93ee-c7753b47d242.data.00000.sql.gz 这一类就是表数据文件


### 程序处理过程

1. 解压nb3文件
2. 解析meta.json文件 分析需要处理的表信息
3. 建立连接池，并行处理表结构创建和数据导入， 校验并输出导入行数情况
4. 关闭连接池结束程序