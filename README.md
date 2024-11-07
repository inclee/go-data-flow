# go-data-flow
go 数据集同步， 清洗工具，可用于日志收集治理，MYSQL数据表实时同步ElasticSearch，更多输出输出 实现对应接口，注册工厂函数即可进行扩展

# 插件化架构
插件化框架，输入，输出数据源，清洗、治理等插件易扩展
## 目前已支持插件
### rename:
日志自动从命名
### combine:
  从已有字段联合成新字段
### delete
  删除字段
### filter
  日志过滤
# 输入输出可扩展
日志输入，输出数据源也是插件化方式扩展，方便进行进一步扩展
## 已支持输入数据源：
MySql Binlog/Kafka
## 已支持输出数据源
ElasticSearch /Kafka/Stdout
