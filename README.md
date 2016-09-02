# 大数据应用
* 这个项目是一个改写`Oracle存储过程`为`SparkApp`的一个应用
* 一共改写了9个存储过程

##主要的功能
* syrk里面是`统计`，`数据质量检查`等
* jf里面是`人员积分`
* ps:src/com/triman/bigdata/jf/里面的代码`可读性更好`，最近实现的（P_RSDQ_4.scala的`注释比较详细`）
* 而src/com/triman/bigdata/syrk比较久远了（而且没有配有原来存储过程的sql文件）
* 而且syrk的查询比较复杂，`用了大量的UDF`，因为我不知道hive sql有没有这些函数

##主要思路
* 把每个`实体`的各项指标`join`在一起，然后map
* 这个做法与sql中的`游标中不断select`的在做法不予苟同
（详见jf里面的存储过程的sql文件）
* 但是`性能`上有很大提升

##主要技术
* 主要使用了`SparkSql` 和`SparkCore`库

##关于
* DBlink信息删去了
* `没有数据`，这个项目就是拿来看看吧。

