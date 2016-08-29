# BigdataApp1
这个项目是一个改写Oracle存储过程为SparkApp的一个应用。
主要的功能：syrk里面是统计，数据质量检查等，jf里面是人员积分。

主要思路是每个实体的各项指标join在一起，然后map。
性能高于游标中不断select。
详见jf里面的存储过程的sql文件。

主要使用了SparkSql 和SparkCore库
PS：DBlink信息删去了

没有数据，这个项目就是拿来看看吧。
