说明：数据原始存储位置在raw文件夹下，存储格式text。
hive原始数据层：由SparkETL2odsV2读取去除脏数据，保存至ods层，格式为parquet；
hive dwd层：由SparkETLods2dwdV2读取解析并关联相关字段，成为大宽表DWD层，格式为parquet；
hive ads层，由SparkETLdwd2adsV2读取，根据业务需要提取、join等操作形成ADS层，格式为parquet；


ODS层：
create external table ods_access(
time string,
ip string,
proxyIp string,
responseTime string,
referer string,
method string,
url string,
httpCode string,
requestSize string,
responseSize bigint,
cache string)
partitioned by (d string)
stored as parquet location "/hadoop-dwV2/dw/ods/access";


./bin/spark-submit \
--master yarn \
--deploy-mode client \
--class com.ruozedata.etl.actiton.ods.SparkETL2odsV2 \
--driver-memory 1g \
--executor-memory 1g \
--num-executors 1 \
--executor-cores 1 \
--verbose \
--conf spark.app.inputPath=/hadoop-dwV2/dw/raw/access/20220423 \
--conf spark.app.outputPath=/hadoop-dwV2/dw/ods/access/d=20220423 \
--conf spark.app.blockSize=128 \
/home/hadoop/app/hadoop-dwV2/lib/SparkOfflineWarehouse-2.0.jar


DWD层：
create external table dwd_access(
ip string,
proxyIp string,
responseTime bigint,
referer string,
method string,
url string,
httpCode string,
requestSize bigint,
responseSize bigint,
cache string,
province string,
city string,
isp string,
protocol string,
host string,
path string,
params string,
year string,
month string,
day string
)
partitioned by (d string)
stored as parquet location '/hadoop-dwV2/dw/dwd/access';

./bin/spark-submit \
--master yarn \
--deploy-mode client \
--class com.ruozedata.etl.actiton.dwd.SparkETLods2dwdV2 \
--driver-memory 1g \
--executor-memory 3g \
--num-executors 1 \
--executor-cores 1 \
--verbose \
--conf spark.app.ipResourcePath=/hadoop-dwV2/lib/ip.txt \
--conf spark.app.inputPath=/hadoop-dwV2/dw/ods/access/d=20220423 \
--conf spark.app.outputPath=/hadoop-dwV2/dw/dwd/access/d=20220423 \
--conf spark.app.blockSize=128 \
--conf spark.app.perBlockRowsForSnappy=5000000 \
--conf spark.app.printSchema=true \
/home/hadoop/app/hadoop-dwV2/lib/SparkOfflineWarehouse-2.0.jar



ADS层：
./bin/spark-submit \
--master yarn \
--deploy-mode client \
--class com.ruozedata.etl.actiton.ads.SparkETLdwd2adsV2 \
--driver-memory 1g \
--executor-memory 1g \
--num-executors 1 \
--executor-cores 1 \
--verbose \
--conf spark.app.inputPath=/hadoop-dwV2/dw/dwd/access/d=20220423 \
--conf spark.app.outputPath=/hadoop-dwV2/dw/ads/access/d=20220423 \
--conf spark.app.blockSize=128 \
--conf spark.app.perBlockRowsForSnappy=5000000 \
--conf spark.app.printSchema=true \
/home/hadoop/app/hadoop-dwV2/lib/SparkOfflineWarehouse-2.0.jar

create external table ads_areatrafstat(
city string,
host string,
count bigint
)
partitioned by (d string)
stored as parquet location '/hadoop-dwV2/dw/ad/access';

// 刷新元数据
alter table ads_areatrafstat add if not exists partition(d=20220423);
