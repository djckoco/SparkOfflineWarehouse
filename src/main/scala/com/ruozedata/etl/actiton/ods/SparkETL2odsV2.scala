package com.ruozedata.etl.actiton.ods

import com.ruozedata.etl.domain.ods.AccessODS
import com.ruozedata.etl.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @theme 原始数据经初级清洗(数据验证)进入数仓ods层，数据存储格式parquet
 * @author 阿左
 * @create 2022-04-15
 * */
object SparkETL2odsV2 {
    private val logger: Logger = LoggerFactory.getLogger("SparkETL2odsV2")

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "hadoop")
        val sparkConf = new SparkConf().setAppName("SparkETL2odsV2")//.setMaster("local[2]")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        logger.error("SparkETL2odsV2:开始啦")

        val inputPath = spark.sqlContext.getConf("spark.app.inputPath")
        val outputPath = spark.sqlContext.getConf("spark.app.outputPath")
        val blockSize = spark.sqlContext.getConf("spark.app.blockSize","128").toInt

        ETLParse(spark , inputPath, outputPath,blockSize)
       //[2022/04/17/:23:08:09 +0800]	222.83.234.112	-	1677	179	GET	http://www.ruozedata.com/video/av34329219	200	152	122	HIT

        //本地测试
        ETLParse(spark , "/hadoop-dwV2/dw/raw/access/20220418",
            "/hadoop-dwV2/dw/ods/accessV2/d=20220418",128)

    }

    def ETLParse(spark: SparkSession, inputPath: String, output: String, blockSize :Int){
        import spark.implicits._
        val sc = spark.sparkContext
        val hadoopConfiguration = sc.hadoopConfiguration
        val fs = FileSystem.get(hadoopConfiguration)

        val lines_input: RDD[String] = sc.textFile(inputPath).coalesce(MakeCoalesce(fs,inputPath,blockSize) )

        val rddRow: RDD[Row] = lines_input.mapPartitions(partition => {
            partition.map(x => { //Executor 中执行
                val line = x.split("\t")
                val time = line(0)
                val ip = line(1)
                val proxyIp = line(2)
                val responseTime = line(3)
                val referer = line(4)
                val method = line(5)
                val url = line(6)
                val httpCode = line(7)
                val requestSize = line(8)

                //此字段产生10% 脏数据。脏数据为"-"
                //如果数据转换异常，直接放弃此条记录
                var responseSize = 0L
                try {
                    responseSize = line(9).toLong
                } catch {
                    case e: NumberFormatException => {
                        //解析错误，标记为-1
                        responseSize = -1
                    }
                }

                val cache = line(10)

                Row(time, ip, proxyIp, responseTime, referer, method,
                    url, httpCode, requestSize, responseSize, cache)
            })
        })
        // 解析数据

        val df: DataFrame = spark.createDataFrame(rddRow, getSchema)
        df.createTempView("tmp")
        val inputNum = df.count()

        val resDf: DataFrame = spark.sql(
            """
              |
              |select *
              |from tmp
              |where responseSize <> -1
              |
              |""".stripMargin)

        resDf.printSchema()
        resDf.show(3)
        val outputNum = resDf.count()

        resDf.write.mode(SaveMode.Overwrite)
                .format("parquet")
                .save(output)

        logger.info(s"inputNum：$inputNum-outputNum$outputNum")
        spark.stop()
    }

    def getSchema:StructType={
        val schema = StructType(
            StructField("time", StringType) ::
                    StructField("ip", StringType) ::
                    StructField("proxyIp", StringType) ::
                    StructField("responseTime", StringType) ::
                    StructField("referer", StringType) ::
                    StructField("method", StringType) ::
                    StructField("url", StringType) ::
                    StructField("httpcode", StringType) ::
                    StructField("requestSize", StringType) ::
                    StructField("responseSize", LongType) ::
                    StructField("cache", StringType) :: Nil
        )
        schema
    }

    /**
     *  计算输入分区数
     * @param fs hadoop文件系统
     * @param inputPath 输入路径
     * @param blockSize 分区大小，默认128M
     * @return
     */
    def MakeCoalesce(fs :FileSystem, inputPath :String, blockSize :Int): Int={
        var fileInptuTotalSize = 0L
        fs.globStatus(new Path(inputPath)).foreach(x =>{
            fileInptuTotalSize = x.getLen + fileInptuTotalSize
        })
        val partitions: Int = (fileInptuTotalSize / 1024 / 1024 / blockSize).toInt
        if(partitions == 0) {
            logger.info(s"rdd的分区数为：1")
            1
        }else{
            logger.info(s"rdd的分区数为：$partitions")
            partitions
        }
    }

}
