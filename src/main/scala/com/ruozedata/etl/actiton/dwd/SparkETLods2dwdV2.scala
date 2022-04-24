package com.ruozedata.etl.actiton.dwd

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URL

/**
 * @theme 日志解析类，ods2dwd，以parquet保存，df进行转换
 * @author 阿左
 * @create 2022-04-15
 * */
object SparkETLods2dwdV2 extends SparkListener{

    private val logger: Logger = LoggerFactory.getLogger("SparkETLods2dwdV2")

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "hadoop")
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkETL")
//        val sparkConf = new SparkConf().setAppName("SparkETL")

        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//        val ipResourcePath = spark.sqlContext.getConf("spark.app.ipResourcePath")
//        val inputPath = spark.sqlContext.getConf("spark.app.inputPath")
//        val outputPath = spark.sqlContext.getConf("spark.app.outputPath")
//        val blockSize = spark.sqlContext.getConf("spark.app.blockSize","128").toInt
//        val perBlockRowsForSnappy = spark.sqlContext.getConf("spark.app.perBlockRowsForSnappy","2500000").toInt
//        val printSchema = spark.sqlContext.getConf("spark.app.printSchema","false").toBoolean


//        ETLParse(spark, ipResourcePath, inputPath, outputPath, blockSize,
//            perBlockRowsForSnappy, printSchema)

//         本地测试/hadoop-dwV2/lib /hadoop-dwV2/ods /hadoop-dwV2/dwd
                ETLParse(spark, "/hadoop-dwV2/lib/ip.txt",
                    "/hadoop-dwV2/dw/ods/access/d=20220423/part*",
                    "/hadoop-dwV2/dw/dwd/accesstt2/d=20220423",
                    128,2500000,true)

    }

    /**
     * 将ods层的数据扩展成dwd层的大宽表
     * @param spark spark统一入口点
     * @param ipResourcePath ipku
     * @param inputPath 输入ods层表位置
     * @param outputPath 输出dwd层表位置
     * @param blockSize hdfs块大小
     * @param perBlockRowsForSnappy 采用snappy个数输出，一个块所存放的条数
     * @param printSchema 是否打印表的schema
     */
    def ETLParse(spark: SparkSession, ipResourcePath: String, inputPath: String, outputPath: String,
                 blockSize :Int, perBlockRowsForSnappy :Int, printSchema : Boolean){

        import spark.implicits._
        val sc = spark.sparkContext

        val lines_ipResource: RDD[String] = sc.textFile(ipResourcePath).coalesce(1)
        //
        val ipInfoRow: RDD[Row] = lines_ipResource.map(x => {
            val line = x.split("\\|")
            val ip = line(0)
            val province = line(6)
            val city = line(7)
            val isp = line(9)
            Row(ip, province, city, isp)
        })

        val ipInfoDf = spark.createDataFrame(ipInfoRow, getIpInfoSchema())

        ipInfoDf.createTempView("ipInfo")
        ipInfoDf.cache() //缓存DF
        if (printSchema) {
            ipInfoDf.printSchema()
        }

        // 处理的dwd层access表
        val hadoopConfiguration = sc.hadoopConfiguration
        val fs = FileSystem.get(hadoopConfiguration)
        val odsAccessDf: DataFrame = spark.read.parquet(inputPath).coalesce(MakeInputCoalesce(fs, inputPath, blockSize))
        if (printSchema) {
            odsAccessDf.printSchema()
        }

        val odsAccessExtendRow: RDD[Row] = odsAccessDf.rdd.mapPartitions(partition => {
            val rows: Iterator[Row] = partition.map(record => {
                val time: String = record.getString(0)
                val ip: String = record.getString(1)
                val proxyIp: String = record.getString(2)

                var responseTime = 0L
                try {
                    responseTime = record.getString(3).toLong
                } catch {
                    case e: Exception
                    => {
                        logger.error("responseTime 转换数值类型异常，数据可能保留")
                    }
                }

                val referer: String = record.getString(4)
                val method: String = record.getString(5)
                val url: String = record.getString(6)
                val httpCode: String = record.getString(7)

                var requestSize = 0L
                try {// 上层已经处理过该字段，这里可以不作处理
                    requestSize = record.getString(8).toLong
                } catch {
                    case e: Exception
                    => {
                        logger.error("requestSize 转换数值类型异常，数据可能保留")
                    }
                }

                // 如果该字段转换异常，那么标记-1，后续根据标记剔除
                var responseSize = 0L
                try {
                    responseSize = record.getLong(9)
                } catch {
                    case e: Exception
                    => {
                        responseSize = -1
                        logger.error("responseSize 转换数值类型异常，数据丢弃" + record.getString(9))
                    }
                }

                val cache: String = record.getString(10)

                // 字段解析
                val timeExtend = timeParse(time).split("\\t") //s"$year-$month-$day"
                val year = timeExtend(0)
                val month = timeExtend(1)
                val day = timeExtend(2)

                val urlExtend = urlParse(url).split("\\t") //s"$protocol-$host-$path-$params"
                val protocol = urlExtend(0)
                val host = urlExtend(1)
                val path = urlExtend(2)
                val params = urlExtend(3)
                Row(time, ip, proxyIp, responseTime, referer, method, url, httpCode, requestSize,
                    responseSize, cache, protocol, host, path, params, year, month, day)
            })
            rows
        })

        val preDwdAccessDf = spark.createDataFrame(odsAccessExtendRow, getOdsAccessExtendSchema())

        preDwdAccessDf.createTempView("preDwdAccess")
        preDwdAccessDf.cache()  //缓存DF
        val rows = preDwdAccessDf.count()

        val dwdAccessDf: DataFrame = spark.sql(
            """
              |
              |select
              |a.ip, a.proxyIp, a.responseTime, a.referer, a.method, a.url, a.httpCode, a.requestSize,
              |a.responseSize, a.cache, i.province, i.city, i.isp, a.protocol, a.host, a.path,
              |a.params, a.year, a.month, a.day
              |from preDwdAccess a
              |left join ipInfo i
              |on a.ip = i.ip
              |""".stripMargin).coalesce(MakeOutputCoalesce(perBlockRowsForSnappy, rows, blockSize))

        if (printSchema) {
            dwdAccessDf.printSchema()
        }

        // 平均一条数据大小1540
        dwdAccessDf.write.mode(SaveMode.Overwrite)
                .format("parquet")
                .save(outputPath)

        logger.info(s"dwd层access表数据处理完成，共处理条数：$rows")
        Thread.sleep(Int.MaxValue)
        spark.stop()
    }

    /**
     *  计算输入分区数
     * @param fs hadoop文件系统
     * @param inputPath 输入路径
     * @param blockSize 分区大小，默认128M
     * @return
     */
    def MakeInputCoalesce(fs :FileSystem, inputPath :String, blockSize :Int): Int={
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

    /**
     * 计算输出分区数，按text大小计算，即parquet压缩前
     * @param perSize 平均一条数据大小
     * @param rows 总条数
     * @param blockSize 块大小，默认128M
     * @return
     */
    def MakeOutputCoalesce(perBlockRowsForSnappy : Int, rows :Long, blockSize :Int): Int={
        var partitions = 0
        // 采用parquet保存，对perSize参数进行优化
        val perSnappySize = rows / perBlockRowsForSnappy
        partitions =( perSnappySize / blockSize ).toInt

        if(partitions == 0) {
            logger.info(s"rdd的分区数为：1")
            1
        }else{
            logger.info(s"rdd的分区数为：$partitions")
            partitions
        }
    }

    def getIpInfoSchema(): StructType={
        val ipInfoSchema = StructType(
            StructField("ip", StringType) ::
            StructField("province", StringType) ::
            StructField("city", StringType) ::
            StructField("isp", StringType) :: Nil
        )
        ipInfoSchema
    }

    def getOdsAccessExtendSchema(): StructType={
        val odsAccessExtendSchema = StructType(
            StructField("time", StringType) ::
            StructField("ip", StringType) ::
            StructField("proxyIp", StringType) ::
            StructField("responseTime", LongType) ::
            StructField("referer", StringType) ::
            StructField("method", StringType) ::
            StructField("url", StringType) ::
            StructField("httpCode", StringType) ::
            StructField("requestSize", LongType) ::
            StructField("responseSize", LongType) ::
            StructField("cache", StringType) ::
            StructField("protocol", StringType) ::
            StructField("host", StringType) ::
            StructField("path", StringType) ::
            StructField("params", StringType) ::
            StructField("year", StringType) ::
            StructField("month", StringType) ::
            StructField("day", StringType) :: Nil
        )
        odsAccessExtendSchema
    }


    //解析时间
    def timeParse(time: String): String = {
        //[15/04/2022:15:00:12 +0800]
        var res = ""

        val timeSplits = time.substring(1, time.length - 1).split(" ")
        val yearMonthDay = timeSplits(0).split("/")
        val day = yearMonthDay(0)
        val month = yearMonthDay(1)
        val year = yearMonthDay(2)

        res = s"$year\t$month\t$day"
        res
    }

    // 解析url字段
    def urlParse(url: String): String = {
        //http://www.ruozedata.com/video/av17651597
        var res = ""
        var host = ""
        var path = ""
        var params = ""
        var protocol = ""

        val u = new URL(url)
        host = if (u.getHost != null) u.getHost else "-"
        path = if (u.getPath != null) u.getPath else "-"
        params = if (u.getQuery != null) u.getQuery else "-"
        protocol = if (u.getProtocol != null) u.getProtocol else "-"

        res = s"$protocol\t$host\t$path\t$params"
        res
    }
}
