package com.ruozedata.etl.actiton.ads

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


/**
 * @theme ods
 * @author 阿左
 * @create 2022-04-15
 * 考虑问题：
 *  1、作业是否有shuffle？ no
 *  2、作业的并行度是多少？输入输出文件个数。coalase 如何确传入的分区数？
 *  3、如何通过调度执行？ 根据输入路径的分区
 *  4、原始数据去重？ 有shuffle
 *  5、重跑？*** 修数据+数据路径输出错误..(1、多次运行数据，数据正确性)
 *      a：使用append方式，写入（前后），将原来的数据删除。
 *      b：使用overwrite方式，将需要的数据保留手动保存，不想要的数据不管，直接被overwrite。
 *  6、hive建分区表。写开始就准备好，表结构与schema匹配。（parquet中包含schema）。维护分区信息
 *
 *  作业是否支持重跑？只要是OverWrite 对应的分区，就支持重跑。Q：如下使用是否是overwrite对应分区？
 *  insertinto API支持自动建表吗？不支持，saveAsTable支持
 *  默认并行度多少？默认spark.sql.shuffle.partition = 200
 *  小文件
 *  数据倾斜问题(key打散)
 *
 * */
object SparkETLdwd2adsV3 {
    private val logger: Logger = LoggerFactory.getLogger("SparkETLdwd2ads")

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "hadoop")
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkETLdwd2ads")
        //改造yarn
//        val sparkConf = new SparkConf().setAppName("SparkETLdwd2ads")

        val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
//        val inputPath = spark.sqlContext.getConf("spark.app.inputPath")
//        val outputPath = spark.sqlContext.getConf("spark.app.outputPath")
//        val blockSize = spark.sqlContext.getConf("spark.app.blockSize","128").toInt
//        val perBlockRowsForSnappy = spark.sqlContext.getConf("spark.app.perBlockRowsForSnappy","2500000").toInt
//        val printSchema = spark.sqlContext.getConf("spark.app.printSchema","false").toBoolean

//        ETLParse(spark, inputPath, outputPath, blockSize,
//            perBlockRowsForSnappy, printSchema)

        // 本地测试
        ETLParse(spark, "/hadoop-dwV2/dw/dwd/access/d=20220423",
            "/hadoop-dwV2/dw/ads/access/d=20220420",128,2500000,true)
    }

    /**
     * 将ods层的数据扩展成dwd层的大宽表
     *
     * @param sc             SparkContext
     * @param ipResourcePath ip地址库
     * @param inputPath      ods层数据
     * @param output         dwd层数据
     */
    def ETLParse(spark: SparkSession, inputPath: String, outputPath: String,
                 blockSize :Int, perBlockRowsForSnappy :Int, printSchema : Boolean){

        val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(hadoopConfiguration)

        val dwdAccessDf: DataFrame = spark.read.parquet(inputPath).coalesce(MakeCoalesce(fs, inputPath, blockSize))
        dwdAccessDf.createTempView("dwd_access")
        val inputRows = dwdAccessDf.count()

        val resultDf: DataFrame = spark.sql(
            """
              |select
              |city, host, count(1) count
              |from dwd_access
              |group by city, host
              |""".stripMargin)

        if (printSchema){
            resultDf.printSchema()
            resultDf.show(55)
        }

        val outputRows = resultDf.count()
        resultDf.cache()
        resultDf.coalesce(MakeOutputCoalesce(perBlockRowsForSnappy, outputRows, blockSize))

//        resultDf.write.mode(SaveMode.Overwrite)
//                .format("parquet")
//                .save(outputPath)

        // 开启enableHiveSupport，那么在hive中创建了相关表结构之后可以直接采用api进行数据插入。
        // df的schema必须与hive表中的schema匹配，输出格式自动匹配hive表的存储格式。
        resultDf.write.mode(SaveMode.Overwrite)
                .insertInto("ads_areatrafstat")

        // 刷元数据 alter table add partition (d=xxx)
//        val outputPathStrings = outputPath.split("/")
//        val partitionByDay = outputPathStrings(outputPathStrings.length-1).substring(2,outputPathStrings(outputPathStrings.length-1).length)  ///hadoop-dwV2/dw/ads/access/d=20220426
//        spark.sql(s"alter table hadoop_dwv2.ods_areatrafstat add if not exists partition (d=$partitionByDay)")

        logger.info(s"ads层access表维度数据处理完成，共处理条数：$inputRows,输出条数$outputRows")
        spark.stop()
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



        def getOdsAccessSchema(): StructType={
        val odsAccessSchema = StructType(
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
            StructField("province", StringType) ::
            StructField("city", StringType) ::
            StructField("isp", StringType) ::
            StructField("protocol", StringType) ::
            StructField("host", StringType) ::
            StructField("path", StringType) ::
            StructField("params", StringType) ::
            StructField("year", StringType) ::
            StructField("month", StringType) ::
            StructField("day", StringType) :: Nil
        )
        odsAccessSchema
    }
}
