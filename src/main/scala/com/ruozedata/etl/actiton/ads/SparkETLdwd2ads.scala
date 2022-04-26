package com.ruozedata.etl.actiton.ads

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
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
 * */
object SparkETLdwd2ads {
    private val logger: Logger = LoggerFactory.getLogger("SparkETLdwd2ads")

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "hadoop")
//        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkETLdwd2ads")
        //改造yarn
        val sparkConf = new SparkConf().setAppName("SparkETLdwd2ads")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        val inputPath = spark.sqlContext.getConf("spark.app.inputPath")
        val outputPath = spark.sqlContext.getConf("spark.app.outputPath")
        val blockSize = spark.sqlContext.getConf("spark.app.blockSize","128").toInt

       ETLParse(spark, inputPath, outputPath, blockSize)

        // 本地测试
//        ETLParse(spark, "/hadoop-dwV2/dw/dwd/access/d=20220420",
//            "/hadoop-dwV2/dw/ads/access/d=20220420",128)
    }

    /**
     * 将ods层的数据扩展成dwd层的大宽表
     *
     * @param sc             SparkContext
     * @param ipResourcePath ip地址库
     * @param inputPath      ods层数据
     * @param output         dwd层数据
     */
    def ETLParse(spark: SparkSession, inputPath: String, outputPath: String, blockSize :Int){

        import spark.implicits._

//      ResultOfText(spark, input, output)

        val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(hadoopConfiguration)
        val line: RDD[String] = spark.sparkContext.textFile(inputPath).coalesce(MakeCoalesce(fs, inputPath, blockSize))
        val rddRow: RDD[Row] = line.mapPartitions(par => {
            par.map(x => {
                val record = x.split("\t")
                val city = record(11)
                val host = record(14)
                Row(city, host)
            })
        })

        val schema = StructType(
            StructField("city", StringType) ::
            StructField("host", StringType) :: Nil
        )

        val df = spark.createDataFrame(rddRow, schema)
//        df.printSchema()
//        df.show(1)
        df.createTempView("tmp")

        val frame: DataFrame = spark.sql(
            """
              |select
              |city, host, count(1) count
              |from tmp
              |group by city, host
              |""".stripMargin)
        frame.printSchema()
        frame.show(2)

        frame.repartition(1).write.mode(SaveMode.Overwrite).format("parquet")
                .save(outputPath)

        // 刷元数据 alter table add partition (d=xxx)
//        val outputPathStrings = outputPath.split("/")
//        val partitionByDay = outputPathStrings(outputPathStrings.length-1).substring(2,outputPathStrings(outputPathStrings.length-1).length)  ///hadoop-dwV2/dw/ads/access/d=20220426
//        spark.sql(s"alter table hadoop_dwv2.ods_areatrafstat add if not exists partition (d=$partitionByDay)")

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

    def ResultOfText(spark :SparkSession, input :String, output :String) = {
        import spark.implicits._
        val rdd: RDD[String] = spark.sparkContext.textFile(input)
        val df: DataFrame = rdd.map(record => {
            val records = record.split("\t")
            val city = records(11)
            val host = records(14)
//            logger.info(s"$city,--$host")
            (city, host)
        }).toDF()

        df.createTempView("hostinfo")

        val resultDf = spark.sql(
            """
              |select
              |host, city as area, count(1) count
              |from
              |hostinfo
              |group by
              |host, city
              |""".stripMargin)

        val dataFrame = resultDf.rdd.map(x => {
            val host = x.getString(0)
            val city = x.getString(1)
            val count = x.getLong(2).toInt
            (host + "\t" + city + "\t" + count + "")
        }).toDF()

        dataFrame.repartition(1).write.format("text")
                .option("compress", "gzip")
                .mode(SaveMode.Overwrite)
                .save(output)
    }

}
