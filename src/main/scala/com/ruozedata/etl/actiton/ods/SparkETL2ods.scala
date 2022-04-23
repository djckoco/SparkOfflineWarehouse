package com.ruozedata.etl.actiton.ods

import com.ruozedata.etl.domain.ods.AccessODS
import com.ruozedata.etl.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @theme 原始数据经初级清洗(数据验证)进入数仓ods层，2ods
 * @author 阿左
 * @create 2022-04-15
 * */
object SparkETL2ods {
    private val logger: Logger = LoggerFactory.getLogger("SparkETL2ods")

    def main(args: Array[String]): Unit = {
        if (args.length != 3) {
            logger.error("请输入正确的参数，如：库/hadoop-dwV2/lib/ip.txt 输入/hadoop-dwV2/originalData/2022-04-17 输出/hadoop-dwV2/ods")
            System.exit(0)
        }

        System.setProperty("HADOOP_USER_NAME", "hadoop")
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkETL2ods")
        val sc = new SparkContext(sparkConf)
        val inputPath = args(0).trim
        val outputPath = args(1).trim

        ETLParse(sc , inputPath, outputPath)
        //        ETLParse(sc, "/hadoop-dwV2/dw/raw/access/20220418", "/hadoop-dwV2/dw/ods/access/d=20220418")
        //[2022/04/17/:23:08:09 +0800]	222.83.234.112	-	1677	179	GET	http://www.ruozedata.com/video/av34329219	200	152	122	HIT
    }

    def ETLParse(sc: SparkContext, inputPath: String, output: String): Unit = {


        //创建累加器
        val succeed = sc.longAccumulator("succeed")
        val failed = sc.longAccumulator("failed")
        val total = sc.longAccumulator("total")

        val lines_input: RDD[String] = sc.textFile(inputPath)

        // 解析数据
        val access: RDD[AccessODS.type] = lines_input.map(x => { //Executor 中执行
            val line = x.split("\t")
            try {
                AccessODS.time = line(0)
                AccessODS.ip = line(1)
                AccessODS.proxyIp = line(2)
                AccessODS.responseTime = line(3)
                AccessODS.referer = line(4)
                AccessODS.method = line(5)
                AccessODS.url = line(6)
                AccessODS.httpcode = line(7)
                AccessODS.requestSize = line(8)

                //此字段产生10% 脏数据。脏数据为"-"
                //如果数据转换异常，直接放弃此条记录
                var responseSize = 0L
                responseSize = line(9).toLong
                AccessODS.responseSize = responseSize

                val cache = line(10)
                AccessODS.cache = cache

                succeed.add(1L)
            } catch {
                case e: NumberFormatException
                => {
                    logger.error("responseSize 转换数值类型异常,主动丢弃数据")
                    failed.add(1L)
                }
            }
            total.add(1L)
            logger.info(s"success:${succeed.value}, failed:${failed.value}, total:${total.value}" + AccessODS.toString)
            AccessODS
        })

        FileUtils.deleteFile(sc.hadoopConfiguration, output)
        access.saveAsTextFile(output)

        logger.info(s"success:${succeed.value}, failed:${failed.value}, total:${total.value}")

        //        Thread.sleep(Int.MaxValue)
    }

    def MakeCoalesce(fs :FileSystem, inputPath :String, blockSize :Int): Int ={
        var fileTotalSize = 0L
        val fileStatuses = fs.globStatus(new Path(inputPath))
        fileStatuses.foreach(x=>{
            fileTotalSize = x.getLen + fileTotalSize
        })
        val partitionsNum = (fileTotalSize / 1024 / 1024 / blockSize).toInt
        if(partitionsNum == 0){
            logger.info(s"输入分区数为：1")
            1
        }else{
            logger.info(s"输入分区数为：$partitionsNum")
            partitionsNum
        }
    }

}
