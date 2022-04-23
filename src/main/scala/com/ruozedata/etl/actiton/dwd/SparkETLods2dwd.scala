package com.ruozedata.etl.actiton.dwd

import com.ruozedata.etl.domain.dwd.AccessDWS
import com.ruozedata.etl.utils.FileUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URL

/**
 * @theme 日志解析类，ods2dwd
 * @author 阿左
 * @create 2022-04-15
 * */
object SparkETLods2dwd {
    private val logger: Logger = LoggerFactory.getLogger("SparkETL")

    def main(args: Array[String]): Unit = {
        if (args.length != 3) {
            logger.error("请输入正确的参数，如：库/hadoop-dwV2/lib/ip.txt 输入/hadoop-dwV2/originalData/2022-04-17 输出/hadoop-dwV2/ods")
            System.exit(0)
        }

        System.setProperty("HADOOP_USER_NAME", "hadoop")
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkETL")
        val sc: SparkContext = new SparkContext(sparkConf)

        val ipResourcePath = args(0).trim
        val inputPath = args(1).trim
        val outputPath = args(2).trim

        ETLParse(sc, ipResourcePath, inputPath, outputPath)

        // 本地测试/hadoop-dwV2/lib /hadoop-dwV2/ods /hadoop-dwV2/dwd
        //        ETLParse(sc, "/hadoop-dwV2/lib/ip.txt",
        //            "/hadoop-dwV2/dw/ods/access/d=20220418/part*",
        //            "/hadoop-dwV2/dw/dwd/access/d=20220418")

    }

    /**
     * 将ods层的数据扩展成dwd层的大宽表
     *
     * @param sc             SparkContext
     * @param ipResourcePath ip地址库
     * @param inputPath      ods层数据
     * @param output         dwd层数据
     */
    def ETLParse(sc: SparkContext, ipResourcePath: String, inputPath: String, output: String): Unit = {

        //创建累加器
        val succeed = sc.longAccumulator("succeed")
        val failed = sc.longAccumulator("failed")
        val total = sc.longAccumulator("total")

        val lines_input: RDD[String] = sc.textFile(inputPath)
        val lines_ipResource: RDD[String] = sc.textFile(ipResourcePath)

        //将ip库广播出去
        val ip_map: collection.Map[String, (String, String, String)] = lines_ipResource.map(x => {
            val line = x.split("\\|")
            val ip = line(0)
            val province = line(6)
            val city = line(7)
            val isp = line(9)
            ip -> (province, city, isp)
        }).collectAsMap()

        val ipBc: Broadcast[collection.Map[String, (String, String, String)]] = sc.broadcast(ip_map)

        // 解析数据
        val access: RDD[AccessDWS.type] = lines_input.map(x => { //Executor 中执行
            val line = x.split("\t")
            try {
                val time = line(0)
                val ip = line(1)
                val proxyIp = line(2)
                AccessDWS.time = time
                AccessDWS.ip = ip
                AccessDWS.proxyIp = proxyIp

                // 转为数值类型，转换出错捕捉异常，程序继续执行
                var responseTime = 0L
                try {
                    responseTime = line(3).toLong
                } catch {
                    case e: NumberFormatException
                    => {
                        logger.error("responseTime 转换数值类型异常，数据可能保留")
                    }
                }
                AccessDWS.responseTime = responseTime

                val referer = line(4)
                val method = line(5)
                val url = line(6)
                val httpCode = line(7)
                AccessDWS.referer = referer
                AccessDWS.method = method
                AccessDWS.url = url
                AccessDWS.httpcode = httpCode

                var requestSize = 0L
                try {
                    requestSize = line(8).toLong
                } catch {
                    case e: NumberFormatException
                    => {
                        logger.error("requestSize 转换数值类型异常，数据可能保留")
                    }
                }
                AccessDWS.requestSize = requestSize

                //此字段产生10% 脏数据。脏数据为"-"
                // 数据转换异常，直接放弃此条记录
                var responseSize = 0L
                responseSize = line(9).toLong
                AccessDWS.responseSize = responseSize

                val cache = line(10)
                AccessDWS.cache = cache

                //time 字段解析
                val timeExtend = timeParse(time).split("\\t") //s"$year-$month-$day"
                val year = timeExtend(0)
                val month = timeExtend(1)
                val day = timeExtend(2)
                AccessDWS.year = year
                AccessDWS.month = month
                AccessDWS.day = day

                // ip 字段解析
                // 从广播变量中拿到
                val broadcastIp: collection.Map[String, (String, String, String)] = ipBc.value
                val ipExtend = ipParse(ip, broadcastIp).split("\\t") //s"$province-$city-$isp"
                val province = ipExtend(0)
                val city = ipExtend(1)
                val isp = ipExtend(2)
                AccessDWS.province = province
                AccessDWS.city = city
                AccessDWS.isp = isp

                // url 字段解析
                val urlExtend = urlParse(url).split("\\t") //s"$protocol-$host-$path-$params"
                val protocol = urlExtend(0)
                val host = urlExtend(1)
                val path = urlExtend(2)
                val params = urlExtend(3)
                AccessDWS.protocol = protocol
                AccessDWS.host = host
                AccessDWS.path = path
                AccessDWS.params = params

                succeed.add(1L)
            } catch {
                case e: NumberFormatException
                => {
                    logger.error("responseSize 转换数值类型异常,主动丢弃数据")
                    failed.add(1L)
                }
            }
            total.add(1L)
            logger.info(s"success:${succeed.value}, failed:${failed.value}, total:${total.value}" + AccessDWS.toString)
            AccessDWS
        })

        FileUtils.deleteFile(sc.hadoopConfiguration, output)
        access.saveAsTextFile(output)

        logger.info(s"success:${succeed.value}, failed:${failed.value}, total:${total.value}")

        //        Thread.sleep(Int.MaxValue)
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


    def ipParse(ip: String, map: collection.Map[String, (String, String, String)]): String = {
        //125.222.100.0
        var res = ""
        var province = "-"
        var city = "-"
        var isp = "-"

        val value: String = map.getOrElse(ip, "(-,-,-)").toString // 从map中取
        val splits = value.substring(1, value.length - 1).split(",") //去除括号

        try {
            province = splits(0)
        } catch {
            case e: ArrayIndexOutOfBoundsException
            => {
                logger.info("province 解析错误，赋默认值")
            }
        }

        try {
            city = splits(1)
        } catch {
            case e: ArrayIndexOutOfBoundsException
            => {
                logger.info("city 解析错误，赋默认值")
            }
        }

        try {
            isp = splits(2)
        } catch {
            case e: ArrayIndexOutOfBoundsException
            => {
                logger.info("isp 解析错误，赋默认值")
            }
        }

        res = s"$province\t$city\t$isp"
        res
    }

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
