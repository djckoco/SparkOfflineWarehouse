package com.ruozedata.etl.actiton

import com.ruozedata.etl.utils.UploadUtils
import org.slf4j.LoggerFactory

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import scala.io.{BufferedSource, Source}
import scala.util.Random

/**
 * @theme mock日志数据，数据包含11个字段
 *        包含20% 的脏数据，
 * @author 阿左
 * @create 2022-04-14
 * */
object MockData {
    //[11/06/2021:20:51:07 +0800]	182.90.24.6	-	111	-	POST	http://www.ruozedata.com	200	38	821	HIT

    private val logger = LoggerFactory.getLogger("MockData")
    private val urlHosts = Array("https://www.bilibili.com", "http://www.ruozedata.com", "https://ruoze.ke.qq.com")
    private val methods = Array("POST", "GET", "HEAD")
    private val httpCodes = Array("200", "301", "404", "500")
    private val caches = Array("HIT", "MISS")
    private val random = Random
    private val URL = "http://bigdata:16666/logserver/upload"

    def main(args: Array[String]): Unit = {
//                if(args.length != 4){
//                    logger.error("请输入正确的参数！ eg：1000, 2022-04-15, data/ip.txt, output/2022-04-15")
//                    System.exit(0);
//                }
//
//                val num = args(0).trim().toInt
//                val date = args(1).trim()
//                val ipResourcePath = args(2).trim()
//                val outputPath = args(3).trim()

        mockAll(1000000, "2022-04-22", "data/ip.txt", "output/20220422")
//        mockAll(num, date, ipResourcePath, outputPath)
//        println(mockUrl())
    }

    //产生时间字段
    /**
     * @param date 传入的时间格式：2022-04-14
     * @return 输出时间格式：年月日以及时区固定，其他数据随机 [11/06/2021:20:51:07 +0800]
     */
    def mockTime(date: String): String = {
        var res: String = ""
        var randomInt: Int = 0

        val split_date = date.split("-")
        val year = split_date(0)
        val month = split_date(1)
        val day = split_date(2)

        //统一时间格式，用两位数表示
        randomInt = random.nextInt(24)
        val hour = if (randomInt < 10) "0" + randomInt + "" else randomInt + ""

        randomInt = random.nextInt(24)
        val min = if (randomInt < 10) "0" + randomInt + "" else randomInt + ""

        randomInt = random.nextInt(24)
        val sec = if (randomInt < 10) "0" + randomInt + "" else randomInt + ""

        res = s"[$year/$month/$day $hour:$min:$sec +0800]"
        res
    }

    //产生ip字段
    def mockip(ipResourcePath: String): String = {

        val bufferedSource: BufferedSource = Source.fromFile(ipResourcePath)
        val lines = bufferedSource.getLines().toList
        bufferedSource.close() //数据读完及时关闭

        val lineSplits = lines(random.nextInt(lines.length)).split("\\|") // 对|分隔符转义
        val ip = lineSplits(0)
        val province = lineSplits(6)
        val city = lineSplits(7)
        val isp = lineSplits(9)

        //经纬度
        //        val lon = lineSplits(13)
        //        val lat = lineSplits(14)
        // ip 省份 城市 isp
        //        ip+","+province+","+city+","+isp

        ip
    }

    //产生url字段
    def mockUrl(): String = {
        val host = urlHosts(random.nextInt(3)) //必选
        val path = if (random.nextInt(10) < 5) "/video" + "/av" + random.nextInt(99999999) else "" //可选
        val params = if (random.nextInt(10) == 5) "?a=b&c=d" else "" //path 基础上可选

        if (path == "") //没有path路径
            host
        else
            host + path + params
    }

    def mockAll(num: Int, date: String, ipResourcePath: String, outputPath: String) {
        var time: String = ""
        var ip: String = ""
        var proxyIp: String = ""
        var responseTime: String = ""
        var referer: String = ""
        var method: String = ""
        var url: String = ""
        var httpCode: String = ""
        var requestSize: String = ""
        var responseSize: String = ""
        var cache: String = ""

        try {
            var i = 0
            val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outputPath))))
            while (i < num) {
                //scala 没有写类，采用java写操作
                time = mockTime(date)

                ip = mockip(ipResourcePath)

                // 10% 的数据有代理ip
                proxyIp = if (random.nextInt(10) == 5) mockip(ipResourcePath) else "-"

                responseTime = random.nextInt(2500) +""

                referer = random.nextInt(500) + ""

                method = methods(random.nextInt(3))

                url = mockUrl()

                httpCode = httpCodes(random.nextInt(4))

                requestSize = random.nextInt(500) +""

                //此字段产生10% 脏数据。脏数据为"-"
                responseSize = if(random.nextInt(10) == 5) "-" else random.nextInt(1000) + ""

                    cache = caches(random.nextInt(2))

                val res = s"$time\t$ip\t$proxyIp\t$responseTime\t$referer\t$method\t$url\t$httpCode\t$requestSize\t$responseSize\t$cache"

                logger.info(res)
                //logger.info(i+"")
                //写入本地
                writer.write(res+"\n")

                //http post 发送 数据
                //UploadUtils.upload(URL, res)
                i = i + 1
            }
            writer.flush()
            writer.close()
        } catch {
            case e: Exception
            => {
                e.printStackTrace()
            }
        }
    }
}
