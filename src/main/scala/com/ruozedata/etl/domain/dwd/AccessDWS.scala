package com.ruozedata.etl.domain.dwd

/**
 * @theme 封装DWD层日志
 * @author 阿左
 * @create 2022-04-14
 * */
object AccessDWS {
    //[11/06/2021:20:51:07 +0800]	182.90.24.6	-	111	-	POST	http://www.ruozedata.com	200	38	821	HIT
    // 原始字段11个
    var time: String = _ //时间
    var ip: String = _
    var proxyIp: String = _
    var responseTime: Long = _
    var referer: String = _
    var method: String = _
    var url: String = _
    var httpcode: String = _
    var requestSize: Long = _
    var responseSize: Long = _
    var cache: String = _

    //time解析字段
    var year: String = _
    var month: String = _
    var day: String = _

    //ip解析字段
    var province: String = _
    var city: String = _
    var isp: String = _

    //url解析字段
    var protocol: String = _
    var host: String = _
    var path: String = _
    var params: String = _

    def Access(params: String) {
        this.params = params
    }


    override def toString = s"$ip\t$proxyIp\t$responseTime\t" +
            s"$referer\t$method\t$url\t$httpcode\t$requestSize\t" +
            s"$responseSize\t$cache\t$province\t" +
            s"$city\t$isp\t$protocol\t$host\t$path\t$params\t$year\t$month\t$day"
}
