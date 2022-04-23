package com.ruozedata.etl.domain.ods

/**
 * @theme 封装ODS层日志，不作数据解析
 * @author 阿左
 * @create 2022-04-14
 * */
object AccessODS {
    //[11/06/2021:20:51:07 +0800]	182.90.24.6	-	111	-	POST	http://www.ruozedata.com	200	38	821	HIT
    // 原始字段11个
    var time: String = _ //时间
    var ip: String = _
    var proxyIp: String = _
    var responseTime: String = _
    var referer: String = _
    var method: String = _
    var url: String = _
    var httpcode: String = _
    var requestSize: String = _
    var responseSize: Long = _
    var cache: String = _

    override def toString = s"$time\t$ip\t$proxyIp\t$responseTime\t" +
            s"$referer\t$method\t$url\t$httpcode\t$requestSize\t" +
            s"$responseSize\t$cache\t"
}
