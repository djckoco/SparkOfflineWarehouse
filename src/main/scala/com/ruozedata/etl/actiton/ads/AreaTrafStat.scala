package com.ruozedata.etl.actiton.ads

/**
 * @theme
 * @author 阿左
 * @create 2022-04-21
 * */
case class Ods_access(time: String, ip: String, proxyIp: String, responseTime: Long, referer: String,
                        method: String, url: String, httpcode: String, requestSize: Long, responseSize: Long,
                        cache: String, year: String, month: String, day: String, province: String,
                        city: String, isp: String, protocol: String, host: String, path: String, params: String
                       )
case class HostInfo(host: String, city: String)
case class AreaTrafStat(host: String, city: String, count :Int)