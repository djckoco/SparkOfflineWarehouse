package com.ruozedata.etl.utils

import java.io.{IOException, OutputStream}
import java.net.{HttpURLConnection, MalformedURLException, URL}

/**
 * @theme http POST发送数据
 * @author 阿左
 * @create 2022-04-15
 * */
object UploadUtils {

    def upload(path: String, log: String): Unit ={
        try{
            val url = new URL(path)
            // 类型强转
            val connection: HttpURLConnection = url.openConnection().asInstanceOf[HttpURLConnection]

            //GET POST
            connection.setRequestMethod("POST")
            connection.setDoOutput(true)

            // json数据==>application/json
            connection.setRequestProperty("Content-Type", "application/text")

            val outputStream: OutputStream = connection.getOutputStream()
            outputStream.write(log.getBytes())
            outputStream.flush()
            outputStream.close()

            val responseCode = connection.getResponseCode
            println(responseCode)

        }catch {
            case e: MalformedURLException
                => e.printStackTrace()
            case e: IOException
                => e.printStackTrace()
        }
    }
}
