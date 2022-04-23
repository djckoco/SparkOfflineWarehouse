package com.ruozedata.etl.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * @theme 删除hdfs目标文件
 * @author 阿左
 * @create 2022-04-17
 * */
object FileUtils {

    def deleteFile(cof :Configuration, output :String): Unit ={
        val fs = FileSystem.get(cof)

        //输出文件存在就删除
        val path = new Path(output)
        if(fs.exists(path)){
            fs.delete(path, true)
        }
    }

}
