package com.ruozedata.etl.actiton.ads

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @theme 将hive数据导出到mysql
 * @author 阿左
 * @create 2022-04-24
 *
 * Q：是否支持重跑?
 * Q：overwrite 主键如何生成？ 将当天的数据删除，接着重跑。
 * */
object MySQL2Hive {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "hadoop")
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkETLdwd2ads")
        //改造yarn
        //        val sparkConf = new SparkConf().setAppName("SparkETLdwd2ads")

        val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        val url = spark.sqlContext.getConf("spark.app.url")
        val dbtable = spark.sqlContext.getConf("spark.app.dbtable","jdbc:mysql://bigdata:3306")
        val user = spark.sqlContext.getConf("spark.app.user","root")
        val password = spark.sqlContext.getConf("spark.app.password","rootroot")
        val printSchema = spark.sqlContext.getConf("spark.app.printSchema","false").toBoolean

        val df = spark.sql(
            """
              |
              |select
              |*
              |from
              |default.ads_areatrafstat
              |
              |""".stripMargin)
        df.cache()

        if (printSchema){
            df.printSchema()
        }

        df.write.format("jdbc")
                .option("url", "jdbc:mysql://bigdata:3306")
                .option("dbtable", "bigdata.test")
                .option("user", "root")
                .option("password", "rootroot")
                .save()
//        重点参数
//        batchsize
//        truncate
//        createTableOptions
//        createTableColumnTypes
//        customSchema

    }

}
