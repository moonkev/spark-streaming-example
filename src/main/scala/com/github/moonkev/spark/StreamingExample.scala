package com.github.moonkev.spark

import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object StreamingExample {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val schema = StructType(StructField("message", StringType, nullable = true) :: Nil)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .getOrCreate()

    val sampleStream = spark.readStream
      .format("sample")
      .option("producerRate", "5")
      .option("queueSize", "100")
      .option("batchSize", "25")
      .schema(schema)
      .load()

    sampleStream.printSchema()

    val query: StreamingQuery = sampleStream
      .selectExpr("ID * 1000 as id", "Msg")
      .writeStream
      .outputMode("append")
      .queryName("sample")
      .format("memory")
      .start()

    for(_ <- 0 to 10) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(2))
      spark.sql("select * from sample order by id desc").show()
    }

    query.stop()
  }
}
