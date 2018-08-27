package ai.migo.tpch

import ai.migo.model.{Part, PartSupp, SampleTable}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/*
 * created by prohith on 8/25/18
 */

class SparkUtilities(spark: SparkSession) extends Serializable {
  import spark.implicits._

  def readWriteParquet(part: Part): Unit ={
    val partDF = spark.createDataFrame(List(part));
    partDF.write
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy")
        .parquet("part.parquet")

    val newDataDF = spark.
      read.parquet("part.parquet")

    newDataDF.show()
  }

  def createTable(sampleTable: SampleTable): Unit = {
    val tableDF = spark.createDataFrame(List(sampleTable))

    tableDF.write
      .partitionBy("month")
      .saveAsTable("sample")
  }

  def updatePartition(sampleDF: DataFrame): Unit ={
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    sampleDF.write.mode("overwrite").insertInto("sample")
  }
}
