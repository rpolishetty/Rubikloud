package ai.migo.tpch

import ai.migo.model.{Part, PartSupp}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/*
 * created by prohith on 8/25/18
 */

class SparkJoinApi(spark: SparkSession) extends Serializable {
  import spark.implicits._

  def joinUsingDataFrame(part: Part, partSupp: PartSupp): Unit ={
    val partDf = spark.createDataFrame(List(part));
    val partSuppDf = spark.createDataFrame(List(partSupp));

    val joinedDf = partDf.joinWith(partSuppDf,
      partDf("partKey") <=> partSuppDf("partKey"))

    joinedDf.show()
  }

  def joinUsingRDD(part: Part, partSupp: PartSupp): Unit ={
    val partRdd = spark.createDataFrame(List(part)).rdd;
    val partSuppRdd = spark.createDataFrame(List(partSupp)).rdd;

    val joinedRDD = partRdd.union(partSuppRdd);
    val a: Array[Row] = joinedRDD.collect()
    println(a(0))
    println(a(1))
  }

  def joinUsingDataset(part: Part, partSupp: PartSupp): Unit ={
    val partDS = spark.createDataset(List(part));
    val partSuppDS = spark.createDataset(List(partSupp));

    val joinedDS = partDS.joinWith(partSuppDS,
      partDS("partKey") <=> partSuppDS("partKey"))

    joinedDS.show()
  }

  def joinUsingSparkSQL(part: Part, partSupp: PartSupp): Unit ={
    val partDf = spark.createDataFrame(List(part));
    val partSuppDf = spark.createDataFrame(List(partSupp));

    partDf.registerTempTable("partDf")
    partSuppDf.registerTempTable("partSuppDf")

    val joinedDF: DataFrame = spark.sql("select partDf.* from partDf join partSuppDf on partDf.partKey = partSuppDf.partKey")
    joinedDF.show()
  }

}
