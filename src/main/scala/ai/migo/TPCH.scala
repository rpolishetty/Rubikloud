package ai.migo

import ai.migo.model.{Part, PartSupp, SampleTable}
import ai.migo.tpch.{SparkJoinApi, SparkUtilities}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
 * created by prohith on 8/25/18
 */

object TPCH {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName)
    val hdfsConfig = new HdfsConfiguration()
    conf.setMaster("local[5]")

    val spark = SparkSession.builder().config(conf)
      .enableHiveSupport()
      .getOrCreate


    val part = Part(partkey = "partKey",
      name = "Pankaj",
      mfgr = "mfgr",
      brand = "brand",
      Type = "type",
      size = "size",
      container = "container",
      retailPrice = "retailPrice",
      comment = "comment")

    val partSupp = PartSupp(
      partKey = "partKey",
      suppKey = "suppKey",
      availQty = "availQty",
      supplyCost = "supplyCost",
      comment = "comment"
    )

    val sparkJoinApi = new SparkJoinApi(spark);


    // Answer to Qus 1:
    sparkJoinApi.joinUsingDataFrame(part, partSupp);
    sparkJoinApi.joinUsingRDD(part, partSupp);
    sparkJoinApi.joinUsingDataset(part, partSupp)

    // Answer To Qus 2:
    sparkJoinApi.joinUsingSparkSQL(part, partSupp)

    // Answer to Qus 3:
    /*
    * Alternate Data Formats: We can use parquet files with snappy compression
    * instead of plain text files.
    * */

    val sparkUtilities = new SparkUtilities(spark);
    sparkUtilities.readWriteParquet(part);


    // Answer to Qus 4:
    /*
    * Every Spark executor in an application has the same fixed number of cores and same fixed heap size.
    * The number of cores can be specified with the --executor-cores flag when invoking spark-submit, spark-shell, and pyspark from the command line,
    * or by setting the spark.executor.cores property in the spark-defaults.conf file or on a SparkConf object.
    * Similarly, the heap size can be controlled with the --executor-memory flag or the spark.executor.memory property.
    * The cores property controls the number of concurrent tasks an executor can run.
    * --executor-cores 5 means that each executor can run a maximum of five tasks at the same time.
    * The memory property impacts the amount of data Spark can cache, as well as the maximum sizes of the shuffle data structures used for grouping, aggregations, and joins.
    * The --num-executors command-line flag or spark.executor.instances configuration property control the number of executors requested.
    * Starting in CDH 5.4/Spark 1.3, you will be able to avoid setting this property by turning on dynamic allocation with the spark.dynamicAllocation.enabled property.
    * Dynamic allocation enables a Spark application to request executors when there is a backlog of pending tasks and free up executors when idle
    */

    // Answer to Qus 5:

    val sampleTable = SampleTable(year = "2018",
      month = "August",
      data = "Pankaj")
    sparkUtilities.createTable(sampleTable)


    // Answer to Qus 6:

    val sampleTable1 = SampleTable(year = "2018",
      month = "Jan",
      data = "Pankaj")

    val sampleTable2 = SampleTable(year = "2018",
      month = "Feb",
      data = "Pankaj")

    val sampleDF = spark.createDataFrame(List(sampleTable1, sampleTable2))

    sparkUtilities.updatePartition(sampleDF)


    // Answer to Question 7:
    /*
    * To Automate ETL Process we can do following:
    * 1. Add the last step after loading or saving data.
    * 2. Read that data in another spark job
    * 3. Add regex corresponding to every column/field in data
    * 4. use transformation(a.map(...)) to filter out all the data which is not correct according to regex pattern
    * 5. use accumulators to keep the count of valid and invalid data
    * 6. create a dashboard using those accumulators
    * */

    // Answer to Qus 8:
    /*
    * For performance tuning
    * 1. Since spark transformation are lazy, we can identify and group common transformations so that they won't be computed again and again.
    * 2. if some dataset are required again in future, then we can persist them to avoid recomputation.
    * 3. To ensure that after every transformations and action, data is distruibuted evenly accross all machines, we can use dataset.repartition
    * 4. we can control parallelism using sc.parallelize
    * 5. Make sure each object extends serializable
    * */
    /*
    * Data locality can have a major impact on the performance of Spark jobs.
    * If data and the code that operates on it are together then computation tends to be fast.
    * But if code and data are separated, one must move to the other.
    * Typically it is faster to ship serialized code from place to place than a chunk of data because code size is much smaller than data.
    * Spark builds its scheduling around this general principle of data locality.
    * */
  }

}
