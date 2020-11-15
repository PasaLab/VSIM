package cn.edu.nju.pasalab.graph.vsim

import java.util.Date

import cn.edu.nju.pasalab.graph.storage.{Serializer, SimpleGraphStore}
import cn.edu.nju.pasalab.graph.util.MurMurHash3
import com.google.common.base.Strings
import com.google.common.primitives.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession
import org.spark_project.jetty.util.security.Credential.MD5

object Preprocess {

  class KeyPartitioner(val numPartitions:Int) extends Partitioner {
    override def getPartition(key: Any): Int = {
        key.asInstanceOf[Int] % numPartitions
    }
  }

  def main(args: Array[String]): Unit = {
    println("VSIM-Preprocess starts!")
    val options = PreprocessOptions(args)
    println(s"Get options: ${options.toString}")
    val spark = SparkSession.builder().appName(s"VSIM-Preprocess: ${options.input}")
      .config("spark.sql.shuffle.partitions", options.parallelism)
      .config("spark.default.parallelism", options.parallelism)
      .getOrCreate()
    val sc = spark.sparkContext
    println(s"${new Date()}, clear the database...")
    import spark.implicits._
    val graphStore = SimpleGraphStore.getSingleton
    graphStore.clearDB()
    val t0 = System.nanoTime()
    println(s"${new Date()}, generate the input task file according to the partition plan...")
    val hdfs = FileSystem.get(new Configuration())
    hdfs.delete(new Path(options.taskFilePath), true)
    val adjDS = spark.read.parquet(options.input).as[VertexRecord].repartition(options.numPartitions).cache()
    val planDS = spark.read.textFile(options.partitionPlanFile).map(str => {
      val f = str.split(" ")
      PartitionInfo(vid=f(0).toInt, partID = f(1).toInt)
    })
    val joinedDF = adjDS.join(planDS, usingColumn = "vid").select($"partID", $"vid", $"adj")
    val joinedRDD = joinedDF.rdd.map(row => (row.getInt(0), (row.getInt(1), row.getSeq[Long](2).toArray)))
    val partitionedRDD = joinedRDD.partitionBy(new KeyPartitioner(options.numPartitions))
    val partitionedAdjRDD = partitionedRDD.map(pair => VertexRecord(vid=pair._2._1, adj=pair._2._2))
    partitionedAdjRDD.toDS().write.parquet(options.taskFilePath)
//    val partitionedDF = joinedDF.repartition(numPartitions = options.numPartitions, partitionExprs = $"partID")
//    val partitionedAdjDS = partitionedDF.map(row => VertexRecord(vid = row.getInt(0), adj = row.getSeq[Long](2).toArray))
//    partitionedAdjDS.write.parquet(options.taskFilePath)
    val t1 = System.nanoTime()
    println(s"Written the task file done (s): ${(t1 - t0)/1e9}")
    println(s"${new Date()}, store the graph file into the database...")
    adjDS.foreachPartition(partIterator => {
      val graphStore = SimpleGraphStore.getSingleton
      partIterator.grouped(options.batchSize).foreach(records => {
        val vids = records.map(r => (r.adj.length.toLong << 32L) | r.vid.toLong).toArray
        val adjs = records.map(r => r.adj).toArray
        graphStore.put(vids, adjs)
      })
    })
    val t2 = System.nanoTime()
    println(s"Written to database done (s): ${(t2 -t1)/1e9}")
    println("Wait for the DB to become globally consistency")
    // Wait for another 10 seconds to make Cassandra-like DB to become globally consistent
    Thread.sleep(1000 * 20)
    println(s"${new Date()}, now start to warm up the database...")
    adjDS.foreachPartition(partIterator => {
      val graphStore = SimpleGraphStore.getSingleton
      partIterator.grouped(options.batchSize).foreach(records => {
        val vids = records.map(r => (r.adj.length.toLong << 32L) | r.vid.toLong).toArray
        val adjs = graphStore.get(vids)
        for (i <- 0 until vids.length)
          assert(adjs(i) != null, s"Vertex key 0x${vids(i).toHexString} got null value.")
        graphStore.get(vids)
      })
    })
    println("Done!")
    val t3 = System.nanoTime()
    println(s"Warmed up the database (s): ${(t3 - t2)/1e9}")
    println(s"Total elapsed time (s): ${(t3 - t0)/1e9}")
    spark.stop()
    System.exit(0)
  }
}

case class PreprocessOptions(args:Array[String]) {

  val input = args(0)
  val numPartitions = args(1).toInt
  val batchSize = args(2).toInt
  val taskFilePath = args(3)
  val parallelism = args(4).toInt
  val partitionPlanFile = args(5)

  override def toString: String = {
    s"""
       |=== Preprocess Options ===
       | input: $input
       | store to db batch size: $batchSize
       | num partitions: $numPartitions
       | task file path: $taskFilePath
       | parallelism: $parallelism
       | partition plan: $partitionPlanFile
     """.stripMargin
  }
}

