package cn.edu.nju.pasalab.graph.partitioning

import java.io.PrintWriter

import cn.edu.nju.pasalab.graph.util.HDFSUtils
import cn.edu.nju.pasalab.graph.vsim.VertexRecord
import org.apache.spark.sql.SparkSession

import scala.util.Random

object GenerateRandomPartitionPlanFile {

  def main(args: Array[String]): Unit = {
    val adjFile = args(0)
    val numPartitions = args(1).toInt
    val outputPlanFile = args(2)
    val spark = SparkSession.builder().appName("Random Partition Plan Generator").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    HDFSUtils.deleteFile(outputPlanFile)
    val adjDS = spark.read.parquet(adjFile).as[VertexRecord]
    val partIdDS = adjDS.map(record => s"${record.vid} ${Random.nextInt(numPartitions)}")
    partIdDS.write.text(outputPlanFile)
    spark.stop
 }

}
