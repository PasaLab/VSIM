package cn.edu.nju.pasalab.graph.partitioning

import cn.edu.nju.pasalab.graph.vsim.{Task, VertexRecord}
import org.apache.spark.sql.SparkSession

/**
  * 将KaHIP工具生成的Partition plan文件（需要预先上传到HDFS上）转换为VSIM能使用的partition plan file，并保存到HDFS上
  */
object ConvertKaHIPPartitionPlanToVSIMPartitionPlan {
  case class PartitionInfo(newVid:Int, partID:Int) {}
  case class VidMap(oldVid:Int, newVid:Int) {}
  case class VidInfo(vid:Int, compVid:Long) {}

  def main(args: Array[String]): Unit = {
    val inputPlanFile = args(0)
    val vidMappingFile = args(1)
    val outputPath = args(2)
    val spark = SparkSession.builder().appName("Partition Plan Converter").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val inputPlanDS = sc.textFile(inputPlanFile).zipWithIndex()
      .map(pair => {
        val (partID, index) = pair
        val newVid = index + 1
        PartitionInfo(newVid.toInt, partID.toInt)
      }).toDS
    val vidMappingDS = spark.read.textFile(vidMappingFile).map(line => {
      val f = line.split(" ")
      VidMap(f(0).toInt, f(1).toInt)
    })
    val partitionDS = inputPlanDS.join(vidMappingDS, usingColumn = "newVid").map(row => {
      val oldVid = row.getAs[Int]("oldVid")
      val partID = row.getAs[Int]("partID")
      s"${oldVid} ${partID}"
    })
    partitionDS.write.text(outputPath)
    spark.stop()
  }

}
