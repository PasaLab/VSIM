package cn.edu.nju.pasalab.graph.vsim

case class VertexRecord(vid: Int, adj: Array[Long]) {}
case class ResultPair(v1: Long, v2: Long, score: Double) {}
case class Task(compVid:Long, partitionID:Int) {}
case class JoinTaskPart(vid:Int, pID:Int){}
case class PartitionInfo(vid:Int, partID:Int) {}

case class TaskStatus() {

  var prefixTotalTime:Double = 0
  var prefixDBTime:Double = 0
  var prefixDBQueryCount:Int = 0
  var execMode:Int = 0
  var suffixTotalTime:Double = 0
  var suffixDBTime:Double = 0
  var suffixDBQueryCount:Int = 0
  var prefixLen:Int = 0
  var suffixLen:Int = 0
  var candidateCount:Int = 0
  var resultCount:Int = 0
  var estimProbe:Double = 0
  var estimVerify:Double = 0

  override def toString: String = s"$prefixTotalTime,$prefixDBTime,$prefixDBQueryCount,$execMode,$suffixTotalTime," +
    s"$suffixDBTime,$suffixDBQueryCount,$prefixLen,$suffixLen,$candidateCount,$resultCount,$estimProbe,$estimVerify"
}