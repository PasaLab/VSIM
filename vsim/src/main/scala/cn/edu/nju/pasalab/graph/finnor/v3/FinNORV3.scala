package cn.edu.nju.pasalab.graph.finnor.v3

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

import cn.edu.nju.pasalab.db.Utils
import cn.edu.nju.pasalab.graph.storage.SimpleGraphStore
import cn.edu.nju.pasalab.graph.util.{HDFSUtils, HostInfoUtil, ProcessLevelSingletonManager}
import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, IntOpenHashSet}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

case class FinNOROptions(args:Array[String]) {

  val input = args(0)
  val defaultParallelism = args(1).toInt
  val mode = args(2)
  val batchSize = args(3).toInt
  val outputStatsInfo = args(4).toBoolean
  val outputPath = args(5)
  val threadNumPerExecutor = args(6).toInt
  val numSplitPerVertex = args(7).toInt
  val numGraphPartitions = args(8).toInt

  override def toString: String = {
    s"""
       |=== Options ===
       |1.input-related:
       | input: $input
       | number of graph partitions: $numGraphPartitions
       | default parallelism: $defaultParallelism
       | store to db batch size: $batchSize
       |2.similarity calculation:
       | number of threads per executor: $threadNumPerExecutor
       | number of splits per vertex: $numSplitPerVertex
       |3. output related:
       | output stats information: $outputStatsInfo
       | output file path: $outputPath
       |
     """.stripMargin
  }

}

object WorkerThreadPool {

  def getExecutionContext(numThread:Int):ExecutionContext = {
    def createExecutionContext(ind:Int):ExecutionContext = {
      val forkJoinPool = new ForkJoinPool(numThread)
      val executionContext = ExecutionContext.fromExecutorService(forkJoinPool)
      executionContext
    }
    ProcessLevelSingletonManager.fetch(5, createExecutionContext).asInstanceOf[ExecutionContext]
  }


}

/**
  * This is version 3.1 of the FinNOR v3 series.
  */
object FinNORV3 {

  def logger = Logger.getLogger("FinNOR")
  type AdjRDD = RDD[(Int, Array[Int])]

  def parseAdjFile(textRDD:RDD[String]): AdjRDD = {
    val adjRDD = textRDD.map(line => {
      val fields = line.split(" ")
      val vid = fields(0).toInt
      val adj = fields.slice(1, fields.length).map(_.toInt).sorted
      (vid, adj)
    })
    adjRDD
  }

  def clearDB() = {
    logger.info("Clearing database, started.")
    SimpleGraphStore.getSingleton.clearDB()
    logger.info("Clearing database, done.")
  }

  def storeAdjToDB(adjRDD:AdjRDD, batchSize:Int) = {
    logger.info("Store to database, started.")
    adjRDD.foreachPartition(partitionIter => {
      val partitionContents = partitionIter.toArray
      partitionContents.grouped(batchSize).foreach(batch => {
        val vids = batch.map(_._1)
        val adjs = batch.map(_._2.sorted)
        val dbClient = SimpleGraphStore.getSingleton.put(vids, adjs)
      })
    })
    logger.info("Store to database, done.")
  }

  def calcDegreeThreshold(adjRDD:AdjRDD, proportion:Double):Int = {
    val degrees = adjRDD.map(_._2.size)
    val numVertices = adjRDD.count()
    val topDegreeVertexNum  = (numVertices * proportion).toInt
    val topDegrees = degrees.top(topDegreeVertexNum)
    if (topDegrees.length == 0) {
      logger.info("The length of the top degree array is 0, degree threshold is invalid!")
    }
    val degreeThreshold = if (topDegrees.length > 0) topDegrees.min else -1
    degreeThreshold
  }

  def preprocessing(options:FinNOROptions):Long = {
    logger.info("Enter preprocessing mode.")
    clearDB()
    logger.info("Writing to database, started.")
    val t0 = System.nanoTime()
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"FinNOR-preprocessing-${options.input}")
    val sc = SparkContext.getOrCreate(sparkConf)
    val textRDD = sc.textFile(options.input, options.defaultParallelism).setName("Text RDD")
    val adjRDD = parseAdjFile(textRDD).setName("Adj RDD")
    storeAdjToDB(adjRDD, options.batchSize)
    val t1 = System.nanoTime()
    logger.info("Writing to database, done.")
    logger.info(s"Execution time: ${(t1 - t0)/1e9} s.")
    (t1 - t0)
  }

  def getPivots(partedAdjRDD: RDD[(Array[(Int, Array[Int])], Array[(Int, Array[Int])])], numSplitPerVertex: Int):Array[Int] = {
    val degreePairRDD = partedAdjRDD
      .flatMap(pair => pair._1.map(p => (p._1, p._2.length)) ++ pair._2.map(p => (p._1, p._2.length)))
    val sampleDegreePair = degreePairRDD.takeSample(false, 1000).sortBy(_._1)
    val degreeCostPerSplit = sampleDegreePair.map(_._2).sum / numSplitPerVertex.toDouble
    val pivots = new ArrayBuffer[Int]()
    var i = 0
    var currentCost = 0
    while (i < sampleDegreePair.length) {
      currentCost += sampleDegreePair(i)._2
      if (currentCost > degreeCostPerSplit) {
        pivots.append(sampleDegreePair(i)._1)
        currentCost = 0
      }
      i += 1
    }
    pivots.append(Int.MaxValue)
    pivots.toArray
  }

  def calculation(options:FinNOROptions):Long = {
    logger.info("Enter calculation mode.")
    val t0 = System.nanoTime()
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"FinNOR-calculation-${options.input}")
    val sc = SparkContext.getOrCreate(sparkConf)
    val numResultPairCounter = sc.longAccumulator("# of result pairs")
    val numResultSumCounter = sc.longAccumulator("# of result sum")
    val dbTimeCounter = sc.longAccumulator("Database time (ns)")
    val calcTimeCounter = sc.longAccumulator("Calculation time (ns)")
    val actualCalcCostCounter = sc.longAccumulator("Actual computing cost")
    val textRDD = sc.textFile(options.input, options.defaultParallelism).setName("Text RDD")
    val adjRDD = parseAdjFile(textRDD).setName("Adj RDD").repartition(options.numGraphPartitions)
    adjRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    adjRDD.count()
    val t1 = System.nanoTime()
    logger.info(s"Load dataset done. [${(t1 - t0)/1e9}]s")

    val statsLines = adjRDD.mapPartitionsWithIndex ((partID, partitionIterator) => {
      if (partitionIterator.hasNext) {
        val statsLines = calculateLightPartitions(partID, partitionIterator, options,
          numResultPairCounter, numResultSumCounter,
          dbTimeCounter, calcTimeCounter, actualCalcCostCounter)
        statsLines.iterator
      } else {
        Iterator()
      }
    }, true)

    if (options.outputStatsInfo) {
      HDFSUtils.deleteFile(options.outputPath)
      logger.info(s"output file ${options.outputPath} deleted.")
      statsLines.saveAsTextFile(options.outputPath)
    } else {
      statsLines.count()
    }
    val t2 = System.nanoTime()
    logger.info(s"Calculation done for adj pairs. [${(t2 - t1)/1e9}]")
    logger.info(s"Number of result pairs: ${numResultPairCounter.value}, Number of result sum: ${numResultSumCounter.value}")
    logger.info(s"DB Time Counter (ns): ${dbTimeCounter.value}, Calc Time Counter (ns): ${calcTimeCounter.value}")
    logger.info(s"Actual computing cost: ${actualCalcCostCounter}")
    //val correctS = correctAnswer(adjRDD)
    //logger.info(s"Correct number of result sum: ${correctS}")
    t2 - t0
  }

  def correctAnswer(adjRDD: AdjRDD):Long = {
    val numEdges = adjRDD.map(pair => pair._2.size).reduce(_ + _)
    val totalSum = adjRDD.map(pair => pair._2.size * pair._2.size.toLong).reduce(_ + _)
    logger.info(s"Number of edges: ${numEdges}, total sum: ${totalSum}")
    (totalSum - numEdges)/2
  }

  def main(args: Array[String]): Unit = {
    logger.info("FinNOR starts!")
    println("FinNOR!")
    val options = new FinNOROptions(args)
    logger.info(s"Input options: " + options)
    if (options.mode == "preprocessing") {
      val executionTime = preprocessing(options)
      println(s"---> Total execution time: ${executionTime/1e9} s.")
    } else {
      val executionTime = calculation(options)
      println(s"---> Total execution time: ${executionTime/1e9} s.")
    }
  }

  def processLightVertex(partID:Int, vid:Int, adj:Array[Int], options:FinNOROptions, executionContext: ExecutionContext,
                         dbTimeCounter:LongAccumulator, calcTimeCounter:LongAccumulator, actualCostCounter:LongAccumulator):Future[(Long, Long, String)] = {
    val dbClient = SimpleGraphStore.getSingleton
    val hostname = HostInfoUtil.getHostName
    val localPairCounter = new AtomicLong()
    val localSumCounter = new AtomicLong()
    val result = Future {
      val t0 = System.nanoTime()
      val counts = new Int2IntOpenHashMap()
      var localCount = 0L
      var localDBTime = 0L
      var localCalcTime = 0L
      var localActualCost = 0L
      for (idx <- 0 until adj.size) {
        val nvid = adj(idx)
        val t0 = System.nanoTime()
        val nadj = dbClient.get(nvid)
        val t1 = System.nanoTime()
        var i = java.util.Arrays.binarySearch(nadj, vid)
        assert(i >= 0, s"Current vid does not belong to its neighbor's adj: vid ${vid}, nvid ${adj(idx)}, nadj [${nadj.mkString(",")}]")
        i = i + 1 // start from the vid larger than the current vid
        while (i < nadj.size) {
          val nnvid = nadj(i)
          counts.addTo(nnvid, 1)
          localCount += 1
          i += 1
        }
        val t2 = System.nanoTime()
        localDBTime += t1 - t0
        localCalcTime += t2 - t1
        localActualCost += nadj.length
      }
      localPairCounter.addAndGet(counts.size())
      localSumCounter.addAndGet(localCount)
      dbTimeCounter.add(localDBTime)
      calcTimeCounter.add(localCalcTime)
      actualCostCounter.add(localActualCost)
      val t1 = System.nanoTime()
      // (Pair Count, Sum Count, Stats)
      (counts.size().toLong, localCount, s"$vid,${adj.size},${counts.size()},${localCount},${localActualCost},$partID,$hostname,${t1-t0},0")
    }(executionContext)
    result
  }

  def calculateLightPartitions(partID:Int, iterator:scala.Iterator[(Int,Array[Int])], options:FinNOROptions,
                         pairCounter:LongAccumulator, sumCounter:LongAccumulator,
                               dbTimeCounter:LongAccumulator, calcTimeCounter:LongAccumulator,
                               actualCostCounter:LongAccumulator):Seq[String] = {
    implicit val executionContext = WorkerThreadPool.getExecutionContext(options.threadNumPerExecutor)
    val results = iterator.toSeq.map { adjPair => processLightVertex(partID, adjPair._1, adjPair._2,
      options, executionContext, dbTimeCounter, calcTimeCounter, actualCostCounter) }
    val lists = Future.sequence(results)
    val stats = Await.result(lists, Duration.Inf)
    for ((pair, sum, stat) <- stats) yield {
      pairCounter.add(pair)
      sumCounter.add(sum)
      stat
    }
  }

  def processHeavyVertex(partID:Int, vid:Int, adj:Array[Int], options:FinNOROptions, executionContext: ExecutionContext,
                         startVid:Int, endVid:Int):Future[(Long, Long, String)] = {
    val dbClient = SimpleGraphStore.getSingleton
    val hostname = HostInfoUtil.getHostName
    val localPairCounter = new AtomicLong()
    val localSumCounter = new AtomicLong()
    val result = Future {
      val t0 = System.nanoTime()
      val counts = new Int2IntOpenHashMap()
      var localCount = 0L
      for (idx <- 0 until adj.size) {
        val nvid = adj(idx)
        val nadj = dbClient.get(nvid)
        // slice the nadj with the (startVid, endVid].
        // convert the startVid and endVid to [startIndex, endIndex].
        var startIndex = java.util.Arrays.binarySearch(nadj, startVid)
        if (startIndex < 0) startIndex =  -startIndex - 1 else startIndex = startIndex + 1
        var endIndex = java.util.Arrays.binarySearch(nadj, endVid)
        if (endIndex < 0) endIndex = -endIndex - 2
        var i = startIndex
        if (i <= endIndex) {
          val nnvid = nadj(i)
          counts.addTo(nnvid, 1)
          localCount += 1
          i += 1
        }
      }
      localPairCounter.addAndGet(counts.size())
      localSumCounter.addAndGet(localCount)
      val t1 = System.nanoTime()
      // (Pair Count, Sum Count, Stats)
      (counts.size().toLong, localCount, s"$vid,${adj.size},${counts.size()},${localCount},$partID,$hostname,${t1-t0},1")
    }(executionContext)
    result
  }

  def calculateHeavyPartitions(partID:Int, iterator:scala.Iterator[(Int,Array[Int],Int,Int)], options:FinNOROptions,
                               pairCounter:LongAccumulator, sumCounter:LongAccumulator):Seq[String] = {
    implicit val executionContext = WorkerThreadPool.getExecutionContext(options.threadNumPerExecutor)
    val results = iterator.toSeq.map {
      case (vid, adj, startVid, endVid) =>
        processHeavyVertex(partID, vid, adj, options, executionContext, startVid, endVid)
    }
    val lists = Future.sequence(results)
    val stats = Await.result(lists, Duration.Inf)
    for ((pair, sum, stat) <- stats) yield {
      pairCounter.add(pair)
      sumCounter.add(sum)
      stat
    }
  }

}



object Debugging {

  def logger = Logger.getLogger("FinNOR")
  type AdjRDD = RDD[(Int, Array[Int])]

  def parseAdjFile(textRDD:RDD[String]): AdjRDD = {
    val adjRDD = textRDD.map(line => {
      val fields = line.split(" ")
      val vid = fields(0).toInt
      val adj = fields.slice(1, fields.length).map(_.toInt).sorted
      (vid, adj)
    })
    adjRDD
  }

  def clearDB() = {
    logger.info("Clearing database, started.")
    SimpleGraphStore.getSingleton.clearDB()
    logger.info("Clearing database, done.")
  }

  def storeAdjToDB(adjRDD:AdjRDD, batchSize:Int) = {
    logger.info("Store to database, started.")
    adjRDD.foreachPartition(partitionIter => {
      val partitionContents = partitionIter.toArray
      partitionContents.grouped(batchSize).foreach(batch => {
        val vids = batch.map(_._1)
        val adjs = batch.map(_._2)
        val dbClient = SimpleGraphStore.getSingleton.put(vids, adjs)
      })
    })
    logger.info("Store to database, done.")
  }

  def calcDegreeThreshold(adjRDD:AdjRDD, proportion:Double):Int = {
    val degrees = adjRDD.map(_._2.size)
    val numVertices = adjRDD.count()
    val topDegreeVertexNum  = (numVertices * proportion).toInt
    val topDegrees = degrees.top(topDegreeVertexNum)
    if (topDegrees.length == 0) {
      logger.info("The length of the top degree array is 0, degree threshold is invalid!")
    }
    val degreeThreshold = if (topDegrees.length > 0) topDegrees.min else -1
    degreeThreshold
  }

  def preprocessing(options:FinNOROptions):Long = {
    logger.info("Enter preprocessing mode.")
    clearDB()
    logger.info("Writing to database, started.")
    val t0 = System.nanoTime()
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"FinNOR-preprocessing-${options.input}")
    val sc = SparkContext.getOrCreate(sparkConf)
    val textRDD = sc.textFile(options.input, options.defaultParallelism).setName("Text RDD")
    val adjRDD = parseAdjFile(textRDD).setName("Adj RDD")
    storeAdjToDB(adjRDD, options.batchSize)
    val t1 = System.nanoTime()
    logger.info("Writing to database, done.")
    logger.info(s"Execution time: ${(t1 - t0)/1e9} s.")
    (t1 - t0)
  }

  def getPivots(partedAdjRDD: RDD[(Array[(Int, Array[Int])], Array[(Int, Array[Int])])], numSplitPerVertex: Int):Array[Int] = {
    val degreePairRDD = partedAdjRDD
      .flatMap(pair => pair._1.map(p => (p._1, p._2.length)) ++ pair._2.map(p => (p._1, p._2.length)))
    val sampleDegreePair = degreePairRDD.takeSample(false, 1000).sortBy(_._1)
    val degreeCostPerSplit = sampleDegreePair.map(_._2).sum / numSplitPerVertex.toDouble
    val pivots = new ArrayBuffer[Int]()
    var i = 0
    var currentCost = 0
    while (i < sampleDegreePair.length) {
      currentCost += sampleDegreePair(i)._2
      if (currentCost > degreeCostPerSplit) {
        pivots.append(sampleDegreePair(i)._1)
        currentCost = 0
      }
      i += 1
    }
    pivots.append(Int.MaxValue)
    pivots.toArray
  }

  def checkCorretness(pair: (Int, Array[Int])):Boolean = {
    val (vid, adj) = pair
    var i = 0
    while (i < adj.length - 1) {
      assert(adj(i) < adj(i + 1), s"Sorted adj error!, vid: ${vid}, adj: [${adj.mkString(",")}], adj(i): ${adj(i)}, adj(i+1): ${adj(i+1)}")
    }

    val dbClient = SimpleGraphStore.getSingleton
    for (idx <- 0 until adj.size) {
      val nvid = adj(idx)
      val nadj = dbClient.get(nvid)
      val i = java.util.Arrays.binarySearch(nadj, vid)
      var j = 0
      while (j < nadj.length) {
        val nnvid = nadj(i)
        if (nnvid < vid) assert(j < i)
        if (nnvid == vid) assert(j == i)
        if (nnvid > vid) assert(j > i)
        j += 1
      }
    }
    true
  }

  def calculation(options:FinNOROptions):Long = {
    logger.info("Enter calculation mode.")
    val t0 = System.nanoTime()
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"FinNOR-calculation-${options.input}")
    val sc = SparkContext.getOrCreate(sparkConf)
    val textRDD = sc.textFile(options.input, options.defaultParallelism).setName("Text RDD")
    val adjRDD = parseAdjFile(textRDD).setName("Adj RDD").repartition(options.numGraphPartitions)
    adjRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    adjRDD.count()
    val t1 = System.nanoTime()
    logger.info(s"Load dataset done. [${(t1 - t0)/1e9}]s")

    val failureRDD = adjRDD.filter(checkCorretness(_)).collect()
    if (failureRDD.length > 0) {
      println(failureRDD.mkString(","))
      assert(failureRDD.length == 0)
    }
    0
  }

  def correctAnswer(adjRDD: AdjRDD):Long = {
    val numEdges = adjRDD.map(pair => pair._2.size).reduce(_ + _)
    val totalSum = adjRDD.map(pair => pair._2.size * pair._2.size.toLong).reduce(_ + _)
    logger.info(s"Number of edges: ${numEdges}, total sum: ${totalSum}")
    (totalSum - numEdges)/2
  }

  def main(args: Array[String]): Unit = {
    logger.info("FinNOR starts!")
    println("FinNOR!")
    val options = new FinNOROptions(args)
    logger.info(s"Input options: " + options)
    if (options.mode == "preprocessing") {
      val executionTime = preprocessing(options)
      println(s"---> Total execution time: ${executionTime/1e9} s.")
    } else {
      val executionTime = calculation(options)
      println(s"---> Total execution time: ${executionTime/1e9} s.")
    }
  }




}