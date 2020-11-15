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


/**
  * Version 3.2 of the FinNOR v3 series.
  * It does not have significant effect. It is abandoned.
  */
object FinNORV3_2 {

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

    val degreeThreshold = calcDegreeThreshold(adjRDD, 10 * options.numGraphPartitions)


    val filteredAdjRDD = adjRDD.filter(_._2.length < degreeThreshold)
    val statsLines = filteredAdjRDD.mapPartitionsWithIndex ((partID, partitionIterator) =>
      calculateLightPartitions(partID, partitionIterator, options,  numResultPairCounter, numResultSumCounter,
        dbTimeCounter, calcTimeCounter, actualCalcCostCounter), true)

    if (options.outputStatsInfo) {
      HDFSUtils.deleteFile(options.outputPath)
      logger.info(s"output file ${options.outputPath} deleted.")
      statsLines.saveAsTextFile(options.outputPath)
    } else {
      statsLines.count()
    }


    val t2 = System.nanoTime()
    logger.info(s"Degree threshold: ${degreeThreshold}.")
    logger.info(s"Calculation done for adj pairs. [${(t2 - t1)/1e9}]")
    logger.info(s"Number of result pairs: ${numResultPairCounter.value}, Number of result sum: ${numResultSumCounter.value}")
    logger.info(s"DB Time Counter (ns): ${dbTimeCounter.value}, Calc Time Counter (ns): ${calcTimeCounter.value}")
    logger.info(s"Actual computing cost: ${actualCalcCostCounter}")

    val heavyStatsLines = processHeavyPart(adjRDD, degreeThreshold, options.numGraphPartitions,
      numResultPairCounter, numResultSumCounter)
    if (options.outputStatsInfo) {
      val outputPath = options.outputPath + "-heavy"
      HDFSUtils.deleteFile(outputPath)
      logger.info(s"output file ${outputPath} deleted.")
      heavyStatsLines.saveAsTextFile(outputPath)
    } else {
      heavyStatsLines.count()
    }
    val t3 = System.nanoTime()
    logger.info(s"Calculation done for heavy part. [${(t3 - t2)/1e9}]")
    logger.info(s"After heavy part, number of result pairs: ${numResultPairCounter.value}, number of result sum: ${numResultSumCounter.value}")
    t3 - t0
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

  def processLightVertex(partID:Int, vid:Int, adj:Array[Int], options:FinNOROptions,
                         dbTimeCounter:LongAccumulator, calcTimeCounter:LongAccumulator, actualCostCounter:LongAccumulator):(Long, Long, String) = {
    val dbClient = SimpleGraphStore.getSingleton
    val hostname = HostInfoUtil.getHostName
    val localPairCounter = new AtomicLong()
    val localSumCounter = new AtomicLong()
    val result = {
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
    }
    result
  }

  def calculateLightPartitions(partID:Int, iterator:scala.Iterator[(Int,Array[Int])], options:FinNOROptions,
                               pairCounter:LongAccumulator, sumCounter:LongAccumulator,
                               dbTimeCounter:LongAccumulator, calcTimeCounter:LongAccumulator,
                               actualCostCounter:LongAccumulator):Iterator[String] = {
    val results = iterator.map { adjPair =>
      val (pair, sum, stat) = processLightVertex(partID, adjPair._1, adjPair._2,
      options, dbTimeCounter, calcTimeCounter, actualCostCounter)
      pairCounter.add(pair)
      sumCounter.add(sum)
      stat
    }
    results
  }

  def calcDegreeThreshold(adjRDD:AdjRDD, numTop:Int):Int = {
    val degrees = adjRDD.map(_._2.size)
    val topDegrees = degrees.top(numTop)
    if (topDegrees.length == 0) {
      logger.info("The length of the top degree array is 0, degree threshold is invalid!")
    }
    val degreeThreshold = if (topDegrees.length > 0) topDegrees.min else -1
    degreeThreshold
  }

  def processHeavyPart(adjRDD: AdjRDD, degreeThreshold:Int, parallelism:Int,
                       numResultPairCounter:LongAccumulator, numResultSumCounter:LongAccumulator):RDD[String] = {
    val sc = adjRDD.sparkContext
    val heavyVertexRDD = adjRDD.filter(_._2.length >= degreeThreshold)
    import scala.util.Random
    val heavyVertexIDs = Random.shuffle(heavyVertexRDD.map(_._1).collect().toSeq)
    val numGroups = Math.ceil(Math.sqrt(parallelism)).toInt
    val groupSize = Math.ceil(heavyVertexIDs.size / numGroups.toDouble).toInt
    val groupedHeavyVertexIDs = heavyVertexIDs.grouped(groupSize).toSeq
    val bc_groupedHeavyVertexIDs = sc.broadcast(groupedHeavyVertexIDs)
    val tasks = for(x <- 0 until groupedHeavyVertexIDs.size; y <- 0 until groupedHeavyVertexIDs.size) yield (x,y)
    val taskRDD = sc.parallelize(tasks, parallelism)
    val statLinesRDD:RDD[String] = taskRDD.flatMap {
      case (idA, idB) => {
        val partA = bc_groupedHeavyVertexIDs.value(idA)
        val partB = bc_groupedHeavyVertexIDs.value(idB)
        val dbClient = SimpleGraphStore.getSingleton
        var localPairCount = 0L
        var localSumCount = 0L
        val statsLines = for (vid1 <- partA; vid2 <- partB if vid1 < vid2) yield {
          val t0 = System.nanoTime()
          val adj1 = dbClient.get(vid1)
          val adj2 = dbClient.get(vid2)
          val t1 = System.nanoTime()
          val overlap = calcOverlap(adj1, adj2)
          val t2 = System.nanoTime()
          if (overlap > 0) {
            localPairCount += 1L
            localSumCount += overlap
          }
          s"${vid1},${vid2},${overlap},${t1-t0},${t2-t1}"
        }
        numResultPairCounter.add(localPairCount)
        numResultSumCounter.add(localSumCount)
        statsLines
      }
    }
    statLinesRDD
  }

  /**
    * Calculate the overlap of two adjacency lists.
    * @param adj1 sorted adjacency list.
    * @param adj2 sorted adjacency list.
    * @return number of overlaps
    */
  private def calcOverlap(adj1:Array[Int], adj2:Array[Int]):Int = {
    var i = 0
    var j = 0
    var overlap = 0
    while (i < adj1.length && j < adj2.length) {
      if (adj1(i) < adj2(j)) i += 1
      else if (adj1(i) > adj2(j)) j+= 1
      else {
        overlap += 1
        i += 1
        j += 1
      }
    }
    overlap
  }


}