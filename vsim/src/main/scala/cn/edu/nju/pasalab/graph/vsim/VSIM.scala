package cn.edu.nju.pasalab.graph.vsim

import java.util
import java.util.Date

import cn.edu.nju.pasalab.graph.storage.SimpleGraphStore
import cn.edu.nju.pasalab.graph.util.ProcessLevelSingletonManager
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, Long2ObjectOpenHashMap}
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.forkjoin.ForkJoinPool

object VSIM {

  def main(args: Array[String]): Unit = {
    println("VSIM starts." + new Date())
    val options = VSIMOptions(args)
    println("Get options: " + options.toString)
    val spark = SparkSession.builder().appName(s"VSIM: ${options.input}-${options.threshold}")
      .config("spark.sql.shuffle.partitions", options.numPartitions)
      .config("spark.default.parallelism", options.numPartitions)
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val accumulators = new Accumulators(sc)
    val t0 = System.nanoTime()
    val taskDS = spark.read.parquet(options.taskFile).as[VertexRecord].cache()
    val numVertices = taskDS.count()
    println(s"Number of vertices: ${numVertices}")
    val t1 = System.nanoTime()
    val results = taskDS.rdd.mapPartitions(recordIterator => {
      val pool = WorkerThreadPool.getForkJoinPool(options.threadNumPerExecutor)
      val taskSupport = new ForkJoinTaskSupport(pool)
      val records = recordIterator.toArray.sortBy(r => -r.vid) // Process the high degree vertices with higher priority
      val scTasks = records.par
      val graphStore = SimpleGraphStore.getSingleton
      scTasks.tasksupport = taskSupport
      // Run the SC task
      val results = scTasks.flatMap(record => processVertex(record.vid, record.adj, options, graphStore, accumulators))
      results.iterator
    })
    if (options.doOutput) {
      results.saveAsTextFile(options.outputPath)
      val t2 = System.nanoTime()
      println("Done!" + new Date())
      println(s"Elasped time (s): ${(t2 - t0) / 1e9}")
      println(s" - Data input time (s): ${(t1 - t0) / 1e9}")
      println(s" - Computation time (s): ${(t2 - t1) / 1e9}")
    } else {
      val (resultCount, useVerifyModeCount, recordCount) = results.map(pair => (pair.v1, pair.v2, 1L))
        .reduce((statA, statB) => (statA._1 + statB._1, statA._2 + statB._2, statA._3 + statB._3))
      val t2 = System.nanoTime()
      println("Done!" + new Date())
      println(s"Elasped time (s): ${(t2 - t0) / 1e9}")
      println(s" - Data input time (s): ${(t1 - t0) / 1e9}")
      println(s" - Computation time (s): ${(t2 - t1) / 1e9}")
      println(s"Number of result pairs: ${resultCount}")
      println(s"Number of vertices using the verification mode: ${useVerifyModeCount}")
      println(s"Number of vertices using the probe-count mode: ${recordCount - useVerifyModeCount}")
      println(s"Proportion of vertices using the verification mode: ${(useVerifyModeCount.toDouble / recordCount)}")
    }
    spark.stop()
    System.exit(0)
  }


  def processVertex(vid: Long, adj:Array[Long], options: VSIMOptions,
                    graphStore: SimpleGraphStore, accumulators: Accumulators): Seq[ResultPair] = {
    val threshold = options.threshold
    val rLen = adj.length
    val compVid = (rLen.toLong << 32L) | vid
    val prefixLen = midPrefixLen(threshold, rLen)
    val lenUpperBound = maxLen(threshold, rLen)
    val overlaps = new Long2IntOpenHashMap()
    val ovThresholds = new Long2IntOpenHashMap()

    def getDegreeFromCompVid(compVid: Long): Int = {
      (compVid >> 32L).toInt
    }

    ///// Process prefix tokens
    val prefixTokensWithPosIndex = adj.slice(0, prefixLen).zipWithIndex
    prefixTokensWithPosIndex.grouped(options.dbBatchSize).foreach(tokenPoses => {
      val nvids = tokenPoses.map(_._1)
      val nadjs = graphStore.get(nvids)
      for (i <- 0 until tokenPoses.length) {
        val nvid = nvids(i)
        val nadj = nadjs(i)
        val pos = tokenPoses(i)._2
        var iPos = util.Arrays.binarySearch(nadj, compVid)
        assert(iPos >= 0, s"Cannot find ${vid} in its neighbor ${nadj}'s adjacency set.")
        iPos += 1
        while (iPos < nadj.length && getDegreeFromCompVid(nadj(iPos)) <= lenUpperBound) {
          val nnvid = nadj(iPos)
          overlaps.addTo(nnvid, 1)
          iPos += 1
        }
      }
    })

    ///// Prune the candidate set with the positional filter
    for (cvid <- overlaps.keySet().toLongArray) {
      val overlap = overlaps.get(cvid)
      val sLen = getDegreeFromCompVid(cvid)
      assert(sLen >= rLen)
      val overlapThreshold = minOverlap(threshold, rLen, sLen)
      if (overlap + rLen - prefixLen < overlapThreshold) {
        overlaps.remove(cvid) // pruned
      } else {
        ovThresholds.put(cvid, overlapThreshold)
      }
    }
    accumulators.numCandidates.add(overlaps.size())

    val results = new ArrayBuffer[ResultPair]()
    var resultCount = 0L
    var useVerifyMode = 0L

    options.mode match {
      case "verification" => {
        useVerifyMode = 1L
        verificationMode()
      }
      case "probe-count" => {
        probeCountMode()
      }
      case _ => {
        // Adaptive
        val verifyModeCost = estimateVerificationCost(overlaps, vid, options)
        val probeCountModeCost = estimateCountingCost(adj.slice(prefixLen, adj.length), options)
        if (verifyModeCost <= probeCountModeCost) {
          ///// Verify Mode
          useVerifyMode = 1L
          verificationMode()
        } else {
          ///// Probe-count mode
          probeCountMode()
        } // end of probe-count mode
      }
    }

    def verificationMode(): Unit = {
      accumulators.numVerifyMode.add(1L)
      val candidates = overlaps.keySet().toLongArray
      candidates.grouped(options.dbBatchSize).foreach(vids => {
        val sAdjs = graphStore.get(vids)
        for (i <- 0 until vids.length) {
          val cvid = vids(i)
          val overlap = overlaps.get(cvid)
          val sLen = getDegreeFromCompVid(cvid)
          val overlapThreshold = ovThresholds.get(cvid)
          val sadj = sAdjs(i)
          val score = verify(overlap, adj, prefixLen, sadj, overlap, overlapThreshold)
          if (score >= threshold) {
            accumulators.numResult.add(1L)
            resultCount += 1L
            if (options.doOutput)
              results.append(ResultPair(vid, cvid, score))
          }
        }
      })
    }

    def probeCountMode() = {
      accumulators.numProbeCountMode.add(1L)
      val suffixTokensWithPosIndex = adj.slice(prefixLen, adj.length).zipWithIndex
      suffixTokensWithPosIndex.grouped(options.dbBatchSize).foreach(tokenPoses => {
        val nvids = tokenPoses.map(_._1)
        val nadjs = graphStore.get(nvids)
        for (i <- 0 until tokenPoses.length) {
          val nvid = nvids(i)
          val nadj = nadjs(i)
          val pos = tokenPoses(i)._2
          var iPos = util.Arrays.binarySearch(nadj, compVid)
          assert(iPos >= 0, s"Cannot find ${vid} in its neighbor ${nadj}'s adjacency set.")
          iPos += 1
          while (iPos < nadj.length && getDegreeFromCompVid(nadj(iPos)) <= lenUpperBound) {
            val nnvid = nadj(iPos)
            overlaps.addTo(nnvid, 1)
            iPos += 1
          }
        }
      }) // end of scanning adj
      import scala.collection.JavaConverters._
      for (entry <- overlaps.long2IntEntrySet().asScala) {
        val cvid = entry.getLongKey
        val overlap = entry.getIntValue
        val sLen = getDegreeFromCompVid(cvid)
        val score = jaccard(overlap, sLen, rLen)
        if (score >= threshold) {
          accumulators.numResult.add(1L)
          resultCount += 1L
          if (options.doOutput)
            results.append(ResultPair(vid, cvid, score))
        }
      } // end of for loop of candidate pairs
    } // end of probe-count mode

    if (options.doOutput)
      results
    else {
      Array(ResultPair(resultCount, useVerifyMode, Double.NaN))
    }
  }


  private[this] def midPrefixLen(threshold: Double, len: Int): Int = {
    len - minOverlap(threshold, len, len) + 1
  }

  private[this] def minOverlap(threshold: Double, len1: Int, len2: Int): Int = {
    Math.ceil(threshold / (1 + threshold) * (len1 + len2)).toInt
  }

  private def maxLen(threshold: Double, len: Int): Int = {
    Math.max(Int.MaxValue, Math.floor(len / threshold).toInt)
  }

  private[this] def verify(overlap: Int, rRecord: Array[Long], rNextPos: Int, sRecord: Array[Long], sNextPos: Int,
                           ovThreshold: Int): Double = {
    var o = overlap
    var rPos = rNextPos
    var sPos = sNextPos
    while (rPos < rRecord.length
      && sPos < sRecord.length
      && Math.min(rRecord.length - rPos, sRecord.length - sPos) + o >= ovThreshold) {
      if (rRecord(rPos) < sRecord(sPos))
        rPos += 1
      else if (sRecord(sPos) < rRecord(rPos))
        sPos += 1
      else {
        o += 1
        rPos += 1
        sPos += 1
      }
    }
    if (o >= ovThreshold)
      o.toDouble / (rRecord.length + sRecord.length - o)
    else
      -1.0
  }

  private def jaccard(overlap: Int, len1: Int, len2: Int): Double = {
    overlap.toDouble / (len1 + len2 - overlap)
  }

  private def retrieveDegree(vid:Long):Int = {
    (vid >> 32L).toInt
  }

  private def estimateVerificationCost(candidates:Long2IntOpenHashMap, vi:Long, options: VSIMOptions):Double = {
    var cost = 0.0
    val di = retrieveDegree(vi)
    val citer = candidates.keySet().iterator()
    while (citer.hasNext) {
      val cvid = citer.nextLong()
      val dc = retrieveDegree(cvid)
      cost += options.alpha * dc + options.beta + options.gamma * (di + dc)
    }
    cost
  }

  private def estimateCountingCost(suffix:Array[Long], options: VSIMOptions):Double = {
    var cost = 0.0
    for (nvid <- suffix) {
      val dn = retrieveDegree(nvid)
      cost += options.alpha * dn + options.beta + options.gammaPrime * dn
    }
    cost
  }


}

case class VSIMOptions(args: Array[String]) {

  val input = args(0)
  val numPartitions = args(1).toInt
  val mode = args(2)
  val threshold = args(3).toDouble
  val threadNumPerExecutor = args(4).toInt
  val outputStatsInfo = args(5).toBoolean
  val outputPath = args(6)
  val dbBatchSize = args(7).toInt
  val doOutput = outputPath != "null"
  val taskFile = args(8)
  val alpha = args(9).toDouble
  val beta = args(10).toDouble
  val gamma = args(11).toDouble
  val gammaPrime = args(12).toDouble

  override def toString: String = {
    s"""
       |=== Options ===
       |1.input-related:
       | input: $input
       | task file: $taskFile
       | number of partitions: $numPartitions
       |2.similarity calculation:
       | number of threads per executor: $threadNumPerExecutor
       | similarity threshold: $threshold
       | mode: $mode
       |3. output related:
       | output stats information: $outputStatsInfo
       | output file path: $outputPath
       |4. DB related:
       | read batch size: $dbBatchSize
       |5. Cost estimation:
       | alpha: $alpha, beta: $beta
       | gamma: $gamma, gamma': $gammaPrime
     """.stripMargin
  }

}

object WorkerThreadPool {

  def getExecutionContext(numThread: Int): ExecutionContext = {
    def createExecutionContext(ind: Int): ExecutionContext = {
      val forkJoinPool = new ForkJoinPool(numThread)
      val executionContext = ExecutionContext.fromExecutorService(forkJoinPool)
      executionContext
    }

    ProcessLevelSingletonManager.fetch(5, createExecutionContext).asInstanceOf[ExecutionContext]
  }

  def getForkJoinPool(numThread: Int): ForkJoinPool = {
    def createForkJoinPool(ind: Int): ForkJoinPool = {
      val forkJoinPool = new ForkJoinPool(numThread)
      forkJoinPool
    }

    ProcessLevelSingletonManager.fetch(4, createForkJoinPool).asInstanceOf[ForkJoinPool]
  }

}

case class Accumulators() {

  var numCandidates: LongAccumulator = null
  var numResult: LongAccumulator = null
  var numVerifyMode: LongAccumulator = null
  var numProbeCountMode: LongAccumulator = null

  def this(sc: SparkContext) = {
    this()
    numCandidates = sc.longAccumulator("Number of candidate")
    numResult = sc.longAccumulator("Number of result pair")
    numVerifyMode = sc.longAccumulator("Number of verification mode usage")
    numProbeCountMode = sc.longAccumulator("Number of probe-count mode usage")
  }
}

class KeyPartitioner(val numPartitions:Int) extends org.apache.spark.Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int] % numPartitions
}
