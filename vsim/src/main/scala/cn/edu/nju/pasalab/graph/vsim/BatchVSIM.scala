package cn.edu.nju.pasalab.graph.vsim

import java.util
import java.util.Date

import cn.edu.nju.pasalab.graph.storage.SimpleGraphStore
import cn.edu.nju.pasalab.graph.util.ProcessLevelSingletonManager
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, Long2ObjectOpenHashMap, LongArrayList, LongOpenHashSet}
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.forkjoin.ForkJoinPool


class SCTask(record:VertexRecord, options:VSIMOptions, accumulators: Accumulators) {

  val vid = record.vid
  val adj = record.adj
  val taskStatus = new TaskStatus()
  val threshold = options.threshold
  val rLen = adj.length
  val compVid = (rLen.toLong << 32L) | vid
  val prefixLen = midPrefixLen(threshold, rLen)
  taskStatus.prefixLen = prefixLen
  taskStatus.suffixLen = adj.length - prefixLen
  val lenUpperBound = maxLen(threshold, rLen)
  val overlaps = new Long2IntOpenHashMap()
  val ovThresholds = new Long2IntOpenHashMap()
  val results = new ArrayBuffer[ResultPair]()
  var resultCount = 0L
  var useVerifyMode = 0L

  private def getDegreeFromCompVid(compVid: Long): Int = {
    (compVid >> 32L).toInt
  }

  private def midPrefixLen(threshold: Double, len: Int): Int = {
    len - minOverlap(threshold, len, len) + 1
  }

  private def minOverlap(threshold: Double, len1: Int, len2: Int): Int = {
    Math.ceil(threshold / (1 + threshold) * (len1 + len2)).toInt
  }

  private def maxLen(threshold: Double, len: Int): Int = {
    Math.max(Int.MaxValue, Math.floor(len / threshold).toInt)
  }

  private def verify(overlap: Int, rRecord: Array[Long], rNextPos: Int, sRecord: Array[Long], sNextPos: Int,
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

  def verificationMode(adjs:Long2ObjectOpenHashMap[Array[Long]]): Unit = {
    accumulators.numVerifyMode.add(1L)
    val candidates = overlaps.keySet().toLongArray
    candidates.foreach(cvid => {
        val overlap = overlaps.get(cvid)
        val sLen = getDegreeFromCompVid(cvid)
        val overlapThreshold = ovThresholds.get(cvid)
        assert(adjs.containsKey(cvid))
        val sadj = adjs.get(cvid)
        val score = verify(overlap, adj, prefixLen, sadj, overlap, overlapThreshold)
        if (score >= threshold) {
          accumulators.numResult.add(1L)
          resultCount += 1L
          if (options.doOutput) {
  //          results.append(ResultPair(vid, cvid, score))
          }
        }
    })
  }

  def probeCountMode(adjs:Long2ObjectOpenHashMap[Array[Long]]) = {
    val tSuffixStart = System.nanoTime()
    accumulators.numProbeCountMode.add(1L)
    val suffixTokensWithPosIndex = adj.slice(prefixLen, adj.length).zipWithIndex
    suffixTokensWithPosIndex.foreach(tokenPose => {
      val nvid = tokenPose._1
      assert(adjs.containsKey(nvid))
      val nadj = adjs.get(nvid)
      val pos = tokenPose._2
      var iPos = util.Arrays.binarySearch(nadj, compVid)
      assert(iPos >= 0, s"Cannot find ${vid} in its neighbor ${nadj}'s adjacency set.")
      iPos += 1
      while (iPos < nadj.length && getDegreeFromCompVid(nadj(iPos)) <= lenUpperBound) {
        val nnvid = nadj(iPos)
        overlaps.addTo(nnvid, 1)
        iPos += 1
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
        if (options.doOutput) {
          //results.append(ResultPair(vid, cvid, score))
        }
      }
    } // end of for loop of candidate pairs
    val tSuffixEnd = System.nanoTime()
    taskStatus.suffixTotalTime = (tSuffixEnd - tSuffixStart) / 1e6
    taskStatus.resultCount = resultCount.toInt
  }

  def getDBQueriesForPrefixPhase():Seq[Long] = {
    adj.slice(0, prefixLen)
  }
  def processPrefixPhase(adjs:Long2ObjectOpenHashMap[Array[Long]]):Unit = {
    ///// Process prefix tokens
    val tPrefixStart = System.nanoTime()
    val prefixTokensWithPosIndex = adj.slice(0, prefixLen).zipWithIndex
    prefixTokensWithPosIndex.foreach(tokenPose => {
        val nvid = tokenPose._1
        assert(adjs.containsKey(nvid))
        val nadj = adjs.get(nvid)
        val pos = tokenPose._2
        var iPos = util.Arrays.binarySearch(nadj, compVid)
        assert(iPos >= 0, s"Cannot find ${vid} in its neighbor ${nadj}'s adjacency set.")
        iPos += 1
        while (iPos < nadj.length && getDegreeFromCompVid(nadj(iPos)) <= lenUpperBound) {
          val nnvid = nadj(iPos)
          overlaps.addTo(nnvid, 1)
          iPos += 1
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
    taskStatus.candidateCount = overlaps.size()

    options.mode match {
      case "verification" => {
        taskStatus.execMode = 1
        useVerifyMode = 1L
      }
      case "probe-count" => {
        taskStatus.execMode = 0
        useVerifyMode = 0L
      }
      case _ => {
        // Adaptive
        val verifyModeCost = estimateVerificationCost(overlaps, vid, options)
        val probeCountModeCost = estimateCountingCost(adj.slice(prefixLen, adj.length), options)
        taskStatus.estimVerify = verifyModeCost
        taskStatus.estimProbe = probeCountModeCost

        if (verifyModeCost <= probeCountModeCost) {
          taskStatus.execMode = 1
          useVerifyMode = 1L
        } else {
          taskStatus.execMode = 0
          useVerifyMode = 0L
        } // end of probe-count mode
      }
    }
    val tPrefixEnd = System.nanoTime()
    taskStatus.prefixTotalTime  = (tPrefixEnd - tPrefixStart) / 1e6
  }
  def getDBQueriesForSuffixPhase():Seq[Long] = {
    if (useVerifyMode == 1L) {
      val queries = overlaps.keySet().toLongArray
      queries
    } else if (useVerifyMode == 0L){
      val queries = adj.slice(prefixLen, adj.length)
      queries
    } else {
      Array[Long]()
    }
  }

  def processSuffixPhase(adjs:Long2ObjectOpenHashMap[Array[Long]]):Unit = {
    if (useVerifyMode == 1L) {
      verificationMode(adjs)
    } else if (useVerifyMode == 0L) {
      probeCountMode(adjs)
    }
  }
  def getResults():Seq[ResultPair] = {
    if (options.doOutput)
      results
    else {
      Array(ResultPair(resultCount, useVerifyMode, Double.NaN))
    }
  }
  def getTaskStats():TaskStatus = taskStatus
}

object BatchVSIM {

  def main(args: Array[String]): Unit = {
    println("VSIM batch mode starts." + new Date())
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
    import scala.collection.JavaConverters._
    val results:RDD[ResultPair] = taskDS.rdd.mapPartitions(recordIterator => {
      val db = SimpleGraphStore.getSingleton
      val allTasks = recordIterator.map(record => new SCTask(record, options, accumulators)).toArray
      val numTaskPerThread = allTasks.length / options.threadNumPerExecutor
      val splittedTasks = allTasks.grouped(numTaskPerThread).toSeq.par
      splittedTasks.tasksupport = new ForkJoinTaskSupport(WorkerThreadPool.getForkJoinPool(options.threadNumPerExecutor))
      val resultsFromTaskGroups = splittedTasks.flatMap(taskGroup => {
        val tasksInPrefixPhase = taskGroup.iterator
        val tasksInSuffixPhase = new util.LinkedList[SCTask]()
        val taskStats = new ArrayBuffer[TaskStatus]()
        val results = new ArrayBuffer[ResultPair]()
        while (!tasksInSuffixPhase.isEmpty || tasksInPrefixPhase.hasNext) {
          val dbBatchQuery = new LongOpenHashSet(options.dbBatchSize)
          val prefixTasksInThisBatch = new ArrayBuffer[SCTask]()
          val suffixTasksInThisBatch = new ArrayBuffer[SCTask]()
          while (!tasksInSuffixPhase.isEmpty && dbBatchQuery.size() < options.dbBatchSize) {
            val task = tasksInSuffixPhase.pop()
            val queries = task.getDBQueriesForSuffixPhase()
            queries.foreach(vid => dbBatchQuery.add(vid))
            suffixTasksInThisBatch.append(task)
          }
          while (dbBatchQuery.size() < options.dbBatchSize && tasksInPrefixPhase.hasNext) {
            val task = tasksInPrefixPhase.next()
            val queries = task.getDBQueriesForPrefixPhase()
            queries.foreach(vid => dbBatchQuery.add(vid))
            prefixTasksInThisBatch.append(task)
          }
          val queryArray = dbBatchQuery.toLongArray
          val adjs = db.get(queryArray)
          val adjMap = new Long2ObjectOpenHashMap[Array[Long]]()
          for (i <- 0 until dbBatchQuery.size())
            adjMap.put(queryArray(i), adjs(i))
          for (task <- suffixTasksInThisBatch) {
            task.processSuffixPhase(adjMap)
            taskStats.append(task.getTaskStats())
            results.append(task.getResults(): _*)
          }
          for (task <- prefixTasksInThisBatch) {
            task.processPrefixPhase(adjMap)
            tasksInSuffixPhase.add(task)
          }
        }
        results
      })
      resultsFromTaskGroups.iterator
        /*
      val tasksInPrefixPhase = recordIterator.map(record => new SCTask(record, options, accumulators))
      val tasksInSuffixPhase = new util.LinkedList[SCTask]()
      val taskStats = new ArrayBuffer[TaskStatus]()
      val results = new ArrayBuffer[ResultPair]()
      val db = SimpleGraphStore.getSingleton
      while (!tasksInSuffixPhase.isEmpty || tasksInPrefixPhase.hasNext) {
        val dbBatchQuery = new LongOpenHashSet(options.dbBatchSize)
        val prefixTasksInThisBatch = new ArrayBuffer[SCTask]()
        val suffixTasksInThisBatch = new ArrayBuffer[SCTask]()
        while(!tasksInSuffixPhase.isEmpty && dbBatchQuery.size() < options.dbBatchSize) {
          val task = tasksInSuffixPhase.pop()
          val queries = task.getDBQueriesForSuffixPhase()
          queries.foreach(vid => dbBatchQuery.add(vid))
          suffixTasksInThisBatch.append(task)
        }
        while(dbBatchQuery.size() < options.dbBatchSize && tasksInPrefixPhase.hasNext) {
          val task = tasksInPrefixPhase.next()
          val queries = task.getDBQueriesForPrefixPhase()
          queries.foreach(vid => dbBatchQuery.add(vid))
          prefixTasksInThisBatch.append(task)
        }
        val queryArray = dbBatchQuery.toLongArray
        val adjs = db.get(queryArray)
        val adjMap = new Long2ObjectOpenHashMap[Array[Long]]()
        for (i <- 0 until dbBatchQuery.size())
          adjMap.put(queryArray(i), adjs(i))
        val suffixFutures = suffixTasksInThisBatch.map(task => Future {
          task.processSuffixPhase(adjMap)
          (task.getTaskStats(), task.getResults())
        }(ec))
        val prefixFutures = prefixTasksInThisBatch.map(task => Future {
          task.processPrefixPhase(adjMap)
          task
        }(ec))
        import scala.concurrent.Await
        val suffixResults = suffixFutures.map(f => Await.result(f, Duration.Inf))
        suffixResults.foreach(pair => {
          taskStats.append(pair._1)
          results.appendAll(pair._2)
        })
        val prefixTasks = prefixFutures.map(f => Await.result(f, Duration.Inf))
        tasksInSuffixPhase.addAll(prefixTasks.asJava)
        /*
        for (task <- suffixTasksInThisBatch) {
          task.processSuffixPhase(adjMap)
          taskStats.append(task.getTaskStats())
          results.append(task.getResults(): _*)
        }
        for (task <- prefixTasksInThisBatch) {
          task.processPrefixPhase(adjMap)
          tasksInSuffixPhase.add(task)
        }
        */
      }
         */
//      taskStats.iterator
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
}

