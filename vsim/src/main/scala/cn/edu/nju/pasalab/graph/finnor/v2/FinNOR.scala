package cn.edu.nju.pasalab.graph.finnor.v2

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
  val degreeProportion = args(7).toDouble
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
       | proportion as top degrees: $degreeProportion
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

object FinNOR {

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

  def calculation(options:FinNOROptions):Long = {
    logger.info("Enter calculation mode.")
    val t0 = System.nanoTime()
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"FinNOR-calculation-${options.input}")
    val sc = SparkContext.getOrCreate(sparkConf)
    val textRDD = sc.textFile(options.input, options.defaultParallelism).setName("Text RDD")
    val adjRDD = parseAdjFile(textRDD).setName("Adj RDD").persist(StorageLevel.MEMORY_AND_DISK_SER)
    adjRDD.count()
    val t1 = System.nanoTime()
    logger.info(s"Load dataset done. [${(t1 - t0)/1e9}]s")

    val numResultPairCounter = sc.longAccumulator("# of result pairs")
    val numResultSumCounter = sc.longAccumulator("# of result sum")
    val degreeThreshold = if (options.degreeProportion < 0) Int.MaxValue else calcDegreeThreshold(adjRDD, options.degreeProportion)
    val t2 = System.nanoTime()
    logger.info(s"Degree threshold: ${degreeThreshold}")
    logger.info(s"Get degree threshold done. [${(t2 - t1)/1e9}]")

    // Part1: Calculate by ScanCount
    val filteredAdjRDD = adjRDD.filter(_._2.size < degreeThreshold)
    val vidRDD = adjRDD.map(_._1)
    val vertexPartitioner = new HashPartitioner(options.numGraphPartitions)
    val partitionedVidRDD = vidRDD.map(vid => (vid, vid)).partitionBy(vertexPartitioner)
    val statsLines = partitionedVidRDD.mapPartitions(calculatePartitions(_, options, numResultPairCounter, numResultSumCounter).iterator,
      true)
    if (options.outputStatsInfo) {
      HDFSUtils.deleteFile(options.outputPath)
      logger.info(s"output file ${options.outputPath} deleted.")
      statsLines.saveAsTextFile(options.outputPath)
    } else {
      //val resultCount = statsLines.map(line => line.split(",")(3).toLong).reduce(_ + _)
      //logger.info(s"Result count calculated by status line: ${resultCount}")
      statsLines.count()
    }
    val t3 = System.nanoTime()
    logger.info(s"Part 1 calculation done. [${(t3 - t2)/1e9}]")
    logger.info(s"After Part 1, Number of result pairs: ${numResultPairCounter.value}, Number of result sum: ${numResultSumCounter.value}")
    // Part2: Calculate by Counting co-occurances
    if (degreeThreshold > 0) {
      val highDegreeVertices = adjRDD.filter(_._2.size >= degreeThreshold).map(_._1).collect()
      val highDegreeVerticesSet = new IntOpenHashSet(highDegreeVertices)
      val bc_highDegreeVertices = sc.broadcast(highDegreeVerticesSet)
      logger.info(s"Number of high degree vertices: ${highDegreeVerticesSet.size()}")
      val coPairs = adjRDD.flatMap {
        case (vid, adj) => {
          val highAdj = adj.filter(bc_highDegreeVertices.value.contains(_))
          for (i <- 0 until highAdj.length - 1; j <- i + 1 until highAdj.length) yield (i.toLong << 32 | j, 1)
        }
      }
      val reducedCoPairs = coPairs.reduceByKey(_ + _)
      reducedCoPairs.foreachPartition(iterater => {
        var localSumCount = 0L
        var localPairCount = 0
        iterater.foreach(pair => {
          localPairCount += 1
          localSumCount += pair._2
        })
        numResultPairCounter.add(localPairCount)
        numResultSumCounter.add(localSumCount)
      })
    } else {
      logger.info("The top degree threshold is invalid. Skip the part 2.")
    }
    val t4 = System.nanoTime()
    logger.info(s"Part 2 calculation done. [${(t4 - t3)/1e9}]")
    logger.info(s"After Part 2, Number of result pairs: ${numResultPairCounter.value}, Number of result sum: ${numResultSumCounter.value}")
    logger.info(s"Correct number of sum: ${correctAnswer(adjRDD)}")
    t4 - t0
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

  def calculatePartitions(iterator:scala.Iterator[(Int,Int)], options:FinNOROptions,
                         pairCounter:LongAccumulator, sumCounter:LongAccumulator):Seq[String] = {
    implicit val executionContext = WorkerThreadPool.getExecutionContext(options.threadNumPerExecutor)
    val dbClient = SimpleGraphStore.getSingleton
    val hostname = HostInfoUtil.getHostName
    val localPairCounter = new AtomicLong()
    val localSumCounter = new AtomicLong()
    val results = iterator.toSeq.map {
      case (partID, vid) => {
        val resultFuture = Future {
          val t0 = System.nanoTime()
          val adj = dbClient.get(vid)
          val counts = new Int2IntOpenHashMap()
          var localCount = 0L
          for (idx <- 0 until adj.size) {
            val nvid = adj(idx)
            val nadj = dbClient.get(nvid)
            var i = java.util.Arrays.binarySearch(nadj, vid)
            //var i = 0
            assert(i >= 0, s"Current vid does not belong to its neighbor's adj: vid ${vid}, nvid ${adj(idx)}, nadj [${nadj.mkString(",")}]")
            i = i + 1 // start from the vid larger than the current vid
            while (i < nadj.size) {
              val nnvid = nadj(i)
              counts.addTo(nnvid, 1)
              localCount += 1
              i += 1
            }
          }
          localPairCounter.addAndGet(counts.size())
          localSumCounter.addAndGet(localCount)
          val t1 = System.nanoTime()
          s"$vid,${adj.size},${counts.size()},${localCount},$partID,$hostname,${t1-t0}"
        }(executionContext)
        resultFuture
      }
    }
    val lists = Future.sequence(results)
    val statsLines = Await.result(lists, Duration.Inf)
    pairCounter.add(localPairCounter.get())
    sumCounter.add(localSumCounter.get())
    statsLines
  }

}

class IdentityPartitioner(val numPartitions:Int) extends org.apache.spark.Partitioner {

  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}

class ModPartitioner(val numPartitions:Int) extends org.apache.spark.Partitioner {
  override def getPartition(key: Any): Int = {Math.abs(key.asInstanceOf[Int]) % numPartitions}
}
