package cn.edu.nju.pasalab.graph.partitioning

import java.io.PrintWriter

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2LongOpenHashMap, Int2ObjectOpenHashMap, IntArrayList, IntOpenHashSet}

import scala.io.Source
import scala.collection.JavaConverters._

/**
  * Convert the file format from the adjacency list file to the metis file.
  */
object ConvertFileFormatToMetis {

  type AdjMap = Int2ObjectOpenHashMap[IntArrayList]

  def readAdjFile(path: String) = {
    val adjMap = new AdjMap()
    val lineIterator = Source.fromFile(path).getLines()
    var count = 0
    for (l <- lineIterator if !l.startsWith("#")) {
      count = count + 1
      val fields = l.split(" ")
      val vid = fields(0).toInt
      val adj = fields.slice(1, fields.length).map(_.toInt).toArray
      adjMap.put(vid, IntArrayList.wrap(adj))
      if (count % 100000 == 0)
        System.err.print(count + "\r")
    }
    System.err.println()
    adjMap
  }

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val vidMappingFile = args(2)
    val adjMap = readAdjFile(input)
    System.err.println("Read adj done!")
    val originalVids = adjMap.keySet().toIntArray
    val oldVid2NewVidMap = new Int2IntOpenHashMap()
    var newVid = 1 // metis starts with 1
    for (oldVid <- originalVids) {
      oldVid2NewVidMap.put(oldVid, newVid)
      newVid += 1
      if (newVid % 10000 == 0)  {
        System.err.print(s"$newVid / ${originalVids.length}...\r")
      }
    }
    originalVids.par.foreach(oldVid => {
      val adjList = adjMap.get(oldVid)
      var i = 0
      while (i < adjList.size()) {
        val oldNvid = adjList.getInt(i)
        val newNvid = oldVid2NewVidMap.get(oldNvid)
        adjList.set(i, newNvid)
        i += 1
      }
    })
    System.err.println("Update adj with new vids done!")

    import java.io.File
    val writer = new PrintWriter(new File(output))
    val vidMappingWriter = new PrintWriter(new File(vidMappingFile))
    val numVertices = adjMap.size()
    val numEdges = adjMap.asScala.map(_._2.size()).sum / 2
    writer.println(s"$numVertices $numEdges 10") // with vertex weight
    System.err.println(s"V=${numVertices}, E=${numEdges}")
    var count = 0
    for(oldVid <- originalVids) {
      val newAdj = adjMap.get(oldVid)
      val weight = newAdj.size()
      writer.println(s"${weight} ${newAdj.toIntArray.mkString(" ")}")
      vidMappingWriter.println(s"${oldVid} ${oldVid2NewVidMap.get(oldVid)}")
      if (count % 10000 == 0) {
        System.err.print(s"${count}/${originalVids.length}...\r")
      }
      count += 1
    }
    writer.close()
    vidMappingWriter.close()
    System.err.println("Done!")
  }
}
