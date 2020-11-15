package cn.edu.nju.pasalab.graph.util

import java.io.{DataInputStream, DataOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object HDFSUtils {

  def hdfs() = {
    FileSystem.get(new Configuration())
  }

  def deleteFile(file:String):Unit = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(file), true)
  }

  def createFile(file:String):DataOutputStream = {
    hdfs.create(new Path(file), true)
  }

  /**
    * Read files from HDFS.
    * @param files If it is a directory, then read all the files under the directory, else just open the file.
    * @return The file input streams
    */
  def readFiles(files:String):Seq[DataInputStream] = {
    val fs = hdfs()
    val path = new Path(files)
    if (fs.isDirectory(path)) {
      fs.listStatus(path).map(fileStatus => fs.open(fileStatus.getPath)).toSeq
    } else {
      Seq(fs.open(path))
    }
  }

  def mergeToFile(srcDir:String, dstFile:String) = {
    FileUtil.copyMerge(hdfs(), new Path(srcDir),
      hdfs(), new Path(dstFile),
      true, new Configuration(), null)
  }

}
