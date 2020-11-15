package cn.edu.nju.pasalab.graph.util

import scala.collection.mutable

/**
  * This is a helper class of file-based broadcast.
  * The data are not broadcasted via the standard Spark's broadcast mechanism which needs to serialize
  * the data and cannot handle data more than 2Gbytes.
  * Instead, we store the data in a file on HDFS and load data when they are used for the first time.
  */
object ProcessLevelSingletonManager {

  // Use a map to store the fetched data. The key is the index of the data.
  private val dataStore = new mutable.HashMap[Int, Any]()

  /**
    * Fetch the broadcast data
    * @param index the index of the data
    * @param loader if the data are not loaded before, load the data with the loader
    * @return the fetched data
    */
  def fetch(index:Int, loader:(Int) => Object):Any = {
    var data = dataStore.get(index)
    if (data.isEmpty) {
      // we need to fetch it
      this.synchronized {
        data = dataStore.get(index)
        // check again
        if (data.isEmpty) {
          // the data is missing, we load it use the loader
          val loadedData = loader(index)
          dataStore.put(index, loadedData)
          data = Some(loadedData)
        }
      }
    }
    data.get // get the data
  }
}
