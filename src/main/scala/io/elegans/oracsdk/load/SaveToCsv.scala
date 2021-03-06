package io.elegans.oracsdk.load

import org.apache.spark.rdd.RDD

object SaveToCsv  extends java.io.Serializable {

  def saveCoOccurrenceInput(input: RDD[(Long, Long, Double)], outputFolder: String): Unit = {
    input.map(item => item._1 + "," + item._2 + "," + item._3).saveAsTextFile(outputFolder)
  }

  def saveStringToLongMapping(input: RDD[(String, Long)], outputFolder: String): Unit = {
    input.map(item => item._1 + "," + item._2).saveAsTextFile(outputFolder)
  }

}



