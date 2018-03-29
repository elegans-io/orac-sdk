package io.elegans.oracsdk.store

import org.apache.spark.rdd.RDD

object SaveToCsv  extends java.io.Serializable {

  def saveCoOccurrenceInput(input: RDD[(String, String, Double)], outputFolder: String): Unit = {
    input.map(item => item._1 + "," + item._2 + "," + item._3).saveAsTextFile(outputFolder)
  }

}



