package com.katchstyle.nlytics


import org.apache.spark.{SparkConf, SparkContext}


object SparkContextInitializer  {

def createSparkContext(name: String) = {
  val sc: SparkConf = basicSparkConf(name)
  val spark = new SparkContext(sc)
  spark.setLogLevel("WARN")
  spark
}


def basicSparkConf(name: String): SparkConf = {
  new SparkConf()
    .setAppName(name)
    .set("spark.io.compression.codec", "lzf")
    .set("spark.speculation", "true")
}

}
