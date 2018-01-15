package com.sparkTutorial.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, _}

object WordCount {

  def main(args: Array[String]) {
    println("context")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    println("splitting")

    val lines = sc.textFile("in/word_count.text")
    val words = lines.flatMap(line => line.split(" "))

//    println("counting")
//    val wordCounts = words.countByValue()
//    for ((word, count) <- wordCounts) println(word + " : " + count)

    println("filtering and counting")
    words.map(word => word.toUpperCase)
      .filter(word => word.contains("IN"))
      .countByValue()
      .foreach(p => println(s"${p._1} : ${p._2}"))
  }
}
