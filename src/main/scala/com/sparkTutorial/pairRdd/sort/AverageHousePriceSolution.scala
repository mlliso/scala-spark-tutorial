package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceSolution {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("AverageHousePriceProblem").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val priceByNumberOfBedrooms = sc.textFile("in/RealEstate.csv")
      .map(line => line.split(",", 0))
      .filter(row => row(0) != "MLS")
      .map(row => (row(3), (1, row(2).toDouble)))
      .reduceByKey((a, b) =>
        (
          a._1 + b._1,
          a._2 + b._2
        )
      ).map(tuple => (tuple._1, tuple._2._2 / tuple._2._1))


    val sortedHousePriceAvg = priceByNumberOfBedrooms.sortBy(e => e._1.toInt)

    println("sortedHousePriceTotal: ")
    for ((bedrooms, avgPrice) <- sortedHousePriceAvg.collect()) println(bedrooms + " : " + avgPrice)
  }

}
