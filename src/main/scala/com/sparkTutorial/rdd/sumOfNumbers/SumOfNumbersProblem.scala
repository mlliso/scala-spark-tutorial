package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    println("context")
    val conf = new SparkConf().setAppName("UnionLogProblem").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val primeNums = sc.textFile("in/prime_nums.text")
      .flatMap(
        line => line.split("\\s+")
          .filter(str => !str.isEmpty)
      )
      .map(num => num.toInt)
      .take(100)

    primeNums.foreach(num => println(s"num: $num"))

    val sum = primeNums.reduce((x, y) => x + y);

    println(s"Sum of prime numbers: $sum")
  }
}
