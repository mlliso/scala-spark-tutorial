package com.sparkTutorial.pairRdd.groupbykey

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByCountryProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,
       output the the list of the names of the airports located in each country.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       "Canada", List("Bagotville", "Montreal", "Coronation", ...)
       "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
       "Papua New Guinea",  List("Goroka", "Madang", ...)
       ...
     */

    val conf = new SparkConf().setAppName("avgHousePrice").setMaster("local[3]")
    val sc = new SparkContext(conf)


    val airportsByCountry = sc.textFile("in/airports.text")
      .map(l => l.split(Utils.COMMA_DELIMITER))
      .map(a => (a(3), a(1)))
      .groupByKey()
      .sortByKey()

    for ((country, airports) <- airportsByCountry.collect()) println(s"${country}, " + airports.toList)


  }
}
