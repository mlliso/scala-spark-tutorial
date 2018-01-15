package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    println("context")
    val conf = new SparkConf().setAppName("UsaAirports").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("in/airports.text")

    val usAirports = airports.map(airport => airport.split(Utils.COMMA_DELIMITER))
      .filter(airport => 40.0 < airport(6).toDouble)
      .map(airport => s"${airport(1)}, ${airport(2)}, ${airport(6)}")

    usAirports.foreach(println(_))

//    usAirports.saveAsTextFile("out/airports_by_lat.text")
  }
}
