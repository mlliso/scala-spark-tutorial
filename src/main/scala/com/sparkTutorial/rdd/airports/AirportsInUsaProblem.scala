package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */
    println("context")
    val conf = new SparkConf().setAppName("UsaAirports").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("in/airports.text")

    val usAirports = airports.map(airport => airport.split(Utils.COMMA_DELIMITER))
      .filter(airport => "\"United States\"".equalsIgnoreCase(airport(3)))
      .map(airport => s"${airport(1)}, ${airport(2)}")

    usAirports.foreach(println(_))

    usAirports.saveAsTextFile("out/airports_in_usa.text")
  }
}
