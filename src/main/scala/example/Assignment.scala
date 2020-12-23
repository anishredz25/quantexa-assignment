package example

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.Window

object Assignment extends App{
    
   //remove unnecessary logs  
   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)
  
   //file paths for the csv files
   val passengerFilePath = "./resources/passengers.csv"
   val flightFilePath = "./resources/flightData.csv"
   
   
     
    
    //get dataframes
    val passengerDf = getPassengerDf(passengerFilePath)               
    val flightDf = getFlightDf(flightFilePath)
   
     
    //question 1 - flights per month
    val question1 = getFlightsPerMonth(flightDf)
    question1.show(false)
    
    //question 2 - most frequent flyers
    val question2 = getFrequentFlyers(flightDf, passengerDf, 100)
    question2.show(false)
    
    //question 3 - longest number of consecutive flights not going through uk
    val question3 = getLongestRunOfFlights(flightDf, "uk")
    question3.show(false)
    
    //question 4 - passengers who shared flights the most
    val question4 = getCommonFlights(flightDf, 3)
    question4.show(false)
  
  
    
  //get passenger dataframe from csv
  def getPassengerDf(passengerFilePath: String): DataFrame = {
       //setup spark session
     val spark = SparkSession.builder()
        .master("local")
        .appName("Flight Data Reader")
        .enableHiveSupport()
        .getOrCreate()
        
     val passengerDf = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(passengerFilePath)

     return passengerDf                        
  }
  
  //get flight dataframe from csv
  def getFlightDf(flightFilePath: String): DataFrame = {
       //setup spark session
       val spark = SparkSession.builder()
          .master("local")
          .appName("Flight Data Reader")
          .enableHiveSupport()
          .getOrCreate()
       
       val df = spark.read
      .option("header", true)
      .option("delimeter", ",")
      .option("inferSchema", "true")
      .csv(flightFilePath)
      
      //get month
      val flightDf = df
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
      
      return flightDf
  }
  
  //question 1
  def getFlightsPerMonth(flightDf: DataFrame): DataFrame = {
    val flightsPerMonth = flightDf
      .withColumn("month", date_format(col("date"), "MM"))
      .select("flightId", "month")
      .groupBy("month")
      .agg(count("flightId").alias("Number of Flights"))
      .orderBy(asc("month"))
    
    return flightsPerMonth
      
  }
  
  //question 2
  def getFrequentFlyers(flightDf: DataFrame, passengerDf: DataFrame,  topN: Int=100): DataFrame = {
     
     val flightsPerPassenger = flightDf
      .select("passengerId", "flightId")
      .groupBy("passengerId")
      .agg(count("flightId").alias("Number of Flights"))
      .orderBy(desc("Number of Flights"))
     
     //join the two dataframes
     val topFlyers = flightsPerPassenger
      .limit(topN)
      .join(passengerDf, "passengerId")
      .orderBy(desc("Number of Flights"))
      
     return topFlyers
  }
  
  //user defined function to act on dataframe
  def getVisitedCountries(flag : Int, currPos : Int, nextPos : Int): Int = {
    
    //if flying from uk - flag = 1, 
    //position of next cell - current position = number of consecutive flights  
    if(flag == 1)
    {
      if(nextPos > 0)
      {
        return nextPos - currPos
      }
      else
      {
        return 0 
      }
    }
    else
    {
      return 0
    }
  }
  
  //question 3
  def getLongestRunOfFlights(flightDf: DataFrame, countryCode : String = "uk"): DataFrame = {
    
   //setup spark session
   val spark = SparkSession.builder()
      .master("local")
      .appName("Flight Data Reader")
      .enableHiveSupport()
      .getOrCreate()
    
    //set flags 1 when flying from, -1 when flying to specified country (UK)
    val flagsSetDf = flightDf.withColumn("flag", 
         when(lower(col("from")) === countryCode, 1)
         .when(lower(col("to")) === countryCode, -1)
            .otherwise(0)
         ).orderBy(asc("passengerId"), asc("date"))
      
      
    flagsSetDf.createOrReplaceTempView("flagsSetDf")
         
       
    val q1 = """ 
           SELECT f.*, ROW_NUMBER() OVER (partition BY passengerId ORDER BY passengerId, date) AS pos 
           FROM flagsSetDf f
           ORDER BY passengerId, date;  
           """
    //create sequence of flights for each passenger
    val positionSetDf = spark.sql(q1)
    positionSetDf.createOrReplaceTempView("positionSetDf")
      
    val q2 = """ SELECT passengerId, flag, pos FROM positionSetDf WHERE flag != 0 """
    val extractedRowsDf = spark.sql(q2)
    
    val window = Window.partitionBy("passengerId").orderBy("pos")
    
    //create user defined function  
    val calcVisited = udf[Int, Int, Int, Int](getVisitedCountries)
    
    //apply user defined function
    val result = extractedRowsDf.withColumn("countries visited", calcVisited(
          col("flag"), col("pos"), lead(col("pos"), 1).over(window)))           
          .na.fill(0)
          .select("passengerId", "countries visited").where(col("countries visited").gt(0))
          .orderBy(desc("countries visited"))
      
    result.show()
    return result
  }  
  
  //question 4
  def getCommonFlights(flightDf: DataFrame, minSharedFlights: Int = 3): DataFrame = {
      //self join on flight id
      val selfjoinDf = flightDf.as("flight1").join(flightDf.as("flight2"),
          col("flight1.flightId") === col("flight2.flightId"), "inner")
          .select(col("flight1.passengerId"), col("flight2.passengerId").as("passengerId2"), col("flight1.flightId")) 
      
      //order by shared flights
      val travelledTogetherDf = selfjoinDf.groupBy("passengerId", "passengerId2")
                .agg(count("flight1.flightId").alias("number of shared flights"))
                .filter(col("number of shared flights") >= minSharedFlights && col("passengerId").notEqual(col("passengerId2")))
                .orderBy(desc("number of shared flights"), asc("passengerId"))
                
      return travelledTogetherDf
  } 
}