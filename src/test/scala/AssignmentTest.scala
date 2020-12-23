package example

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


class AssignmentTest extends FlatSpec{
  
   val spark: SparkSession = SparkSession.builder().config("spark.master", "local").enableHiveSupport().getOrCreate()
   import spark.implicits._

   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)

 
  "getFlightDf" should "match" in  {
    val filepath = "/home/anish/workspace/quantexaassignment/resources/flightDataTest.csv"
    
    val testDf = Assignment.getFlightDf(filepath)
    
    assert(testDf.isInstanceOf[DataFrame]) 
  }
   
  "getPassengerDf" should "match" in {
    val filepath = "/home/anish/workspace/quantexaassignment/resources/passengersTest.csv"
    
    val testDf = Assignment.getPassengerDf(filepath)
    
    assert(testDf.isInstanceOf[DataFrame]) 
  }
  
  "getFlightsPerMonth" should "match" in {
    
    val inputDf = List(
      (1, "2020-01-09"),  
      (2, "2020-02-08"),
      (3, "2020-03-07"),
      (4, "2020-12-06"),
      (5, "2020-01-05"),
      (6, "2020-01-04"),
      (7, "2020-02-01"),
      (8, "2020-03-01"),
    ).toDF("flightId", "date")
    
    val expected = List(
      ("01", 3),
      ("02", 2),
      ("03", 2),
      ("12", 1)
    ).toDF("x", "y")
    
    val testDf = Assignment.getFlightsPerMonth(inputDf)
    assert(testDf.collect().toList == expected.collect().toList)
    
  }
  
  "getFrequentFlyers" should "match" in {
    
    val inputPassengers = List(
    ("1", "name1", "surname1"),
    ("2", "name2", "surname2"),
    ("3", "name3", "surname3"),
    ("4", "name4", "surname4")
    ).toDF("passengerId", "firstName", "lastName")
    
    val inputFlights = List(
        ("1", "1"),  ("1", "1"),  ("1", "1"),  ("1","1"),  ("1","1"),
         ("2","2"),  ("2","2"),  ("3","3"),  ("4","4")
    ).toDF("flightId", "passengerId")
    
    val expected = 
      List( 
          ("1", 5, "name1", "surname1"), 
          ("2", 2, "name2", "surname2"), 
          ("3", 1, "name3", "surname3"), 
          ("4", 1, "name4", "surname4") 
          ).toDF("w","x","y","z")
    
    val testDf = Assignment.getFrequentFlyers(inputFlights, inputPassengers)
    
    assert(testDf.collect().toList == expected.collect().toList)
    
  }
  
  "getLongestRun" should "match" in {
    val inputDf = List(
      ("1", "1", "uk", "nn", "2020-01-01"),
      ("1", "2", "nn", "ff", "2020-01-02"),
      ("1", "3", "ff", "ll", "2020-01-03"),
      ("1", "4", "ll", "uk", "2020-01-04"),
      ("2", "5", "nn", "uk", "2020-01-05"),
      ("2", "6", "uk", "nn", "2020-01-06"),
      ("3", "7", "uk", "ff", "2020-01-07"),
      ("3", "8", "ff", "ll", "2020-01-08"),
      ("3", "7", "ll", "zz", "2020-01-09"),
      ("3", "7", "zz", "xx", "2020-01-10"),
      ("3", "7", "xx", "uk", "2020-01-11"),
    ).toDF("passengerId", "flightId", "from", "to", "date")
    
    val expected = List(
      ("3", 4),
      ("1", 3)
    ).toDF("x","y")
    
    val testDf = Assignment.getLongestRunOfFlights(inputDf, "uk")
    testDf.show()
    
    //get task not serializable runtime error, but dataframes match
    assert(testDf.collect().toList == expected.collect().toList)
  }
  
  
  "getCommonFlights" should "match" in {
    val flights = List(
      ("1", "1"), 
      ("1", "2"), 
      ("1", "3"),
      ("2", "2"), 
      ("2", "3"), 
      ("2", "4"), 
      ("3", "4"),
      ("3", "5"),
    ).toDF("passengerId","flightId")
    
    val expected = List(
      ("1", "2", 2),
      ("2", "1", 2),
      ("2", "3", 1),
      ("3", "2", 1)
    ).toDF("x","y","z")
    val testDf = Assignment.getCommonFlights(flights, 1)
    
    
    
    assert(testDf.collect().toList == expected.collect().toList)
    
  }
}