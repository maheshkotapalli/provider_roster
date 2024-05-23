# Provider Visits

This is a Spark project used to generate reports that deliver data about providers.

## Requirements
* Language: Scala
* Framework: Spark
* Output: json file/files 

## Steps to execute
* Import the code to any IDE
* Build the project and jar
* run the jar by passing input folder (folder which has all the provider and visits file) and output folder (where we neeed Jsons to be dropped)
* Alternatively test case can be run by providing input and output folders.

## Points to pondor
* logger has been enabled to display on console for better visibility (rolling appenders to file can also be setup)
* This code was executed in windows machine using winutils
* Constants object is created to host application level constants 
* Util object is created to host most commonly used methods
* SparkApplication object is created to create SparkSession with required configurations.

