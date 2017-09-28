package org.leggMason.rawToProcessed

import org.apache.spark.sql._
import org.leggMason.helper.{ CommandLineArgs, ObjectSchemaCreator }
import org.api.client.{ CallAPIGateway, CallAPIJobParameter, CallAPIJobBusinessDate, CallAPIJobControl }
import java.io._
import java.lang.NullPointerException
import scala.util.control.NonFatal
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by himanshu on 8/5/2017.
 */
object ETLDriver {

  def main(args: Array[String]): Unit = {

     var job_id=""
     var run_date=""
     
     try {
      //Verify command line arguments
      val commandLineArgs = new CommandLineArgs(args)

      //Read Source JSON File and convert it into an object
      val objectSchema = ObjectSchemaCreator.getObjectSchema(commandLineArgs.getMetaFileLocation)

      //get Business date from API
      job_id = objectSchema.getJobId
      run_date = CallAPIJobBusinessDate.getBusinessDate(new java.lang.StringBuilder("?job_id=").append(job_id))
      
      
      if (CallAPIGateway.callPreExecutionSteps(job_id, run_date)) {

        //Get Params from API
        val paramValues = CallAPIJobParameter.parseJsonParams(new java.lang.StringBuilder("?job_id=").append(job_id).append("&run_date=").append(run_date))
        val outputPath = paramValues.get(0)
        val inputpath = paramValues.get(1)
        

        println("outputPath :" + outputPath)
        println("inputpath :" + inputpath)

        //Get Spark Session
        val spark = SparkSession.builder().appName("RawToProcessedETL").master("local").getOrCreate()

        //Create instance of ETL
        val rawToProcessedETL = new RawToProcessedETL(spark, objectSchema)

        //Run the ETL Process
        rawToProcessedETL.run(outputPath, inputpath)

        //update job status to COMPLETED in control table
        System.out.println(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()));

        if (CallAPIGateway.callPostExecutionSteps(new java.lang.StringBuilder("?end_date_time=").append(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date())).append("&job_id=").append(job_id).append("&run_date=").append(run_date).append("&stage=COMPLETED")))
          println("Status updated to COMPLETED for JobId: " + objectSchema.getJobId);
        else
          println("status could not be updated for JobId: " + objectSchema.getJobId);
      } else {
        println("pre-execution steps not completed...can not continue..")
        //update job status to FAILED in control table
        CallAPIJobControl.updateJobStatus(new java.lang.StringBuilder("?job_id=").append(job_id).append("&run_date=").append(run_date).append("&stage=FAILED"))
     		
      }
    } catch {
      
      case NonFatal(ex) =>
        ex match {
          
          case ex: FileNotFoundException => {
            println("Couldn't find required file.")
            println(ex.printStackTrace())
            //update job status to FAILED in control table
        CallAPIJobControl.updateJobStatus(new java.lang.StringBuilder("?job_id=").append(job_id).append("&run_date=").append(run_date).append("&stage=FAILED"))
          }
          
          case ex: IOException => {
            println("Had an IOException trying to read file")
            println(ex.printStackTrace())
            //update job status to FAILED in control table
        CallAPIJobControl.updateJobStatus(new java.lang.StringBuilder("?job_id=").append(job_id).append("&run_date=").append(run_date).append("&stage=FAILED"))
          }

          case ex: NullPointerException => {
            println("NullPointerException...")
            println(ex.printStackTrace())
            //update job status to FAILED in control table
        CallAPIJobControl.updateJobStatus(new java.lang.StringBuilder("?job_id=").append(job_id).append("&run_date=").append(run_date).append("&stage=FAILED"))
          }

          // handling any other exception that might come up
          case unknown: Exception => {
            println("Got this unknown exception: " + unknown)
            println(unknown.printStackTrace())
            //update job status to FAILED in control table
        CallAPIJobControl.updateJobStatus(new java.lang.StringBuilder("?job_id=").append(job_id).append("&run_date=").append(run_date).append("&stage=FAILED"))
          }
        }

    }

  }

}
