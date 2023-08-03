
import java.util.Properties
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.TypesafeMap.Key
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.types.IntegerType
//import org.itc.com.analyzeData
//import org.itc.com.sql_Demo.{members_df, spark, sqlQuery}

import scala.collection.immutable.List
import scala.util.Try


object dataTransfer extends App {
  //System.setProperty("hadoop.home.dir", "C:\\hadoop")

  Logger.getLogger("org").setLevel(Level.ERROR)
  //val host = "local[1]"
  val host = "yarn"
  val spark: SparkSession = SparkSession.builder()
    .master(host)
    .appName("AmazonProductReviewEDA")
    .enableHiveSupport()
    .getOrCreate()


  //val dfOct = spark.read.option("header", "true").csv("C:\\Users\\khali\\OneDrive\\Desktop\\Data_Sets\\Ecommer_Datasets\\ECom Dataset\\2019-Oct.csv")
  //val dfNov = spark.read.option("header", "true").csv("C:\\Users\\khali\\OneDrive\\Desktop\\Data_Sets\\Ecommer_Datasets\\ECom Dataset\\2019-Nov.csv")
  //val dfUser = spark.read.option("header", "true").csv("C:\\data\\user_data\\user_data.csv")
  val dfUser = spark.read.option("header", "true").csv(args(0))


  val dfCount = dfUser.count()
  println(s"Row count: $dfCount")

  dfUser.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable(args(1))
  println("completed......")
}


