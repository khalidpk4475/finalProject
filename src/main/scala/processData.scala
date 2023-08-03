

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


  object  processData extends App{

    Logger.getLogger("org").setLevel(Level.ERROR)


    //val host = "local[1]"
    val host = "yarn"
    val spark: SparkSession = SparkSession.builder()
      .master(host)
      .appName("AmazonProductReviewEDA")
      .enableHiveSupport()
      .getOrCreate()



    val data = spark.sql("SELECT * FROM " + args(0))





    var filePath = args(1)

    //val dfOct = spark.read.option("header", "true").csv("C:\\Users\\khali\\OneDrive\\Desktop\\Data_Sets\\Ecommer_Datasets\\ECom Dataset\\2019-Oct.csv")
    //val dfUser = spark.read.option("header", "true").csv("C:\\Users\\khali\\OneDrive\\Desktop\\Data_Sets\\Ecommer_Datasets\\ECom Dataset\\2019-Nov.csv")
    val dfUser = spark.sql("SELECT * FROM user_data_muh01")


    val dfCount = dfUser.count()
    println(s"Row count: $dfCount")

    //CleanDF.show()
    //######################################################
    //******************Data Cleaning**********************
    // Removing Duplicate Values
    val filteredDF = dfUser.dropDuplicates().coalesce(1)

    //# converting event time to date and hour only
    val modifiedDFDate = filteredDF.withColumn("event_date", to_date(col("event_time")).cast("date"))
    val CleanDF1 = modifiedDFDate.withColumn("event_hour", substring(col("event_time")
      .cast("string"), 12, 2).cast("string"))
      .where("event_type != 'remove_from_cart'")
    //val CleanDF = modifiedDF.na.drop("event_time")

    val CleanDF = CleanDF1.na.fill("unknown", Seq("brand"))

    //*********Analyzing Customer Behavior**************
    // Calculating Total Number of visits(purchased, view, add cart)
    val visitors = CleanDF.groupBy("event_type")
      .agg(count("user_id").alias("count_of_users"))
      .orderBy(col("count_of_users").desc)
      .select(col("event_type"), col("count_of_users"))


    // Saving the Result in csv File
    visitors.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save((filePath + "/total_visitors"))
    visitors.show()


    //#####################################################
    val modifiedActivity = CleanDF.groupBy(col("event_date"), col("event_hour"))
      .agg(count(col("user_id")).alias("count"))
      .orderBy(col("event_date"), col("event_hour"))
    val modifiedActivityAddTime = CleanDF.withColumn("time", date_format(to_timestamp(col("event_hour"), "H"), "hh aa"))
    val modifiedActivityAddWeek = modifiedActivityAddTime.withColumn("week_day", date_format(col("event_date"), "EEEE"))
      .withColumn("day", dayofweek(col("event_date")))
    val activity = modifiedActivityAddWeek


    //Calculating Total Users By Each Date of the Month
    val monthActivity = modifiedActivity.groupBy(col("event_date"))
      .agg(sum(col("count")).alias("sum"))
      .orderBy(col("event_date"))
    // Saving the Result in csv File
    monthActivity.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save((filePath + "/visitors_day"))


    monthActivity.show(40)

    // Calculating Total Users by Each Day of the Week
    val weekActivity = activity.groupBy(col("week_day"), col("day"))
      .agg(round(count(col("user_id")) / 7).cast(IntegerType).alias("total_user_by_day"))
      .orderBy(col("day"))
      .select(col("week_day"), col("day"), col("total_user_by_day"))

    // Saving the Result in csv File
    weekActivity.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save((filePath + "/visitors_week"))
    weekActivity.show()

    // Calculating Total Users By Each Hour of the Day
    val timeActivity = activity.groupBy(col("event_hour"), col("time"))
      .agg(round(count(col("user_id")) / 24).cast(IntegerType).alias("total_users_by_hour"))
      .orderBy("event_hour")
      .select(col("event_hour"), col("time"), col("total_users_by_hour"))


    // Saving the Result in csv File
    timeActivity.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save((filePath + "/visitors_hour"))
    timeActivity.show()
    //#####################################################


    // Calculate revenue by grouping on 'event_date' and 'hour' and add 'revenu', 'time', 'week_day' and 'day' column
    val purchaseDf = modifiedActivityAddWeek.filter(col("event_type") === "purchase")
    /* val revenueDf = purchaseDf.groupBy("event_date", "event_hour")
       .agg(sum("price").alias("revenue"))
       .select("event_date", "event_hour", "revenue")
       .withColumn("revenue", round(col("revenue"), 2))
       .withColumn("time", date_format(to_timestamp(col("event_hour"), "H"), "hh aa"))
       .withColumn("week_day", date_format(col("event_date"), "EEEE"))
       .withColumn("day", date_format(col("event_date"), "u")
         .cast("int")).orderBy("event_date", "event_hour")*/

    // Group by 'event_date', calculate the sum of 'price', and round the result to 2 decimal places
    val revMonthDF = purchaseDf.groupBy("event_date")
      .agg(round(sum("price"), 2).alias("sum_price")).orderBy("event_date")

    revMonthDF.show()
    // Avg Purchase every day
    val purMonthDF = purchaseDf.groupBy("event_date")
      .agg(round(count("user_id")).alias("purchase_count")).orderBy("event_date")
    // Saving the Result in csv File
    purMonthDF.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save((filePath + "/purchase_day"))
    purMonthDF.show()


    // top  10 brand by revenu and purchase
    val topTenBrandDf = purchaseDf.groupBy("brand")
      .agg(round(sum("price"), 2).alias("price"))
      .orderBy(col("price").desc)
    // Saving the Result in csv File
    val topTenDf = topTenBrandDf.limit(10)
    topTenDf.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save((filePath + "/topten_brand_price"))
    topTenDf.show()


    val topTenPurchaseCountDf = purchaseDf.groupBy("brand")
      .agg(round(count("user_id")).alias("purchase_count"))
      .orderBy(col("purchase_count").desc).limit(10)
    // Saving the Result in csv File
    topTenPurchaseCountDf.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save((filePath + "/topten_brand_count"))
    topTenPurchaseCountDf.show()


    // top 10 most viewed brand
    val viewedDf = modifiedActivityAddWeek.filter(col("event_type") === "cart")

    val topTenViewedBrandDf = viewedDf.groupBy("brand")
      .agg(round(count("user_id")).alias("view_count"))
      .orderBy(col("view_count").desc)
    val topViewedBrandDf = topTenViewedBrandDf.limit(10)
    // Saving the Result in csv File
    topViewedBrandDf.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save((filePath) + "/topten_brand_viewed")


    topViewedBrandDf.show()

  }

