package edu.vanderbilt.accre.nyctaxi

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import java.io._
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/*
  * Created by joshuaarnold on 5/31/17.
  */

object TaxiApp {

  /**
    * Cluster a DataFrame
    *
    * @param df
    */
  def cluster(df: DataFrame): Unit = {

    val assembler = new VectorAssembler()
        .setInputCols(Array("pickup_longitude", "pickup_latitude"))
        .setOutputCol("features")

    val bkm = new BisectingKMeans()
        .setK(10)
        .setSeed(1)
        .setFeaturesCol("features")
        .setPredictionCol("prediction")

    val pipeline = new Pipeline()
        .setStages(Array(assembler, bkm))

    val model = pipeline.fit(df)

    val bkmModel = model.stages(1).asInstanceOf[BisectingKMeansModel]

    val dfNew: DataFrame =
      model.transform(df.sample(withReplacement = false, 0.01))
          .persist(StorageLevel.MEMORY_AND_DISK)

    dfNew.show()

    dfNew.select("prediction", "pickup_longitude", "pickup_latitude")
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save("sample_pickups")

    val cost: Double = bkmModel.computeCost(dfNew)
    println(s"Within Set Sum of Squared Errors = $cost")

    val centers = bkmModel.clusterCenters

    // FileWriter
    val file = new File("cluster_centers.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("center,pickup_longitude,pickup_latitude\n")
    for {
      line <- centers.zipWithIndex.map {
        case (f, i: Int) => i.toString + "," +
            f(0).toString + "," + f(1).toString() + "\n"
      }
    } bw.write(line)
    bw.close()
  }

  val customSchema = StructType(Array(
    StructField("vendor_id",          StringType,  nullable = false),
    StructField("pickup_datetime",    StringType,  nullable = false),
    StructField("dropoff_datetime",   StringType,  nullable = false),
    StructField("passenger_count",    LongType,    nullable = false),
    StructField("trip_distance",      DoubleType,  nullable = false),
    StructField("pickup_longitude",   DoubleType,  nullable = false),
    StructField("pickup_latitude",    DoubleType,  nullable = false),
    StructField("rate_code",          IntegerType, nullable = false),
    StructField("store_and_fwd_flag", StringType,  nullable = false),
    StructField("dropoff_longitude",  DoubleType,  nullable = false),
    StructField("dropoff_latitude",   DoubleType,  nullable = false),
    StructField("payment_type",       StringType,  nullable = false),
    StructField("fare_amount",        DoubleType,  nullable = false),
    StructField("surcharge",          DoubleType,  nullable = false),
    StructField("mta_tax",            DoubleType,  nullable = false),
    StructField("tip_amount",         DoubleType,  nullable = false),
    StructField("tolls_amount",       DoubleType,  nullable = false),
    StructField("total_amount",       DoubleType,  nullable = false)
  ))

  final case class Trip(vendor_id: String,
                        pickup_datetime: String,
                        dropoff_datetime: String,
                        passenger_count: Long,
                        trip_distance: Double,
                        pickup_longitude: Double,
                        pickup_latitude: Double,
                        rate_code: Integer,
                        store_and_fwd_flag: String,
                        dropoff_longitude: Double,
                        dropoff_latitude: Double,
                        payment_type: String,
                        fare_amount: Double,
                        surcharge: Double,
                        mta_tax: Double,
                        tip_amount: Double,
                        tolls_amount: Double,
                        total_amount: Double
                       )

  def customRound(d: Double, p: Int): Double =
    // 10m = 0.00009 degrees latitude
    // 10m = 0.00012 degrees longitude
    scala.math.BigDecimal(d)
        .setScale(p, BigDecimal.RoundingMode.HALF_UP).toDouble

  final case class Point(lng: Double, lat: Double) {
    def round = Point(customRound(lng, 4), customRound(lat, 4))
  }


  def main(args: Array[String]): Unit = {

    val inputPaths: Array[String] = args.length match {
      case 0 => throw new IllegalArgumentException(
        "Usage: TaxiApp inputPath*")
      case _ => args
    }

    /*
    val conf = new SparkConf()

    val sc = new SparkContext(conf)
     */

    val spark = SparkSession
        .builder()
        .appName("TaxiApp")
        .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    // import spark.implicits._
    val df: DataFrame = spark.read
        .option("header", true)
        .option("treatEmptyValuesAsNulls", true)
        .schema(customSchema)
        .csv(inputPaths: _*)

    df.show()

    val columns = Array("pickup_longitude", "pickup_latitude",
      "dropoff_longitude", "dropoff_latitude")

    val pointsRDD: RDD[Row] =
      df.select(columns.head, columns.tail: _*).rdd
          .filter {
            case Row(0.0, _, _, _) => false
            case Row(_, 0.0, _, _) => false
            case Row(_, _, 0.0, _) => false
            case Row(_, _, _, 0.0) => false
            case Row(_: Double, _: Double, _: Double, _: Double) =>
              true
            case _ => false
          }
          .map {
            case Row(pLng: Double, pLat: Double, dLng: Double, dLat: Double)
          => Row(Point(pLng, pLat).round, Point(dLng, dLat).round)
          }

    // Give each point a unique id
    val indexed: RDD[(Point, Long)] =
      pointsRDD.map{ case Row(a: Point, b: Point) => Seq(a.round, b.round) }
          .flatMap(p => p).distinct.zipWithUniqueId
          .persist(StorageLevel.MEMORY_AND_DISK)

    // Join the pickup with its index
    val pickups = pointsRDD.keyBy[Point]{
      case Row(a: Point, b: Point) => a.round }.join(indexed)

    // Construct the edges
    // Join the dropoff with it's index
    val edges: RDD[Edge[String]] = pickups.map {
      case (_, (Row(_: Point, p1: Point), v0: Long)) =>
        (p1.round, v0)
    } .join(indexed).values
        .map { case (v0: Long, v1: Long) => Edge(v0, v1, "foo") }

    // Reformat indexed to be vertices
    val vertices: RDD[(VertexId, Point)] = indexed.map({
      case (p: Point, v: Long) => (v, p)
    })

    // Specify default point
    val defaultPoint = Point(-1.0, -1.0)

    // Create the graph
    val graph = Graph(vertices, edges, defaultPoint)

    // Run PageRank
    val ranks = graph pageRank 0.0001 vertices

    ranks take 10 foreach println

    // Map to Row and sort the page ranks descending
    val sorted = ranks.join(vertices).map(
      { case (_: VertexId, (v: Double, p: Point)) => Row(p.lng, p.lat, v) }
    ).sortBy[Double](
      { case Row(_: Double, _: Double, rank: Double) => rank },
      ascending = false
    )


    // Convert RDD to DataFrame

    // Generate the schema based on the string of schema
    val pageRankSchema = StructType(Array(
      StructField("longitude",   DoubleType,  nullable = false),
      StructField("latitude",    DoubleType,  nullable = false),
      StructField("pagerank",    DoubleType,  nullable = false)
    ))

    val sortedDF = spark.createDataFrame(sorted, pageRankSchema)

    // Write the DataFrame to disk
    sortedDF.write
        .format("com.databricks.spark.csv")
        .option("header", true)
        .save("location_ranks")


  }
}
