
package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    // pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    pickupInfo = pickupInfo.where(s"x>= $minX and x<= $maxX and y>= $minY and y<= $maxY and z>= $minZ and z<= $maxZ")
    var data = pickupInfo.groupBy("x", "y", "z").agg(count("*").as("cnt"))
    data.createOrReplaceTempView("ST_cell")

    // Register Spark UDFs from HotcellUtils

    var NeighborCnt = udf((x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int)
    => HotcellUtils.getNeighbourCount(x, y, z, minX, maxX, minY, maxY, minZ, maxZ))
    // Compute sum(neighbor counts) with Spark SQL
    spark.udf.register("Weights", (x1:Double, y1:Double, z1:Double, x2:Double, y2:Double, z2:Double)=> HotcellUtils.getWeight(x1,y1,z1,x2,y2,z2))


    var GScore = udf((x: Int, y: Int, z: Int, Wt: Int, neighCnt: Int, Ncell: Int, X_bar: Double, S: Double)
    => HotcellUtils.GettisOrd(x, y, z, Wt, neighCnt, Ncell,X_bar, S))

    // Mean and Standard Deviation
    val X_bar = data.agg(sum("cnt")/numCells).first().getDouble(0)
    // splitting S to avoid ambiguous reference errors

    val squareSum = spark.sql("SELECT SUM(POW(cnt, 2)) FROM ST_cell")

    val S = math.sqrt(squareSum.first().getDouble(0) / numCells - math.pow(X_bar, 2.0))

    // gettingWeights, Ncells

    var Wdf = spark.sql("SELECT c1.x AS x, c1.y AS y, c1.z AS z, sum(c2.cnt) AS NeighCount "
      + "FROM ST_cell AS c1, ST_cell AS c2 "
      + "WHERE Weights(c1.x,c1.y,c1.z,c2.x,c2.y,c2.z) "
      + "GROUP BY c1.x, c1.y, c1.z")


    var NeighborDf = Wdf.withColumn("Wt", NeighborCnt(col("x"),
      col("y"), col("z"), lit(minX.toInt), lit(maxX.toInt), lit(minY.toInt), lit(maxY.toInt),
      lit(minZ.toInt), lit(maxZ.toInt)))


    // Getis-ord stat
    var Result = NeighborDf.withColumn("gscore", GScore(col("x"), col("y"),
      col("z"), col("Wt"), col("NeighCount"), lit(numCells.toInt), lit(X_bar.toDouble), lit(S.toDouble))).
      orderBy(desc("gscore")).select("x", "y", "z").limit(50)

    return Result // YOU NEED TO CHANGE THIS PART
  }
}