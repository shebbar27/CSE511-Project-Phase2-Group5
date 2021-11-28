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
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    pickupInfo = pickupInfo
      .select("x", "y", "z")
      .where("x >= " + minX + " AND x <= " + maxX
        + " AND y >= " + minY + " AND y <= " + maxY
        + " AND z >= " + minZ +  " AND z <= " + maxZ)
      .orderBy("z", "y", "x")
    var hotnessOfCellsDf = pickupInfo
      .groupBy("z", "y", "x").count()
      .withColumnRenamed("count", "cell_hotness")
      .orderBy("z", "y", "x")
    hotnessOfCellsDf.createOrReplaceTempView("HotnessOfCells")

    // calculate average of hotness of cells
    val avg = hotnessOfCellsDf.select("cell_hotness").agg(sum("cell_hotness")).first().getLong(0).toDouble / numCells

    // calculate Standard deviation
    val stdDev = scala.math.sqrt((hotnessOfCellsDf.withColumn("sqr_cell", pow(col("cell_hotness"), 2)).select("sqr_cell").agg(sum("sqr_cell")).first().getDouble(0) / numCells) - scala.math.pow(avg, 2))

    // get all the adjacent hot cells count by comparing the x,y,z coordinates
    var adjHotCellNumber = spark.sql("SELECT h1.x AS x, h1.y AS y, h1.z AS z, "
      + "sum(h2.cell_hotness) AS cellNumber "
      + "FROM HotnessOfCells AS h1, HotnessOfCells AS h2 "
      + "WHERE (h2.y = h1.y+1 OR h2.y = h1.y OR h2.y = h1.y-1) AND (h2.x = h1.x+1 OR h2.x = h1.x OR h2.x = h1.x-1) AND (h2.z = h1.z+1 OR h2.z = h1.z OR h2.z = h1.z-1)"
      + "GROUP BY h1.z, h1.y, h1.x "
      + "ORDER BY h1.z, h1.y, h1.x" )

    // user defined function to calculate the number of adjacent cells for a given cell
    var calculateNumberOfAdjFunc = udf(
      (minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int, X: Int, Y: Int, Z: Int)
      => HotcellUtils.calculateNumberOfAdjacentCells(minX, maxX, minY, maxY, minZ, maxZ, X, Y, Z))
    var sumOfAdjacentCellHotness = adjHotCellNumber.withColumn("AdjacentCellHotness", calculateNumberOfAdjFunc(lit(minX), lit(maxX), lit(minY), lit(maxY), lit(minZ), lit(maxZ), col("x"), col("y"), col("z")))

    // user defined function to calculate G (Getis-Ord) based on the calculated information
    var gScoreFunc = udf(
      (numCells: Int , x: Int, y: Int, z: Int, sumOfAdjacentCellHotness: Int, cellNumber: Int, avg: Double, stdDev: Double)
      => HotcellUtils.calculateGScore(numCells, x, y, z, sumOfAdjacentCellHotness, cellNumber, avg, stdDev))

    // calculate G Score for every cell, order the data frame by GScore in descending order and keep only first 50 cells
    var gScoreHotCell = sumOfAdjacentCellHotness
      .withColumn("gScore", gScoreFunc(lit(numCells), col("x"), col("y"), col("z"), col("AdjacentCellHotness"), col("cellNumber"), lit(avg), lit(stdDev)))
      .orderBy(desc("gScore")).limit(50)
    gScoreHotCell.show()

    pickupInfo = gScoreHotCell.select(col("x"), col("y"), col("z"))

    return pickupInfo
  }
}
