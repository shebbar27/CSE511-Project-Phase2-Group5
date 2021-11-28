package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  /**
  * Function to calculate the number of adjacent cells for a given cell
  */
  def calculateNumberOfAdjacentCells( minX: Int,  maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int, X: Int, Y: Int, Z: Int): Int = {
    var adjAxisBoundariesCount = 0

    // cell lies on X-boundary
    if (X == minX || X == maxX) {
      adjAxisBoundariesCount += 1
    }
    // cell lies on Y-boundary
    if (Y == minY || Y == maxY) {
      adjAxisBoundariesCount += 1
    }
    // cell lies on Z-boundary
    if (Z == minZ || Z == maxZ) {
      adjAxisBoundariesCount += 1
    }

    adjAxisBoundariesCount match {
      // cell does not lie on any of the axis boundaries => number of adjacent hot cells is 27
      case 0 => 26
      // cell lies on one of the axis boundaries => number of adjacent hot cells is 18
      case 1 => 17
      // cell lies on two of the axis boundaries => number of adjacent hot cells is 12
      case 2 => 11
      // cell lies on three of the axis boundaries => number of adjacent hot cells is 8
      case 3 => 7
      // default case, cell cannot lie on more than three axis boundaries
      case _ => 0
    }
  }

  /**
   * function to calculate the GScore for a given cell
   */
  def calculateGScore(numOfCells: Int, x: Int, y: Int, z: Int, sumOfAdjacentCells: Int, cellNumber: Int , avg: Double, stdDev: Double): Double = {
     (cellNumber.toDouble - (avg * sumOfAdjacentCells.toDouble)) / (stdDev
       * math.sqrt((( numOfCells.toDouble * sumOfAdjacentCells.toDouble)
       - (sumOfAdjacentCells.toDouble * sumOfAdjacentCells.toDouble)) / (numOfCells.toDouble - 1.0)))
  }
}
