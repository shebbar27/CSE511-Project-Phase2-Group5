package cse512

object HotzoneUtils {
  /*
  * Input: queryRectangle:String, pointString:String
  * Output: Boolean (true or false)
  * Definition: Parse queryRectangle as x & Y coordinates of two diagonally opposite points
  *             Parse pointString as X & Y coordinates
  *             Check whether the queryRectangle fully contains the point considering on-boundary points as well
  * Example Inputs: queryRectangle = "-155.940114, 19.081331, -155.618917, 19.5307"
  *                 pointString = "-88.331492, 32.324142"
  * */
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // check for validity of input data, i.e whether input is null or empty
    if(queryRectangle == null || queryRectangle.isEmpty || pointString == null || pointString.isEmpty) {
      return false
    }

    val rectangleCoordinates = queryRectangle.split(",")
    val pointCoordinates = pointString.split(",")

    // check whether the points have correct number of coordinates
    if(rectangleCoordinates.length < 4 || pointCoordinates.length < 2) {
      return false
    }

    val xOfCorner1 = rectangleCoordinates(0).trim.toDouble
    val yOfCorner1 = rectangleCoordinates(1).trim.toDouble
    val xOfCorner2 = rectangleCoordinates(2).trim.toDouble
    val yOfCorner2 = rectangleCoordinates(3).trim.toDouble
    val pointX = pointCoordinates(0).trim.toDouble
    val pointY = pointCoordinates(1).trim.toDouble

    //check whether the rectangle contains given point
    if(pointX >=  math.min(xOfCorner1, xOfCorner2) && pointX <= math.max(xOfCorner1, xOfCorner2)
      && pointY >= math.min(yOfCorner1, yOfCorner2) && pointY <= math.max(yOfCorner1, yOfCorner2)) {
      return true
    }

    return false
  }

}
