package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.math.abs

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

  def  getNeighbourCount(x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int = {
    // first determine the number of neighbors (include itself)
    var cellTracker = 0

    if (x == minX || x == maxX) {
      cellTracker += 1
    }

    if (y == minY || y == maxY) {
      cellTracker += 1
    }

    if (z == minZ || z == maxZ) {
      cellTracker += 1
    }

    if (cellTracker == 3) { // cell is on corner if all x,y, z value are all min's or all max's or mix of min and max
      cellTracker = 8
    } else if (cellTracker == 2) { // cell is on edge apart from corner
      cellTracker = 12
    } else if (cellTracker == 1) { // cell is on surface, apart from above 2 cases
      cellTracker = 18
    } else {
      cellTracker = 27 // cell is inside

    }
    return cellTracker.toInt
  }
  // Get Weights :  W
  def getWeight(x1:Double, y1:Double, z1:Double, x2:Double, y2:Double, z2:Double): Boolean = {
    return (abs(x1-x2)<=1 && abs(y1-y2)<=1 && abs(z1-z2)<=1)
  }


  // YOU NEED TO CHANGE THIS PART
  def GettisOrd(x: Int, y: Int, z: Int, Wt: Int, neighCnt: Int, Ncell: Int, X_bar: Double, S: Double): Double = {
    var gs = (neighCnt.toDouble - X_bar * Wt.toDouble) / (S * scala.math.sqrt((Ncell.toDouble * Wt.toDouble - (Wt.toDouble * Wt.toDouble)) / (Ncell.toDouble - 1.0)))
    return gs.toDouble
  }


}


