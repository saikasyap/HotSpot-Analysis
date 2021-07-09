package cse512
import scala.math.{max, min}

object HotzoneUtils {
  // Same as Project Milestone-4
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val rect = queryRectangle.split(',').map((x: String) => x.trim().toDouble)
    val point = pointString.split(',').map((x: String) => x.trim().toDouble)
    val max_x = max(rect(0), rect(2))
    val min_x = min(rect(0), rect(2))
    val max_y = max(rect(1), rect(3))
    val min_y = min(rect(1), rect(3))
    return (point(0) >= min_x && point(0) <= max_x && point(1) >= min_y && point(1) <= max_y)
  }
}
