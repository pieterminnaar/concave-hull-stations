package net.stedin

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.opensphere.geometry.algorithm._
import com.vividsolutions.jts.geom._
import collection.JavaConversions._
import com.vividsolutions.jts.operation.buffer._
import org.postgis.{Point => PGPoint, LinearRing, Polygon => PGPolygon, PGgeometry} 
import java.sql.DriverManager
import java.nio.file.Paths
import net.stedin.dms.logparser.Switch

//run
/* ./bin/spark-submit --class net.stedin.FeederGenerator --master local[2] /Users/pieterm/dev/concave-hull-sbt/target/scala-2.10/concave_hull-assembly-1.0.jar /Users/pieterm/dev/concave-hull-sbt/data */

object FeederGenerator {

  def fileNameMapper(comb: (String, String)) = {
    val fileName = Paths.get(comb._1).getFileName().toString.replace(".log", "")
    (fileName, comb._2)
  }

  def main(args: Array[String]) {
    val logPath = args(0)
    val conf = new SparkConf().setAppName("Feeders")
    val sc = new SparkContext(conf)
    val logs = sc.wholeTextFiles(logPath + "/*.log")   
    val feederLogs = logs.map({fileNameMapper(_)})
    val feederLogLines = feederLogs.flatMap(tup => tup._2.split("\n").map(l => (tup._1, l)))
    val dpfLogLines = feederLogLines.filter(tup => tup._2.contains("-I-,DPF190,,DPF results"))
    val switchLines = dpfLogLines.filter(tup => tup._2.contains("ternal Switch:"))
    val switches = switchLines.map(tup => Switch.parseSwitch(tup))
    switches.take(100).foreach(l => {println(l)})
    sc.stop()
  }
}
