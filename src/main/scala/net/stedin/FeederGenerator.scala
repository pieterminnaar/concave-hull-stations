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
import net.stedin.dms.logparser._
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SQLContext

//run
/* ./bin/spark-submit --class net.stedin.FeederGenerator --master local[2] /Users/pieterm/dev/concave-hull-sbt/target/scala-2.10/concave_hull-assembly-1.0.jar /Users/pieterm/dev/concave-hull-sbt/data */

object FeederGenerator {

  def fileNameMapper(comb: (String, String)) = {
    val fileName = Paths.get(comb._1).getFileName().toString.replace(".log", "")
    (fileName, comb._2)
  }

  def transformers(sc: SparkContext, logPath: String): RDD[Transformer] = {
    val txTxt = sc.textFile(logPath + "/trafo_schakelaar.txt")
    val txRDD = txTxt.map(l => l.split("\t"))
    txRDD.map(l => Transformer(l(0), l(1), l(2)))
  }

  def transformerCustomerCounts(sc: SparkContext, logPath: String): RDD[TransformerCustomerCount] = {
    val custTxt = sc.textFile(logPath + "/ProductionCustomersModel.txt")
    val trafoRDD = custTxt.map(l => l.slice(150, 169).replace("SP_", "").trim())
    val trafos = sc.parallelize(trafoRDD.countByValue().toSeq)
    trafos.map(tup => TransformerCustomerCount(tup._1, tup._2))
  }

  def switches(sc: SparkContext, logPath: String): RDD[Switch] = {
    val logs = sc.wholeTextFiles(logPath + "/*.log")   
    val feederLogs = logs.map({fileNameMapper(_)})
    val feederLogLines = feederLogs.flatMap(tup => tup._2.split("\n").map(l => (tup._1, l)))
    val dpfLogLines = feederLogLines.filter(tup => tup._2.contains("-I-,DPF190,,DPF results"))
    val switchLines = dpfLogLines.filter(tup => tup._2.contains("ternal Switch:"))
    switchLines.map(tup => Switch.parseSwitch(tup))
  }

  def processResults(sc: SparkContext, 
                     switches: RDD[Switch], 
                     transformers: RDD[Transformer], 
                     transCustCounts: RDD[TransformerCustomerCount]): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._
    import sqlContext.implicits._
    val dmsSwitches = switches.toDF()
    dmsSwitches.registerTempTable("switches")
    val dmsTransformers = transformers.toDF()
    dmsTransformers.registerTempTable("trafos")
    val dmsTrafoCustCounts = transCustCounts.toDF()
    dmsTrafoCustCounts.registerTempTable("transformer_counts") 
    val joined = sqlContext.sql("""
       SELECT s.substation, s.feeder, 
              count(distinct(t.stationId)) as stations, 
              sum(c.customerCount) customers
       FROM switches s
       JOIN trafos t
       ON s.id = t.switchId
       JOIN transformer_counts c
       ON c.trafoId = t.id
       GROUP BY s.substation, s.feeder""")
    joined.rdd.saveAsTextFile("/Users/pieterm/dev/concave_hull/feeders")
  }

  def main(args: Array[String]) {
    val logPath = args(0)
    val conf = new SparkConf().setAppName("Feeders")
    val sc = new SparkContext(conf)
    val dmsSwitches = switches(sc, logPath)
    val trafos = transformers(sc, logPath)
    val dmsCustomers = transformerCustomerCounts(sc, logPath)
    processResults(sc, dmsSwitches, trafos, dmsCustomers)
    //TODO: do group by query
    sc.stop()
  }
}
