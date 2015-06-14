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

//run
/* ./bin/spark-submit --class net.stedin.InsertPostGis --master local[2] /Users/pieterm/dev/concave-hull-sbt/target/scala-2.10/concave_hull-assembly-1.0.jar jdbc:postgresql://localhost:5432/stedin postgres manager */
/* Hostname	stedin.cleo3mrfvpnt.eu-west-1.rds.amazonaws.com */
object InsertPostGis {
  val gf = new GeometryFactory()

  def createPointGeometry(x: Int, y: Int) = {
     val crd = new Coordinate(x, y)
     gf.createPoint(crd).asInstanceOf[Geometry]
  }

  def createAreaGeometry(coords: Array[Geometry]): Geometry = {
    val gc = gf.createGeometryCollection(coords)
    val ch = new ConcaveHull(gc, 50)
    var geom: Geometry = null
    try {
          geom = ch.getConcaveHull()
    } catch {
      case e: Exception => geom = gc
    }
    val bo = new BufferOp(geom)
    bo.getResultGeometry(5)
  }

  def main(args: Array[String]) {
    val url = args(0)
    val user = args(1)
    val password = args(2)
    val conf = new SparkConf().setAppName("Concave Hull")
    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile("/Users/pieterm/dev/concave-hull-sbt/data/leveringspunt_coords.txt")
    val valsRDD = inputRDD.filter(l => l.indexOf(",,") < 0).map(l => l.split(","))
    val keysRDD = valsRDD.map(l => (l(0).toInt, l))
    val keysCrdsRDD = keysRDD.mapValues(l => createPointGeometry(l(1).trim.toInt, l(2).trim.toInt))
    val groups = keysCrdsRDD.groupByKey()
    val groupsArrs = groups.mapValues(crds => createAreaGeometry(crds.toArray))
    val groupsPolys = groupsArrs.filter(geom => geom._2.getNumGeometries() == 1)
    val groupsCrds = groupsPolys.mapValues(crds => crds.getCoordinates())
    val groupsPnts = groupsCrds.mapValues(crds => crds.map(crd => new PGPoint(crd.x, crd.y)))
    val groupsLR = groupsPnts.mapValues(pnts => new LinearRing(pnts))
    val groupsPols = groupsLR.mapValues(lrs => new PGPolygon(Array(lrs)))
    val groupsPGeoms = groupsPols.mapValues(pol => new PGgeometry(pol))
    groupsPGeoms.foreachPartition(part => {
	Class.forName("org.postgresql.Driver") 
	/*val url = "jdbc:postgresql://localhost:5432/stedin" */
	val conn = DriverManager.getConnection(url, user, password)
	conn.asInstanceOf[org.postgresql.PGConnection].addDataType("geometry", Class.forName("org.postgis.PGgeometry"))
	val s = conn.prepareStatement("INSERT INTO station_areas (station_id, area_geom) VALUES (?, ?)")
	part.foreach(gc => {
            s.setInt(1, gc._1)
            s.setObject(2, gc._2)
            try {
              s.executeUpdate() 
            } catch {
              case e: Exception => println("Insert failed: ", gc._1)
	    }
	})
	s.close()
	conn.close()
      }
    )
    sc.stop()
  }
}
