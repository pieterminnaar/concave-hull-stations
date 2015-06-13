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

object InsertPostGis {
  def main(args: Array[String]) {
    val gf = new GeometryFactory()
    val conf = new SparkConf().setAppName("Concave Hull")
    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile("/Users/pieterm/dev/concavehull/leveringspunt_coords.txt")
    val valsRDD = inputRDD.filter(l => l.indexOf(",") != 0).map(l => l.split(","))
    val keysRDD = valsRDD.map(l => (l(0).toInt, l))
    val keysXyRDD = keysRDD.mapValues(l => (l(3).trim.toInt, l(4).trim.toInt))
    val keysCrdsRDD = keysXyRDD.mapValues(t => new Coordinate(t._1, t._2))
    val keysGeomsRDD = keysCrdsRDD.mapValues(crd => gf.createPoint(crd).asInstanceOf[Geometry])
    val groups = keysGeomsRDD.groupByKey()
    val groupsArrs = groups.mapValues(crds => crds.toArray)
    val groupsGC = groupsArrs.mapValues(crdArr => gf.createGeometryCollection(crdArr))
    val groupsCH = groupsGC.mapValues(gc => new ConcaveHull(gc, 50))
    val groupsBO = groupsCH.mapValues(ch => new BufferOp(ch.getConcaveHull))
    val groupsBuf = groupsBO.mapValues(bo => bo.getResultGeometry(5))
    val groupsCrds = groupsBuf.mapValues(crds => crds.getCoordinates())
    val groupsPnts = groupsCrds.mapValues(crds => crds.map(crd => new PGPoint(crd.x, crd.y)))
    val groupsLR = groupsPnts.mapValues(pnts => new LinearRing(pnts))
    val groupsPols = groupsLR.mapValues(lrs => new PGPolygon(Array(lrs)))
    val groupsPGeoms = groupsPols.mapValues(pol => new PGgeometry(pol))
    groupsPGeoms.foreachPartition(part => {
	Class.forName("org.postgresql.Driver") 
	val url = "jdbc:postgresql://localhost:5432/stedin" 
	val conn = DriverManager.getConnection(url, "postgres", "manager")
	conn.asInstanceOf[org.postgresql.PGConnection].addDataType("geometry", Class.forName("org.postgis.PGgeometry"))
	val s = conn.prepareStatement("INSERT INTO station_areas (station_id, area_geom) VALUES (?, ?)")
	//val s = conn.prepareStatement("INSERT INTO station_areas (station_id, area_geom) VALUES (?)")
	part.foreach(gc => {
            s.setInt(1, gc._1)
            s.setObject(2, gc._2)
            s.executeUpdate()
            //println(gc._1)
	})
    //conn.commit()
	s.close()
	conn.close()
      }
    )
    sc.stop()
  }
}
