package net.stedin.dms.logparser

object Switch {
  def parseSwitch(logLine: (String, String)): Switch = {
    val vals = logLine._2.split("ternal Switch: ")(1).split(" - ")
    val id = vals(0)
    val feeder = vals(2)
    Switch(logLine._1, feeder, id)
  }
}

case class Switch(substation: String, feeder: String, id: String)
