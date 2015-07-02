package net.stedin.dms.logparser

object Customer {
  def parseCustomer(clientFileLine: String): Customer = {
    val trafoId = clientFileLine.slice(150, 169).replace("SP_", "").trim()
    val x = clientFileLine.slice(242, 252).trim()
    val y = clientFileLine.slice(255, 268).trim()
    Customer(trafoId, x, y)
  }
}

case class Customer(trafoId: String, x: String, y: String)
