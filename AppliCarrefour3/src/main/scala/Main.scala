import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

import scala.io.Source
import scala.reflect.io.Path

object Main extends App {

  var TRANSACTION = "transaction"
  private val FOLDER = "C:\\Users\\yasmine\\IdeaProjects\\AppliCarrefour3\\src\\data\\"
  private val TOP = 100


  /**
    * Get list of transactions files
    *
    * @param dir
    * @return
    */
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles
        .filter(_.isFile)
        .filter(_.getName.startsWith(TRANSACTION))
        .toList
    } else {
      List.empty[File]
    }
  }

  val transactionFiles = getListOfFiles(FOLDER)


  processTransactions(transactionFiles)


  /**
    * Get list of referentiel products File of current date
    *
    * @param date
    * @param dir
    * @return
    */
  def getRefProductByDate(date: String, dir: String): List[File] = {

    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles
        .filter(n => {
          n.isFile
        })
        .filter(n => {
          n.getName.startsWith("reference_prod")

        }
        )
        .toList
    } else {
      List[File]()
    }
  }

  /**
    * Build map of price by store
    *
    * @param date
    * @param productPricesList
    * @return
    */
  def buildMapOfPricesByShop(date: String, productPricesList: List[File]) = {

    var mapOfPricesByShop = Map.empty[String, Map[String, String]]

    productPricesList.foreach(
      priceFile => {
        val shopId = priceFile.getName.replace("reference_prod-", "").replace("_" + date + ".data", "")
        var mapOfPrices = Map.empty[String, String]
        for (line <- Source.fromFile(FOLDER + priceFile.getName).getLines) {
          mapOfPrices = mapOfPrices + (line.split('|')(0) -> line.split('|')(1))
        }
        mapOfPricesByShop = mapOfPricesByShop + (shopId -> mapOfPrices)

      }
    )
    mapOfPricesByShop

  }

  /**
    * Enriching transactions with prices
    *
    * @param file
    * @param mapOfPricesByShop
    * @return
    */
  def enrichTransaction(file: File, mapOfPricesByShop: Map[String, Map[String, String]]) = {
    var l = List.empty[Transaction]
    for (line <- Source.fromFile(FOLDER + file.getName).getLines) {
      val shopId = line.split('|')(2)
      val productId = line.split('|')(3)
      val price = mapOfPricesByShop.getOrElse(shopId, Map.empty).getOrElse(productId, "0")

      l = l :+ Transaction(line.split('|')(0), line.split('|')(1), line.split('|')(2), line.split('|')(3), line.split('|')(4).toInt, price.toDouble)
    }
    l
  }

  /**
    * Transaction processing
    *
    * @param file
    */
  def processTransaction(file: File) = {

    println("process transaction " + file)

    val date = file.getName.replace("transactions_", "").replace(".data", "")
    val productPricesList = getRefProductByDate(date, FOLDER)
    val mapOfPricesByShop = buildMapOfPricesByShop(date, productPricesList)

    val transactions = enrichTransaction(file, mapOfPricesByShop)

    /**
      * Calculate Top 100 sales by store
      */

    val top100SalesByShop = transactions
      .groupBy(t => (t.shopId, t.productId))
      .map(g => g._2.reduce((a, b) => a.copy(qty = a.qty + b.qty)))
      .groupBy(t => t.shopId)
      .map(g => {
        (g._1, g._2.toList.sortWith((t1, t2) => t1.qty > t2.qty).take(TOP))
      })

    // println(top100Sales)

    println(" ************ TOP 100 des produits vendus par magasin *************** ")
    top100SalesByShop.foreach(g => {
      println("TOP 100 des produits dans le magasin " + g._1)
      g._2.foreach(a => {
        println("\t Le produit " + a.productId + " a été vendu " + a.qty + " fois dans le magasin " + g._1)
      })
    })
    ///////////////////////////////////////////////////////////////////////////////////////////////
    top100SalesByShop.foreach(g => {
      val filename = "top_100_ventes_" + g._1 + "_" + date + ".data"


      g._2.foreach(a => {

        val content = (a.productId + "|" + a.qty )
          .getBytes()
        Files.write(Paths.get(filename), content)

        //
      })
    })



    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
      * Calculate global Top 100 sales
      */
    val top100SalesGlobal = transactions
      .groupBy(t => (t.productId))
      .map(g => g._2.reduce((a, b) => a.copy(qty = a.qty + b.qty)))
      .toList.sortWith((t1, t2) => t1.qty < t2.qty)
      .take(TOP)

    println("************** TOP 100 des produits dans tous les magasins *********************")

    top100SalesGlobal.foreach(g => println("\t Le produit " + g.productId + " a été vendu " + g.qty + " fois"))


    /**
      * Calculate Top 100 turnover by store
      */
    val top100RevenueByShop = transactions
      .groupBy(t => (t.shopId, t.productId))
      .map(g => g._2.reduce((a, b) => a.copy(revenu = a.qty * a.price + b.qty * b.price)))
      .groupBy(t => t.shopId)
      .map(g => {
        (g._1, g._2.toList.sortWith((t1, t2) => t1.revenu > t2.revenu).take(TOP))
      })

    println("************** TOP 100 CA par magasin ******************** ")

    top100RevenueByShop.foreach(g => {
      println("TOP 100 dans le magasin " + g._1)
      g._2.foreach(a => {
        println("\t Le chiffre d'affaire du produit " + a.productId + " est de " + a.revenu + " € dans le magasin " + g._1)
      })
    })


    /**
      * Calculate global Top 100 turnover
      */
    val top100Revenue = transactions
      .groupBy(t => t.productId)
      .map(g => g._2.reduce((a, b) => a.copy(globalTurnover = a.qty * a.price + b.qty * b.price)))
      .toList.sortWith((t1, t2) => t1.globalTurnover > t2.globalTurnover)
      .take(TOP)


    println("CA dans tous les magasins ")

    top100Revenue.foreach(g => println("\t Le produit " + g.productId + " a un chiffre d'affaire de " + g.globalTurnover + " €"))


  }

  def processTransactions(tFiles: List[File]): Unit = {
    tFiles.foreach(processTransaction)
  }

  case class ProductPrice(productId: String, price: String)

  case class Transaction(id: String, date: String, shopId: String, productId: String, qty: Int, price: Double, revenu: Double = 0.0, globalTurnover: Double = 0.0)

}