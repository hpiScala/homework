package excercise2

import java.sql.{Connection, DriverManager, JDBCType, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import com.sap.db.jdbc.exceptions.DatabaseException
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

class Connection_JDBC {
  val driver = "com.sap.db.jdbc.Driver"
  val url = "jdbc:sap://side.eaalab.hpi.uni-potsdam.de:31815/?autocommit=false"
  val username = "EXERCICE_GROUP"
  val password = "Exercice2016hpi"
  var con: Option[Connection] = None

  def currentMethodName() : String = Thread.currentThread.getStackTrace()(2).getMethodName
  def errorHandler(e : Throwable, methodName : String) : Unit = println("##### Error occurred #####\nSomething went wrong in " + methodName + "\n" + e.getMessage + "\n#####")

  def parseDateTime(input: String): DateTime = {
    val pattern = "yyyyMMdd"
    val dateTime: DateTime = DateTime.parse(input, DateTimeFormat.forPattern(pattern))
    return dateTime
  }

  def parseDateTimeWeb(input: String): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("dd.MM.yyyy")
    val date: Date = simpleDateFormat.parse(input)
    val ans = new SimpleDateFormat("yyyyMMdd").format(date)
    return ans
  }

  def parseSales(input: Int): String = {
    val sales = input.toString().replace(".","")
    return sales
  }

  def establishConnection(): Unit = {
    try {
      Class.forName(driver)
      con = Some(DriverManager.getConnection(url, username, password))
    } catch {
      case e: Throwable => errorHandler(e, currentMethodName())
    }
  }

  def closeConnection(): Unit = {
    try {
      con.foreach(_.close())
    } catch {
      case e: Throwable => errorHandler(e, currentMethodName())
    }
  }

  def retrieveById(id: String): Kunde = {

    try {
      con.foreach(connection => {
        // user input (maybe Sanderson Sander's)
        val cust_id = id.replaceAll("'", "''")

        // create the statement, and run the select query
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(s"SELECT * FROM SAPQ92.KNA1_HPI WHERE KUNDE = '$cust_id'")

        if (resultSet.next()) {
          val customer = Kunde(resultSet)
          return customer
        }

      })
    } catch {
      case e: DatabaseException => errorHandler(e, currentMethodName())
    }
    return null
  }

  def retrieveByNameAndPlz(cust_name: String, cust_plz: String) : List[Map[String, String]] =  {
    try {
      con.foreach(connection => {
        val name = cust_name.replaceAll("'", "''")
        val plz = cust_plz.replaceAll("'", "''")
        var ergebnisse: List[Map[String, String]] = List()
        val statement = connection.createStatement()

        val resultSet = statement.executeQuery(s"SELECT * FROM SAPQ92.KNA1_HPI WHERE PLZ LIKE '%$plz%' AND LOWER(NAME) LIKE LOWER('%$name%')")
        while (resultSet.next()) {
          val kunde = Kunde(resultSet)

          ergebnisse = Map("kundenNummer" -> kunde.kundenNummer, "land" -> kunde.land,
            "name" -> kunde.name, "ort" -> kunde.ort,
            "plz" -> kunde.plz, "region" -> kunde.region,
            "strasse" -> kunde.strasse, "branche" -> kunde.branche,
            "kundengruppe" -> kunde.kundengruppe) :: ergebnisse
        }

        return ergebnisse
      })
    } catch {
      case e: DatabaseException => errorHandler(e, currentMethodName())
    }
    return null
  }

  def retrievePaymentDetails(id: String, zeitpunkt: String): (Int, String, List[Map[String, Any]]) = {

    try {
      con.foreach(connection => {
        val cust_id = id.replaceAll("'", "''")
        val moment = parseDateTimeWeb(zeitpunkt.replaceAll("'", "''"))

        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(s"SELECT * FROM SAPQ92.ACDOCA_HPI WHERE KUNDE = '$cust_id' AND BUCHUNGSDATUM <= '$moment' AND (KONTO = '0000893015' OR KONTO = '0000792000') ORDER BY BUCHUNGSDATUM ASC, KONTO ASC, BELEGNUMMER ASC")

        val queue = new scala.collection.mutable.Queue[Bestellung]
        var remaining: Int = 0
        var outstandingBills: Int = 0
        var transactions: List[Map[String, Any]] = List()
        var waitingTime: List[Int] = List()

        while (resultSet.next()) {
          val bestellung = Bestellung(resultSet)

          if (bestellung.konto == "0000792000") {
            if (queue.isEmpty && remaining >= bestellung.hausBetrag) {
              waitingTime = 0 :: waitingTime
              remaining -= bestellung.hausBetrag
            }
            else {
              queue += bestellung
            }
          }
          else {
            remaining += bestellung.hausBetrag

            while (queue.nonEmpty && remaining >= queue.head.hausBetrag) {
              waitingTime = Days.daysBetween(parseDateTime(queue.head.buchungsdatum), parseDateTime(bestellung.buchungsdatum)).getDays :: waitingTime
              remaining -= queue.dequeue().hausBetrag
            }
          }
        }

        queue.foreach((bestellung: Bestellung) => {
          outstandingBills += bestellung.hausBetrag
          transactions = Map("buchungskreis" -> bestellung.buchungskreis, "geschaeftsjahr" -> bestellung.geschaeftsjahr,
            "belegnummer" -> bestellung.belegnummer, "sollHabenKenn" -> bestellung.sollHabenKenn,
            "konto" -> bestellung.konto, "hausBetrag" -> bestellung.hausBetrag,"intBetrag" -> parseSales(bestellung.hausBetrag),
            "hausWaehrung" -> bestellung.hausWaehrung, "transaktionsBetrag" -> bestellung.transaktionsBetrag,
            "transaktionsWaehrung" -> bestellung.transaktionsWaehrung, "kunde" -> bestellung.kunde,
            "werk" -> bestellung.werk, "material" -> bestellung.material,
            "buchungsdatum" -> bestellung.buchungsdatum) :: transactions
        })

        outstandingBills -= remaining

        val waitingTimeAverage = {
          if (waitingTime.nonEmpty) {
            val avg = Math.round(waitingTime.sum.toDouble / waitingTime.length * 100.0) / 100.0
            if (avg >= 1.0) {avg.toInt.toString}
            else {avg.toString}
        } else {"0"}}
        return (outstandingBills, waitingTimeAverage, transactions)
      })
    } catch {
      case e: DatabaseException => errorHandler(e, currentMethodName())
    }
    return null
  }

  def retrieveHitlist(material1: String, kunde1: String, land1: String,
  region1: String, werk1: String, waehrung1: String, startdatum1: String, enddatum1: String): List[Map[String, Any]] = {

    try {
      con.foreach(connection => {
        val start = parseDateTimeWeb(startdatum1.replaceAll("'", "''"))
        val end = parseDateTimeWeb(enddatum1.replaceAll("'", "''"))
        val material = "%" + material1.replaceAll("'", "''") + "%"
        val kunde = "%" + kunde1.replaceAll("'", "''") + "%"
        val land = "%" + land1.replaceAll("'", "''") + "%"
        val region = "%" + region1.replaceAll("'", "''") + "%"
        val werk = "%" + werk1.replaceAll("'", "''") + "%"
        val waehrung = "%" + waehrung1.replaceAll("'", "''") + "%"

        val statement = connection.createStatement()
        var query = s"SELECT MATERIAL, HAUS_WAEHRUNG, SUM( HAUS_BETRAG) AS UMSATZ FROM SAPQ92.ACDOCA_HPI WHERE KONTO = '0000893015' AND BUCHUNGSDATUM <= '$end' AND BUCHUNGSDATUM >= '$start' AND MATERIAL LIKE '$material' AND KUNDE LIKE '$kunde' AND WERK LIKE '$werk' AND HAUS_WAEHRUNG LIKE '$waehrung'"
        if (land != "%%") {
            query += s" AND KUNDE IN ( SELECT KUNDE  FROM SAPQ92.KNA1_HPI AS A  INNER JOIN SAPQ92.T005T_HPI AS B ON A.LAND = B.LAND WHERE B.NAME LIKE '$land' AND B.SPRACHE = 'D')"
          }
        if (region != "%%"){
            query += s" AND KUNDE IN ( SELECT KUNDE FROM SAPQ92.KNA1_HPI AS A INNER JOIN SAPQ92.T005U AS B ON (A.LAND = B.LAND1 AND A.REGION = B.BLAND ) WHERE B.BEZEI LIKE '$region' AND B.SPRAS = 'D')"
          }
        query += s" GROUP BY MATERIAL, HAUS_WAEHRUNG ORDER BY UMSATZ ASC"

        val resultSet = statement.executeQuery(query)

        var products: List[Map[String, Any]] = List()
        var productsFinal: List[Map[String, Any]] = List()
        var total: Int = 0

        while (resultSet.next()) {
          products = Map("material" -> resultSet.getString("MATERIAL"), "umsatz" -> resultSet.getInt("UMSATZ"), "intUmsatz" -> parseSales(resultSet.getInt("UMSATZ")),
            "waehrung" -> resultSet.getString("HAUS_WAEHRUNG")) :: products
          total += resultSet.getInt("UMSATZ")
        }

        products.foreach((entry: Map[String, Any]) => {
          val anteil = Math.round(entry("umsatz").toString.toDouble / total * 10000.0) / 100.0
          productsFinal = (entry + ("anteil" -> anteil)) :: productsFinal
        })
        return productsFinal.sortWith(_("anteil").toString.toDouble > _("anteil").toString.toDouble)
      })
    } catch {
      case e: DatabaseException => errorHandler(e, currentMethodName())
    }
    return null
  }

  def retrieveSalesInCountries(material1: String, kunde1: String, land1: String,
                      region1: String, werk1: String, waehrung1: String, startdatum1: String, enddatum1: String): List[Map[String, Any]] = {

    try {
      con.foreach(connection => {
        val start = parseDateTimeWeb(startdatum1.replaceAll("'", "''"))
        val end = parseDateTimeWeb(enddatum1.replaceAll("'", "''"))
        val material = "%" + material1.replaceAll("'", "''") + "%"
        val kunde = "%" + kunde1.replaceAll("'", "''") + "%"
        val land = "%" + land1.replaceAll("'", "''") + "%"
        val region = "%" + region1.replaceAll("'", "''") + "%"
        val werk = "%" + werk1.replaceAll("'", "''") + "%"
        val waehrung = "%" + waehrung1.replaceAll("'", "''") + "%"

        val query = s"SELECT B.NAME AS LANDNAME, A.LAND AS CODE, A.UMSATZ AS UMSATZ FROM (SELECT KAEUFER.LAND AS LAND, SUM(Verkauf.HAUS_BETRAG) AS UMSATZ FROM SAPQ92.KNA1_HPI AS Kaeufer INNER JOIN SAPQ92.ACDOCA_HPI AS Verkauf ON Verkauf.KUNDE = Kaeufer.KUNDE WHERE Konto = '0000893015' AND Verkauf.BUCHUNGSDATUM <= '$end' AND Verkauf.BUCHUNGSDATUM >= '$start' AND Verkauf.MATERIAL LIKE '$material' AND Verkauf.KUNDE LIKE '$kunde' AND Verkauf.WERK LIKE '$werk' AND Verkauf.HAUS_WAEHRUNG LIKE '$waehrung' GROUP BY Kaeufer.LAND) AS A, SAPQ92.T005T_HPI AS B WHERE A.LAND = B.LAND AND B.SPRACHE = 'D' AND B.NAME LIKE '$land' ORDER BY UMSATZ ASC"
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(query)

        var products: List[Map[String, Any]] = List()

        while (resultSet.next()) {
          products = Map("land" -> resultSet.getString("LANDNAME"), "code" -> resultSet.getString("CODE"), "umsatz" -> resultSet.getInt("UMSATZ"), "intUmsatz" -> parseSales(resultSet.getInt("UMSATZ")), "waehrung" -> "EUR") :: products        }

        return products
      })
    } catch {
      case e: DatabaseException => errorHandler(e, currentMethodName())
    }
    return null
  }


  def retrieveSales(id: String) : List[String] = {
    try {
      con.foreach(connection => {

        val cust_id = id.replaceAll("'", "''")

        var sales: List[String] = List()

        val statement = connection.createStatement()

        val resultSet = statement.executeQuery(s"SELECT GESCHAFTSJAHR, SUM (HAUS_BETRAG) as UMSATZ, HAUS_WAEHRUNG FROM SAPQ92.ACDOCA_HPI WHERE KONTO = '0000893015' AND KUNDE = '$cust_id' AND (GESCHAFTSJAHR = '2014' OR GESCHAFTSJAHR = '2015') GROUP BY GESCHAFTSJAHR, HAUS_WAEHRUNG ORDER BY GESCHAFTSJAHR ASC")
        while (resultSet.next()) {
          sales = resultSet.getString("HAUS_WAEHRUNG") :: sales
          sales = resultSet.getInt("UMSATZ").toString :: sales
        }

        return sales
      })
    } catch {
      case e: DatabaseException => e.printStackTrace()
    }
    return null
  }

  def retrieveExpenses(id: String) : List[String] = {
    try {
      con.foreach(connection => {

        val cust_id = id.replaceAll("'", "''")

        var expenses: List[String] = List()

        val statement = connection.createStatement()

        val resultSet = statement.executeQuery(s"SELECT GESCHAFTSJAHR, SUM (HAUS_BETRAG) as KOSTEN, HAUS_WAEHRUNG FROM SAPQ92.ACDOCA_HPI WHERE KONTO = '0000792000' AND KUNDE = '$cust_id' AND (GESCHAFTSJAHR = '2014' OR GESCHAFTSJAHR = '2015') GROUP BY GESCHAFTSJAHR, HAUS_WAEHRUNG ORDER BY GESCHAFTSJAHR ASC")
        while (resultSet.next()) {
          expenses = resultSet.getString("HAUS_WAEHRUNG") :: expenses
          expenses = resultSet.getInt("KOSTEN").toString :: expenses
        }

        return expenses
      })
    } catch {
      case e: DatabaseException => e.printStackTrace()
    }
    return null
  }

  def retrieveTransactions2(id: String) : List[Map[String, Any]] = {
    try {
      con.foreach(connection => {

        val cust_id = id.replaceAll("'", "''")

        var transactions2: List[Map[String, Any]] = List()

        val statement = connection.createStatement()

        val resultSet = statement.executeQuery(s"SELECT * FROM SAPQ92.ACDOCA_HPI WHERE KONTO = '0000893015' AND KUNDE = '$cust_id' ORDER BY BUCHUNGSDATUM DESC , BELEGNUMMER DESC LIMIT 10")
        while (resultSet.next()) {
          val bestellung = Bestellung(resultSet)
          transactions2 = Map("buchungskreis" -> bestellung.buchungskreis,
            "geschaeftsjahr" -> bestellung.geschaeftsjahr,
            "belegnummer" -> bestellung.belegnummer,
            "sollHabenKenn" -> bestellung.sollHabenKenn,
            "konto" -> bestellung.konto,
            "hausBetrag" -> bestellung.hausBetrag,
            "intBetrag" -> parseSales(bestellung.hausBetrag),
            "hausWaehrung" -> bestellung.hausWaehrung,
            "transaktionsBetrag" -> bestellung.transaktionsBetrag,
            "transaktionsWaehrung" -> bestellung.transaktionsWaehrung,
            "kunde" -> bestellung.kunde,
            "werk" -> bestellung.werk,
            "material" -> bestellung.material,
            "buchungsdatum" -> bestellung.buchungsdatum) :: transactions2
        }

        return transactions2
      })
    } catch {
      case e: DatabaseException => e.printStackTrace()
    }
    return null
  }

}


