package excercise2

import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatra._
import org.scalatra.scalate.ScalateSupport

class WebApp extends RechnungswesenAppStack with ScalateSupport {

  var last_customer: Kunde = null
  var last_date: String = ""

  get("/") {
    contentType = "text/html"
    layoutTemplate("/login", "layout" -> "")
  }

  post("/hana-brain/") {
    var command = params.getOrElse("text", "")

    if (command == "") "FEHLER"

    val inst: Connection_JDBC = new Connection_JDBC()
    inst.establishConnection()

    if (command.contains("Zahlungsmoral der ")) {
      command = command.split("Zahlungsmoral der ")(1)

      var name: String = ""
      var zeitpunkt: String = ""

      if (command.contains(" für den ")) {
        name = command.split(" für den ")(0)
        zeitpunkt = command.split(" für den ")(1).replace(' ', '.').replace('-', '.')
      }
      else {
        name = command
        zeitpunkt = new SimpleDateFormat("dd.mm.yyyy").format(Calendar.getInstance().getTime())
      }

      val ergebnisse = inst.retrieveByNameAndPlz(name, "")

      if (ergebnisse.size == 1) {
        val kunde: Kunde = inst.retrieveById(ergebnisse(0)("kundenNummer"))

        last_customer = kunde
        last_date = zeitpunkt

        val transactions = inst.retrievePaymentDetails(kunde.kundenNummer, zeitpunkt)

        if (transactions._1 == 0) {
          if (transactions._2.contains("0")) {
            "Das ist ein super Kunde! Die " + kunde.name + " hat keine offenen Zahlungen und begleicht immer alle Rechnungen sofort." + "-DIV-" + ""
          }
          else {
            "Das ist ein super Kunde! Die " + kunde.name + " hat keine offenen Zahlungen und bezahlt ihre Rechnungen in durchschnittlich " + transactions._2 + " Tagen." + "-DIV-" + ""
          }
        }
        else {
          "Mit solchen Schmarotzern solltest du besser keine Geschäfte mehr machen. Die " + kunde.name + " hat offene Zahlungen in Höhe von " + transactions._1 + " Euro und bezahlt ihre Rechnungen in durchschnittlich " + transactions._2 + " Tagen." + "-DIV-" + ""
        }
      }
      else {
        "Ich konnte leider keinen eindeutigen Kunden finden." + "-DIV-" + ""
      }
    }
    else if (command.contains("Zeig") && command.contains("offenen Zahlungen")) {
      if (last_customer != null && last_date != "") {
        val transactions = inst.retrievePaymentDetails(last_customer.kundenNummer, last_date)

        val d: String = layoutTemplate("/offene-zahlungen", "rest" -> transactions._1, "wartezeit" -> (transactions._2 + " Tage"), "transactions" -> transactions._3, "layout" -> "")

        "Hier sind die offenen Zahlungen der " + last_customer.name + " für den " + last_date + ":" + "-DIV-" + d.split("</h4>")(2)
      }
      else {
        "Mir ist nicht klar, welchen Kunden Du meinst." + "-DIV-" + ""
      }
    }
    else if (command.contains("Bestseller") && command.contains("Kunde")) {
      val kundenNummer = request.getHeader("referer").split(Array('/', '?'))(4)
      val kunde: Kunde = inst.retrieveById(kundenNummer)
      last_customer = kunde

      if (kunde != null) {
        val products = inst.retrieveHitlist("", kunde.kundenNummer, "", "", "", "", "01.01.2010", "01.06.2016")

        if (products.nonEmpty) {
          "Der Bestseller der " + kunde.name + " ist das Produkt " + products(0)("material") + ". Mit einem Umsatz in Höhe von " + products(0)("umsatz") + " " + products(0)("waehrung") + " macht dieses Produkt " + products(0)("anteil") + "% des Gesamtumsatzes aus." + "-DIV-" + ""
        }
        else {
          "Dieser Kunde hat noch kein Produkt verkauft." + "-DIV-" + ""
        }
      }
      else {
        "Mir ist nicht klar, welchen Kunden Du meinst." + "-DIV-" + ""
      }
    }
    else if (command.contains("Land") && command.contains("meisten Umsatz")) {
      val countries = inst.retrieveSalesInCountries("", "", "", "", "", "", "01.01.2010", "01.06.2016")

      "In " + countries(0)("land") + " machen wir mit " + countries(0)("umsatz") + " " + "EUR" + " den meisten Umsatz." + "-DIV-" + ""
    }
    else if (command.contains("Umsatz") && command.contains("Deutschland")) {
      val countries = inst.retrieveSalesInCountries("", "", "Deutschland", "", "", "", "01.01.2010", "01.06.2016")

      "In Deutschland machen wir einen Umsatz in Höhe von " + countries(0)("umsatz") + " " + "EUR" + "." + "-DIV-" + ""
    }
    else if (command.contains("zweite Quartal") && command.contains("2014")) {
      if (last_customer != null) {
        val products = inst.retrieveHitlist("", last_customer.kundenNummer, "", "", "", "", "01.04.2014", "30.06.2014")

        if (products.nonEmpty) {
          "Im zweiten Quartal von 2014 ist der Bestseller der " + last_customer.name + " das Produkt " + products(0)("material") + ". Mit einem Umsatz in Höhe von " + products(0)("umsatz") + " " + products(0)("waehrung") + " macht dieses Produkt " + products(0)("anteil") + "% des Gesamtumsatzes aus." + "-DIV-" + ""
        }
        else {
          "Dieser Kunde hat noch kein Produkt verkauft." + "-DIV-" + ""
        }
      }
      else {
        "Mir ist nicht klar, welchen Kunden Du meinst." + "-DIV-" + ""
      }
    }
    else if (command.contains("Zeig") && command.contains("Kunde")) {
      command = {
        if (command.contains("Kunden ")) command.split("Kunden ")(1)
        else command.split("Kunde ")(1)
      }

      var name: String = command

      val ergebnisse = inst.retrieveByNameAndPlz(name, "")

      if (ergebnisse.size == 1) {
        "Hier ist der Kunde " + ergebnisse(0)("name") + "-DIV-" + "<script>window.document.location='/kunde/" + ergebnisse(0)("kundenNummer") + "';</script>"
      }
      else {
        "Ich konnte leider keinen eindeutigen Kunden finden." + "-DIV-" + ""
      }
    }
    else if (command.contains("Zeig") && command.contains("Analyse")) {
      "Hier ist die Analyse" + "-DIV-" + "<script>window.document.location='/analyse/';</script>"
    }
    else if (command.contains("Witz")) {
      "Die Daten, die Ich verwalten muss, sind ein Witz! Cool Werkzeug GmbH, echt jetzt?!" + "-DIV-" + ""
    }
    else {
      "Ich habe dich leider nicht verstanden." + "-DIV-" + ""
    }
  }

  get("/search/") {
    contentType = "text/html"
    layoutTemplate("/search", "hana" -> params.getOrElse("hana", false))
  }

  get("/ergebnisse/") {
    val inst: Connection_JDBC = new Connection_JDBC()
    inst.establishConnection()
    val cust_id = params("id")

    if (!params("id").isEmpty) redirect("/kunde/" + params("id"))

    val ergebnisse = inst.retrieveByNameAndPlz(params("name"), params("plz"))

    if (ergebnisse.size == 1) redirect("/kunde/" + ergebnisse(0)("kundenNummer"))

    contentType = "text/html"

    layoutTemplate("/ergebnisse", "hana" -> params.getOrElse("hana", false), "ergebnisse" -> ergebnisse)
  }

  get("/analyse/") {
    val inst: Connection_JDBC = new Connection_JDBC()
    inst.establishConnection()

    contentType = "text/html"

    layoutTemplate("/analyse", "hana" -> params.getOrElse("hana", false))
  }

  post("/analyse-daten/") {
    val inst: Connection_JDBC = new Connection_JDBC()
    inst.establishConnection()

    val products = inst.retrieveHitlist(params.getOrElse("material", ""), params.getOrElse("kunde", ""), params.getOrElse("land", ""), params.getOrElse("region", ""), params.getOrElse("werk", ""), params.getOrElse("waehrung", ""), params.getOrElse("startdatum", ""), params.getOrElse("enddatum", ""))

    contentType = "text/html"

    layoutTemplate("/analyse-daten", "products" -> products, "layout" -> "")
  }

  post("/analyse-karte/") {
    val inst: Connection_JDBC = new Connection_JDBC()
    inst.establishConnection()

    val products = inst.retrieveSalesInCountries(params.getOrElse("material", ""), params.getOrElse("kunde", ""), params.getOrElse("land", ""), params.getOrElse("region", ""), params.getOrElse("werk", ""), params.getOrElse("waehrung", ""), params.getOrElse("startdatum", ""), params.getOrElse("enddatum", ""))

    //val regions = inst.retrieveSalesInRegions(params.getOrElse("material", ""), params.getOrElse("kunde", ""), params.getOrElse("land", ""), params.getOrElse("region", ""), params.getOrElse("werk", ""), params.getOrElse("waehrung", ""), params.getOrElse("startdatum", ""), params.getOrElse("enddatum", ""))

    contentType = "text/html"

    layoutTemplate("/analyse-karte", "products" -> products, /*"regions" -> regions,*/ "layout" -> "")
  }

  get("/kunde/:id") {
    val inst: Connection_JDBC = new Connection_JDBC()
    inst.establishConnection()

    val kunde: Kunde = inst.retrieveById(params("id"))
    if (kunde == null) redirect("/search/")

    val sales = inst.retrieveSales(kunde.kundenNummer)
    val salesWaehrung = {
      if (sales.size > 0) " " + sales(1)
      else ""
    }
    val umsatz2014 = {
      if (sales.size > 0) sales(0).toInt
      else 0
    }
    val umsatz2015 = {
      if (sales.size > 2) sales(2).toInt
      else 0
    }

    val expenses = inst.retrieveExpenses(kunde.kundenNummer)
    val expensesWaehrung = {
      if (expenses.size > 0) " " + expenses(1)
      else ""
    }
    val ergebnis2014 = {
      if (sales.size > 0 && expenses.size > 0) (sales(0).toInt + expenses(0).toInt)
      else 0
    }

    val ergebnis2015 = {
      if (sales.size > 2 && expenses.size > 2) (sales(2).toInt + expenses(2).toInt)
      else 0
    }

   val transactions2 = inst.retrieveTransactions2(kunde.kundenNummer)


    //val transactions = inst.retrievePaymentDetails(params("id"), params.getOrElse("enddatum", "03.06.2014"))

    contentType = "text/html"

    layoutTemplate("/kunde", "hana" -> params.getOrElse("hana", false), "kundenNummer" -> kunde.kundenNummer, "land" -> kunde.land,
      "name" -> kunde.name, "ort" -> kunde.ort, "plz" -> kunde.plz, "region" -> kunde.region,
      "strasse" -> kunde.strasse, "branche" -> kunde.branche, "kundengruppe" -> kunde.kundengruppe,
      //"rest" -> transactions._1, "wartezeit" -> (transactions._2 + " Tage"), "transactions" -> transactions._3,
      "umsatz2014" -> (umsatz2014.toString + salesWaehrung), "umsatz2015" -> (umsatz2015.toString + salesWaehrung),
      "ergebnis2014" -> (ergebnis2014.toString + expensesWaehrung), "ergebnis2015" -> (ergebnis2015.toString + expensesWaehrung),
      "transactions2" -> transactions2)
    //inst.closeConnection()

  }

  post("/offene-zahlungen/") {
    val inst: Connection_JDBC = new Connection_JDBC()
    inst.establishConnection()

    val kunde: Kunde = inst.retrieveById(params("id"))

    val transactions = inst.retrievePaymentDetails(params("id"), params("zeitpunkt"))

    contentType = "text/html"

    layoutTemplate("/offene-zahlungen", "rest" -> transactions._1, "wartezeit" -> (transactions._2 + " Tage"), "transactions" -> transactions._3, "layout" -> "")
    //inst.closeConnection()

  }

  // prevents output of useless scalatra exceptions
  error {
    case e: ScalatraException => redirect("/search/")
    //case e: Throwable => redirect("/")
  }

}
