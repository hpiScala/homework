import excercise2.{Connection_JDBC, Kunde}

val inst: Connection_JDBC = new Connection_JDBC()
inst.establishConnection()

//val products = inst.retrieveHitlist("04.06.2013", "04.06.2014")

val regions = inst.retrieveHitlist("", "", "", "", "", "", "01.10.2014", "31.12.2014")
val countries = inst.retrieveSalesInCountries("", "", "De", "", "", "", "16.06.2010", "16.06.2016")