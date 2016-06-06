package excercise2

import java.sql.ResultSet
/**
  * Created by Anka on 08/05/16.
  */
case class Bestellung (result: ResultSet) {
  val buchungskreis = result.getString("BUCHUNGSKREIS")
  val geschaeftsjahr = result.getString("GESCHAFTSJAHR")
  val belegnummer = result.getString("BELEGNUMMER")
  val sollHabenKenn = result.getString("SOLL_HABEN_KEN")
  val konto = result.getString("KONTO")
  val hausBetrag = Math.abs(result.getInt("HAUS_BETRAG")) //hausbeträge sind negativ, daher -
  val hausWaehrung = result.getString("HAUS_WAEHRUNG")
  val transaktionsBetrag = result.getInt("TRANSAKTIONS_BETRAG")
  val transaktionsWaehrung = result.getString("TRANSAKTIONS_WAEHRUNG")
  val kunde = result.getString("KUNDE")
  val werk = result.getString("WERK")
  val material = result.getString("MATERIAL")
  val buchungsdatum = result.getString("BUCHUNGSDATUM")
}