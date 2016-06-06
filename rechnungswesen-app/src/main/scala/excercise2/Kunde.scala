package excercise2

import java.sql.ResultSet
/**
  * Created by Anka on 08/05/16.
  */
case class Kunde (data: ResultSet) {
  val kundenNummer = data.getString("KUNDE")
  val land = data.getString("LAND")
  val name = data.getString("NAME")
  val ort = data.getString("ORT")
  val plz = data.getString("PLZ")
  val region = data.getString("REGION")
  val strasse = data.getString("STRASSE")
  val branche = data.getString("BRANCHE")
  val kundengruppe = data.getString("KUNDENGRUPPE")
}
