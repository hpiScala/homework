package team2.unternehmensanwendungen.exercise1

import java.sql.ResultSet
/**
  * Created by Anka on 08/05/16.
  */
class Sachkontenstamm(result: ResultSet){
  val konto = result.getString("KONTO")
  val sprache = result.getString("SPRACHE")
  val kurzText = result.getString("KURZ_TEXT")
  val langText = result.getString("LANG_TEXT")
}
