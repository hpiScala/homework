import sbt._
import Keys._
import org.scalatra.sbt._
import org.scalatra.sbt.PluginKeys._
import com.earldouglas.xwp.JettyPlugin
import com.mojolly.scalate.ScalatePlugin._
import ScalateKeys._

object Excercise2 extends Build {
  val Organization = "team1.unternehmensanwendungen"
  val Name = "excercise1"
  val Version = "0.1.0"
  val ScalaVersion = "2.11.8"
  val ScalatraVersion = "2.4.0"

  lazy val project = Project (
    "excercise1",
    file("."),
    settings = ScalatraPlugin.scalatraSettings ++ scalateSettings ++ Seq(
      organization := Organization,
      name := Name,
      version := Version,
      scalaVersion := ScalaVersion,
      resolvers += Classpaths.typesafeReleases,
      resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
      libraryDependencies ++= Seq(
        "org.scalatra" %% "scalatra" % ScalatraVersion,
        "org.scalatra" %% "scalatra-scalate" % ScalatraVersion,
        "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test",
        "ch.qos.logback" % "logback-classic" % "1.1.5" % "runtime",
        "org.eclipse.jetty" % "jetty-webapp" % "9.2.15.v20160210" % "container",
        "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
        "org.scalikejdbc" %% "scalikejdbc"       % "2.3.5",
        "ch.qos.logback"  %  "logback-classic"   % "1.1.3",
        "pl.metastack"    %% "metarx"            % "0.1.6"
      ),
      javaOptions ++= Seq(
        "-Xdebug",
        "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"
      ),
      scalateTemplateConfig in Compile <<= (sourceDirectory in Compile){ base =>
        Seq(
          TemplateConfig(
            base / "webapp" / "WEB-INF" / "templates",
            Seq.empty,  /* default imports should be added here */
            Seq(
              Binding("context", "_root_.org.scalatra.scalate.ScalatraRenderContext", importMembers = true, isImplicit = true)
            ),  /* add extra bindings here */
            Some("templates")
          )
        )
      }
    )
  ).enablePlugins(JettyPlugin)
}
