//Importe les bibliothèques nécessaires pour Spark SQL et XChart
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.knowm.xchart.{BitmapEncoder, CategoryChartBuilder, SwingWrapper}
import org.knowm.xchart.style.Styler

// l'objet principal LogA avec la méthode main
object LogA {
  def main(args: Array[String]): Unit = {
    // Crée une instance de SparkSession avec le nom d'application et le mode local.
    val spark = SparkSession.builder
      .appName("Log Analyzer")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
   //Charge le fichier de log, applique parseLogLine et crée un DataFrame.
    val logFile = "C:/Users/trabelsm/IdeaProjects/LogA/server.log"
    val logDF = spark.read.textFile(logFile)
      .map(parseLogLine)
      .toDF("ip", "client", "user", "date", "request", "status", "size")

    // Calcule les statistiques sur les codes de statut, les affiche.
    val statusCounts = logDF.groupBy("status").count().orderBy("status")
       statusCounts.show()
    val statuses = statusCounts.select("status").as[Int].collect()
    val counts = statusCounts.select("count").as[Long].collect()

    // Crée un graphique XChart avec les données.
    val chart = new CategoryChartBuilder()
      .width(800)
      .height(600)
      .title("HTTP Response Codes")
      .xAxisTitle("Status Code")
      .yAxisTitle("Count")
      .build()
    chart.getStyler.setLegendPosition(Styler.LegendPosition.InsideNW)
    chart.getStyler.setHasAnnotations(true)

    // Convertit les comptes en tableau d'entiers pour le tracé
    val countsInt = counts.map(_.toInt)
    chart.addSeries("Response Codes", statuses, countsInt)

    // Affiche le graphique
    new SwingWrapper(chart).displayChart()

    // Sauvegarde le graphique en PNG
    BitmapEncoder.saveBitmap(chart, "./ResponseCodes", BitmapEncoder.BitmapFormat.PNG)

    //arrête Spark.
    spark.stop()
  }
//fonction parselogline pour convertir les fichiers logs en classe
  def parseLogLine(log: String): (String, String, String, String, String, Int, Int) = {
    val logPattern = """^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+ \S+ \S+)" (\d{3}) (\d+|-)$""".r
    log match {
      case logPattern(ip, client, user, date, request, status, size) =>
        (ip, client, user, date, request, status.toInt, if (size == "-") 0 else size.toInt)
      case _ => ("", "", "", "", "", 0, 0)
    }
  }
}
