import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.knowm.xchart.{BitmapEncoder, CategoryChartBuilder, SwingWrapper}
import org.knowm.xchart.style.Styler

object LogA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Log Analyzer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load log data
    val logFile = "C:/Users/trabelsm/IdeaProjects/LogA/server.log"
    val logDF = spark.read.textFile(logFile)
      .map(parseLogLine)
      .toDF("ip", "client", "user", "date", "request", "status", "size")

    // Calculate statistics
    val statusCounts = logDF.groupBy("status").count().orderBy("status")

    // Display results
    statusCounts.show()

    // Extract data for plotting
    val statuses = statusCounts.select("status").as[Int].collect()
    val counts = statusCounts.select("count").as[Long].collect()

    // Plotting with XChart
    val chart = new CategoryChartBuilder()
      .width(800)
      .height(600)
      .title("HTTP Response Codes")
      .xAxisTitle("Status Code")
      .yAxisTitle("Count")
      .build()

    chart.getStyler.setLegendPosition(Styler.LegendPosition.InsideNW)
    chart.getStyler.setHasAnnotations(true)

    // Convert counts to Int array for plotting
    val countsInt = counts.map(_.toInt)

    chart.addSeries("Response Codes", statuses, countsInt)

    // Display chart
    new SwingWrapper(chart).displayChart()

    // Save chart as PNG
    //BitmapEncoder.saveBitmap(chart, "./ResponseCodes", BitmapEncoder.BitmapFormat.PNG)

    spark.stop()
  }

  def parseLogLine(log: String): (String, String, String, String, String, Int, Int) = {
    val logPattern = """^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+ \S+ \S+)" (\d{3}) (\d+|-)$""".r
    log match {
      case logPattern(ip, client, user, date, request, status, size) =>
        (ip, client, user, date, request, status.toInt, if (size == "-") 0 else size.toInt)
      case _ => ("", "", "", "", "", 0, 0)
    }
  }
}
