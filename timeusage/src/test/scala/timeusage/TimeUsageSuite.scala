package timeusage

import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{ColumnName, DataFrame, Row}

import scala.util.Random
import scala.util.Properties.isWin

class TimeUsageSuite extends munit.FunSuite:
  val (types, dataFrame) = TimeUsage.read("src/main/resources/timeusage/atussum.csv")
  val cols = TimeUsage.classifiedColumns(types)
  val f = TimeUsage.timeUsageSummary(cols._1, cols._2, cols._3, dataFrame)

  TimeUsage.timeUsageGroupedSql(f).show()
  TimeUsage.timeUsageGrouped(f).show()
  TimeUsage.timeUsageGroupedTyped(TimeUsage.timeUsageSummaryTyped(f)).show()