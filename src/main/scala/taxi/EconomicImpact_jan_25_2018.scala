package taxi

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, from_unixtime, round, sum, unix_timestamp}

/**
  * JAR Edition
  */
object EconomicImpact_jan_25_2018 {
	def main(args: Array[String]): Unit = {

		if (args.length != 3) {
			println("Need 1) big data source \n 2) taxi zones data source \n 3) output data destination")
			System.exit(1)
		}
		val spark = SparkSession.builder()
			.config("spark.master", "local")
			.appName("Taxi Big Data Application")
			.getOrCreate()

		import spark.implicits._

		// minimized version (1 Day) of nyc taxi rides
		val taxiDF = spark.read.load(args(0))

		val taxiZonesDF = spark.read
			.option("header", "true")
			.option("inferSchema", "true")
			.csv(args(1))

		val groupAttemptsDF = taxiDF.
			select(
				round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinID"),
				col("PULocationID"), col("total_amount")
			)
			.where(col("passenger_count") < 3)
			.groupBy(col("fiveMinID"), col("PULocationID"))
			.agg(
				count("*").as("total_trips"),
				sum(col("total_amount")).as("total_amount")
			)
			.withColumn("approximate_datetime", from_unixtime(col("fiveMinID") * 300))
			.drop("fiveMinID")
			.join(taxiZonesDF, col("PULocationID") === col("LocationID"))
			.drop("LocationID", "service_zone")
			.orderBy(col("total_trips").desc_nulls_last)

		val percentGroupAttempt = 0.05
		val percentAcceptGrouping = 0.3
		val discount = 5
		val extraCost = 2
		val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

		val groupingEstimateEconomicImpactDF =
			groupAttemptsDF
				.withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
				.withColumn(
					"acceptedGroupedRidesEconomicImpact",
					col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount)
				)
				.withColumn("rejectedColumnRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
				.withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedColumnRidesEconomicImpact"))


		val totalEconomicImpactDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
		// 40k/day = 12 million/year!!!

		totalEconomicImpactDF.show(false)
		totalEconomicImpactDF.write
			.mode(SaveMode.Overwrite)
			.option("header", "true")
			.csv(args(2))

	}
}
