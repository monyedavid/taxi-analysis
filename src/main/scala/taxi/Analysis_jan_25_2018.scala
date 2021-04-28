package taxi

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object Analysis_jan_25_2018 extends App {
	val spark = SparkSession.builder()
		.config("spark.master", "local")
		.appName("Taxi Big Data Application")
		.getOrCreate()

	import spark.implicits._

	// minimized version (1 Day) of nyc taxi rides
	val taxiDF = spark.read.load(constants.localResourceData + "yellow_taxi_jan_25_2018")

	val taxiZonesDF = spark.read
		.option("header", "true")
		.option("inferSchema", "true")
		.csv(constants.localResourceData + "taxi_zones.csv")

	/**
	  * Questions: <br/>
	  *
	  * 1. Which zones have the most pickups/dropoffs overall?<br/>
	  * 2. What are the peak hours for taxi?<br/>
	  * 3. How are the trips distributed by length? Why are people taking the cab?<br/>
	  * 4. What are the peak hours for long/short trips?<br/>
	  * 5. What are the top 3 pickup/dropoff zones for long/short trips?<br/>
	  *
	  * 6. How are people paying for the ride, on long/short trips?<br/>
	  * 7. How is the payment type evolving with time?<br/>
	  * 8. Can we explore a ride-sharing opportunity by grouping close short trips?<br/>
	  * 9. Estimate time of day for peak-demands in all zones | approximation of where&when taxis should be available on sight in %
	  */

	// 1. Which zones have the most pickups/drop-offs overall?<br/>
	val pickupByTaxiZoneDF = taxiDF.groupBy("PULocationID")
		.agg(count("*").as("totalTrips"))
		.join(taxiZonesDF, col("PULocationID") === col("LocationID"))
		.drop("LocationID", "service_zone")
		.orderBy(col("totalTrips").desc_nulls_last)

	/**
	  * pickupByTaxiZoneDF.show(false)
	  * +------------+----------+---------+----------------------------+
	  * |PULocationID|totalTrips|Borough  |Zone                        |
	  * +------------+----------+---------+----------------------------+
	  * |237         |15945     |Manhattan|Upper East Side South       |
	  * |161         |15255     |Manhattan|Midtown Center              |
	  * |236         |13767     |Manhattan|Upper East Side North       |
	  * |162         |13715     |Manhattan|Midtown East                |
	  * |170         |11702     |Manhattan|Murray Hill                 |
	  * |234         |11488     |Manhattan|Union Sq                    |
	  * |230         |11455     |Manhattan|Times Sq/Theatre District   |
	  * |186         |10319     |Manhattan|Penn Station/Madison Sq West|
	  * |48          |10091     |Manhattan|Clinton East                |
	  * |163         |9845      |Manhattan|Midtown North               |
	  * |142         |9810      |Manhattan|Lincoln Square East         |
	  * |138         |9009      |Queens   |LaGuardia Airport           |
	  * |107         |8045      |Manhattan|Gramercy                    |
	  * |239         |7908      |Manhattan|Upper West Side South       |
	  * |164         |7896      |Manhattan|Midtown South               |
	  * |141         |7744      |Manhattan|Lenox Hill West             |
	  * |68          |7733      |Manhattan|East Chelsea                |
	  * |79          |7344      |Manhattan|East Village                |
	  * |100         |7171      |Manhattan|Garment District            |
	  * |238         |6764      |Manhattan|Upper West Side North       |
	  * +------------+----------+---------+----------------------------+
	  *
	  */

	// 1b - group by borough
	val pickupByBorough = pickupByTaxiZoneDF.groupBy(col("Borough"))
		.agg(sum(col("totalTrips")).as("totalTrips"))
		.orderBy(col("totalTrips").desc_nulls_last)

	/**
	  * pickupByBorough.show
	  * +-------------+----------+
	  * |      Borough|totalTrips|
	  * +-------------+----------+
	  * |    Manhattan|    304266|
	  * |       Queens|     17712|
	  * |      Unknown|      6644|
	  * |     Brooklyn|      3037|
	  * |        Bronx|       211|
	  * |          EWR|        19|
	  * |Staten Island|         4|
	  * +-------------+----------+
	  *
	  * [nb:]
	  * Data is extremely skewed towards Manhattan
	  *
	  * [proposal:]
	  * . Differentiate prices according to the pickup/drop-off area, & by demand
	  * .    - slightly increase prices for manhattan areas because of popularity
	  * .    - decrease the prices of other borough to incentives
	  */

	// 2. What are the peak hours for taxi?
	val pickupByHourDF = taxiDF
		.withColumn("hour", hour(col("tpep_pickup_datetime")))
		.groupBy("hour")
		.agg(count("*").as("totalTrips"))
		.orderBy(col("totalTrips").desc_nulls_last)

	/**
	  *
	  * pickupByHourDF.show
	  * +----+----------+
	  * |hour|totalTrips|
	  * +----+----------+
	  * |  17|     22121|
	  * |  18|     21598|
	  * |  20|     20884|
	  * |  19|     20318|
	  * |  21|     19528|
	  * |   7|     18867|
	  * |  16|     18664|
	  * |  14|     17843|
	  * |  13|     17483|
	  * |   8|     16840|
	  * |  15|     16160|
	  * |  12|     16082|
	  * |  11|     16001|
	  * |  10|     15564|
	  * |   6|     15445|
	  * |   9|     15348|
	  * |  22|     14652|
	  * |   5|      8600|
	  * |  23|      7050|
	  * |   0|      3978|
	  * +----+----------+
	  *
	  * [proposal]:
	  * . Differentiate prices according to demand
	  * .      - 7,8 am decreased prices to incentives
	  * .      - 5-9m slightly increase prices | surge
	  */

	// 3.  How are the trips distributed by length? Why are people taking the cab
	val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
	val LongDistanceThreshold = 30 // 30 Miles

	val tripDistanceStatDF = tripDistanceDF.select(
		count("*").as("count"),
		lit(LongDistanceThreshold).as("threshold"),
		mean("distance").as("mean"),
		stddev("distance").as("stddev"),
		min("distance").as("min"),
		max("distance").as("max")
	)


	/**
	  * tripDistanceStatDF.show
	  *
	  * +------+---------+-----------------+-----------------+---+----+
	  * | count|threshold|             mean|           stddev|min| max|
	  * +------+---------+-----------------+-----------------+---+----+
	  * |331893|       30|2.717989442380494|3.485152224885052|0.0|66.0|
	  * +------+---------+-----------------+-----------------+---+----+
	  */

	val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= LongDistanceThreshold)
	val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count() // How many trips are long\short with a 30 Miles threshold

	/**
	  *
	  * tripsByLengthDF.show
	  *
	  * +------+------+
	  * |isLong| count|
	  * +------+------+
	  * |  true|    83|
	  * | false|331810|
	  * +------+------+
	  *
	  */

	/**
	  * (4) What are the peak hours for long/short trips
	  * - This Question has been marked irrelevant (Long trips do not have enough presence in data set)
	  */

	// 4. What are the peak hours for long/short trips
	val pickupByHourAndLengthDF = tripsWithLengthDF
		.withColumn("hour", hour(col("tpep_pickup_datetime")))
		.groupBy("hour", "isLong")
		.agg(count("*").as("totalTrips"))
		.orderBy(col("totalTrips").desc_nulls_last)

	/**
	  * pickupByHourAndLengthDF.show(48)
	  *
	  * +----+------+----------+
	  * |hour|isLong|totalTrips|
	  * +----+------+----------+
	  * |  17| false|     22119|
	  * |  18| false|     21589|
	  * |  20| false|     20874|
	  * |  19| false|     20314|
	  * |  21| false|     19525|
	  * |   7| false|     18862|
	  * |  16| false|     18662|
	  * |  14| false|     17840|
	  * |  13| false|     17478|
	  * |   8| false|     16834|
	  * |  15| false|     16155|
	  * |  12| false|     16077|
	  * |  11| false|     15998|
	  * |  10| false|     15561|
	  * |   6| false|     15441|
	  * |   9| false|     15346|
	  * |  22| false|     14647|
	  * |   5| false|      8600|
	  * |  23| false|      7049|
	  * |   0| false|      3975|
	  * |   4| false|      3133|
	  * |   1| false|      2536|
	  * |   2| false|      1609|
	  * |   3| false|      1586|
	  * |  20|  true|        10|
	  * |  18|  true|         9|
	  * |   8|  true|         6|
	  * |  15|  true|         5|
	  * |  22|  true|         5|
	  * |   7|  true|         5|
	  * |  13|  true|         5|
	  * |  12|  true|         5|
	  * |   6|  true|         4|
	  * |  19|  true|         4|
	  * |  10|  true|         3|
	  * |  14|  true|         3|
	  * |   0|  true|         3|
	  * |  11|  true|         3|
	  * |  21|  true|         3|
	  * |   9|  true|         2|
	  * |  17|  true|         2|
	  * |  16|  true|         2|
	  * |   1|  true|         2|
	  * |   2|  true|         1|
	  * |  23|  true|         1|
	  * +----+------+----------+
	  *
	  */

	// 5. What are the top 3 pickup/drop-off zones for long/short trips?
	def pickUpDropOffPopularityDF(predicate: Column): Dataset[Row] = tripsWithLengthDF
		.where(predicate)
		.groupBy("PULocationID", "DOLocationID")
		.agg(count("*").as("totalTrips"))
		.join(taxiZonesDF, col("PULocationID") === col("LocationID"))
		.withColumnRenamed("Zone", "Pickup_Zone")
		.drop("LocationID", "Borough", "service_zone")
		.join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
		.withColumnRenamed("Zone", "Dropoff_Zone")
		.drop("LocationID", "Borough", "service_zone")
		.drop("PULocationID", "DOLocationID")
		.orderBy(col("totalTrips").desc_nulls_last)


	/**
	  * pickUpDropOffPopularityDF(not(col("isLong"))).show(false) \\ short trips
	  *
	  * +----------+----------------------------+----------------------------+
	  * |totalTrips|Pickup_Zone                 |Dropoff_Zone                |
	  * +----------+----------------------------+----------------------------+
	  * |5558      |NV                          |NV                          |
	  * |2425      |Upper East Side South       |Upper East Side North       |
	  * |1962      |Upper East Side North       |Upper East Side South       |
	  * |1944      |Upper East Side North       |Upper East Side North       |
	  * |1928      |Upper East Side South       |Upper East Side South       |
	  * |1052      |Upper East Side South       |Midtown Center              |
	  * |1012      |Upper East Side South       |Midtown East                |
	  * |987       |Midtown Center              |Upper East Side South       |
	  * |965       |Upper West Side South       |Upper West Side North       |
	  * |882       |Midtown Center              |Midtown Center              |
	  * |865       |Lenox Hill West             |Upper East Side North       |
	  * |850       |Penn Station/Madison Sq West|Midtown Center              |
	  * |828       |Upper West Side North       |Upper West Side South       |
	  * |824       |Upper West Side South       |Lincoln Square East         |
	  * |809       |Lincoln Square East         |Upper West Side South       |
	  * |808       |Lenox Hill West             |Upper East Side South       |
	  * |797       |Midtown East                |Murray Hill                 |
	  * |784       |Upper East Side South       |Lenox Hill West             |
	  * |763       |Yorkville West              |Upper East Side North       |
	  * |757       |Times Sq/Theatre District   |Penn Station/Madison Sq West|
	  * +----------+----------------------------+----------------------------+
	  *
	  */

	/**
	  * pickUpDropOffPopularityDF(col("isLong")).show(false)
	  *
	  * +----------+----------------------------+-----------------------------------+
	  * |totalTrips|Pickup_Zone                 |Dropoff_Zone                       |
	  * +----------+----------------------------+-----------------------------------+
	  * |14        |JFK Airport                 |NA                                 |
	  * |8         |LaGuardia Airport           |NA                                 |
	  * |4         |JFK Airport                 |JFK Airport                        |
	  * |4         |JFK Airport                 |Newark Airport                     |
	  * |3         |Midtown South               |NA                                 |
	  * |3         |NV                          |NV                                 |
	  * |2         |LaGuardia Airport           |Newark Airport                     |
	  * |2         |Clinton East                |NA                                 |
	  * |2         |JFK Airport                 |Riverdale/North Riverdale/Fieldston|
	  * |2         |Midtown North               |Newark Airport                     |
	  * |2         |Penn Station/Madison Sq West|NA                                 |
	  * |1         |LaGuardia Airport           |Bay Ridge                          |
	  * |1         |Flushing                    |NA                                 |
	  * |1         |JFK Airport                 |Van Nest/Morris Park               |
	  * |1         |JFK Airport                 |Times Sq/Theatre District          |
	  * |1         |JFK Airport                 |Fort Greene                        |
	  * |1         |JFK Airport                 |Williamsburg (North Side)          |
	  * |1         |Midtown Center              |NA                                 |
	  * |1         |JFK Airport                 |Eltingville/Annadale/Prince's Bay  |
	  * |1         |Midtown Center              |Charleston/Tottenville             |
	  * +----------+----------------------------+-----------------------------------+
	  *
	  *
	  * [proposal]
	  * - clear separation of long/short trips
	  * - short trips in b/w wealthy zones (bars, restaurants)
	  * - Long trips mostly used for airport transit
	  *
	  * - to the nyc town hall: airport rapid transit
	  * - separate market segments and tailor services to each
	  * - ??Partnerships with bars/restaurants for pickup services??
	  */

	// 6. How are people paying for the ride, on long/short trips?
	val rateCodeDistributionDF = taxiDF
		.groupBy(col("RateCodeID"))
		.agg(count("*").as("totalTrips"))
		.orderBy(col("totalTrips").desc_nulls_last)


	/**
	  * rateCodeDistributionDF.show
	  *
	  * +----------+----------+
	  * |RateCodeID|totalTrips|
	  * +----------+----------+
	  * |         1|    324387| => credit Card
	  * |         2|      5878| => cash
	  * |         5|       895| => unknown
	  * |         3|       530| => no charge
	  * |         4|       193| => dispute
	  * |        99|         7| => ???
	  * |         6|         3| => voided
	  * +----------+----------+
	  *
	  * [conclusion:]
	  * . cash is dead
	  * . warn: downtime of credit card system is loss of income
	  */


	/**
	  * 7. Tracking the Progress|Evolution of each rate code by single day
	  *
	  * [&nb:]
	  * TaxiDF dataset is only a single day(|2018-01-24) so 🌚
	  */
	val rateCodeEvolutionDF = taxiDF
		.groupBy(
			to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RateCodeID")
		)
		.agg(count("*").as("totalTrips"))
		.orderBy(col("pickup_day"))


	/**
	  * +----------+----------+----------+
	  * |pickup_day|RateCodeID|totalTrips|
	  * +----------+----------+----------+
	  * |2018-01-24|         5|        53|
	  * |2018-01-24|         1|      6842|
	  * |2018-01-24|         6|         1|
	  * |2018-01-24|         2|       144|
	  * |2018-01-24|         3|         4|
	  * |2018-01-24|         4|         6|
	  * |2018-01-25|        99|         7|
	  * |2018-01-25|         3|       526|
	  * |2018-01-25|         2|      5734|
	  * |2018-01-25|         1|    317545|
	  * |2018-01-25|         4|       187|
	  * |2018-01-25|         6|         2|
	  * |2018-01-25|         5|       842|
	  * +----------+----------+----------+
	  */

	/**
	  * [8]
	  * Taxi trips that could theoretically be grouped by 5min Id and location [Ride.Sharing]
	  * |=> group taxi rides starting in same pickup zones in 5 min window
	  */

	val passengerCountDF = taxiDF.where(col("passenger_count") < 3).select(count("*"))
	// passengerCountDF.show
	// taxiDF.select(count("*")).show


	val groupedAttempts5MinRangeDF = taxiDF.
		select(
			round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinID"), // => define 5 min bucketID
			col("PULocationID"), col("total_amount")
		)
		.where(col("passenger_count") < 3) // => passenger limit \\ can only use ride share on cab rides with @most 2 passengers
		.groupBy(col("fiveMinID"), col("PULocationID"))
		.agg(
			count("*").as("total_trips"),
			sum(col("total_amount")).as("total_amount")
		)
		.withColumn("approximate_datetime", from_unixtime(col("fiveMinID") * 300)) // => human readable 5MinID
		.drop("fiveMinID")
		.join(taxiZonesDF, col("PULocationID") === col("LocationID"))
		.drop("LocationID", "service_zone")
		.orderBy(col("total_trips").desc_nulls_last)

	/**
	  * groupedAttempts5MinRangeDF.show(false)
	  *
	  * +------------+-----------+------------------+--------------------+---------+---------------------+
	  * |PULocationID|total_trips|total_amount      |approximate_datetime|Borough  |Zone                 |
	  * +------------+-----------+------------------+--------------------+---------+---------------------+
	  * |237         |115        |1376.199999999999 |2018-01-25 14:15:00 |Manhattan|Upper East Side South|
	  * |236         |110        |1308.1399999999985|2018-01-25 07:05:00 |Manhattan|Upper East Side North|
	  * |236         |105        |1128.3999999999992|2018-01-25 14:05:00 |Manhattan|Upper East Side North|
	  * |237         |104        |1164.9699999999991|2018-01-25 13:40:00 |Manhattan|Upper East Side South|
	  * |142         |103        |1393.9899999999984|2018-01-25 21:10:00 |Manhattan|Lincoln Square East  |
	  * |142         |102        |1410.8599999999985|2018-01-25 21:05:00 |Manhattan|Lincoln Square East  |
	  * |236         |101        |1087.0899999999988|2018-01-25 14:00:00 |Manhattan|Upper East Side North|
	  * |237         |100        |1215.0499999999988|2018-01-25 17:25:00 |Manhattan|Upper East Side South|
	  * |142         |99         |1372.2099999999987|2018-01-25 20:35:00 |Manhattan|Lincoln Square East  |
	  * |162         |99         |1615.1199999999983|2018-01-25 18:05:00 |Manhattan|Midtown East         |
	  * |237         |99         |1224.8099999999993|2018-01-25 18:40:00 |Manhattan|Upper East Side South|
	  * |161         |97         |1429.0299999999986|2018-01-25 18:40:00 |Manhattan|Midtown Center       |
	  * |161         |97         |1352.659999999999 |2018-01-25 19:35:00 |Manhattan|Midtown Center       |
	  * |237         |96         |1146.6399999999994|2018-01-25 18:15:00 |Manhattan|Upper East Side South|
	  * |237         |96         |1108.739999999999 |2018-01-25 14:10:00 |Manhattan|Upper East Side South|
	  * |161         |96         |1428.9899999999993|2018-01-25 19:05:00 |Manhattan|Midtown Center       |
	  * |236         |95         |1333.379999999999 |2018-01-25 07:10:00 |Manhattan|Upper East Side North|
	  * |161         |95         |1310.079999999999 |2018-01-25 18:45:00 |Manhattan|Midtown Center       |
	  * |236         |95         |1251.4599999999998|2018-01-25 07:15:00 |Manhattan|Upper East Side North|
	  * |236         |94         |1056.7099999999991|2018-01-25 14:20:00 |Manhattan|Upper East Side North|
	  * +------------+-----------+------------------+--------------------+---------+---------------------+
	  *
	  * [proposal]:
	  * - incentive on grouped ride @ discount
	  * - lower costs
	  * - more competitive with lower prices
	  * - fewer emissions - can ask for subsidy on the project
	  */


	/**
	  * [model for estimating potential economic impact over the dataset]
	  *
	  * 5% of taxi trips detected to be group-able @any time
	  * 30% of people actually accept to be grouped
	  * 5$ discount on grouped rides
	  * $2 extra to take an individual ride
	  * if 2 rides are grouped, reducing cost(maintenance, etc.) by 60% of one average ride \\ avgCostReduction
	  */
	val percentGroupAttempt = 0.05 // 5% of taxi trips detected to be group-able @any time
	val percentAcceptGrouping = 0.3 // 30% of people actually accept to be grouped
	val discount = 5 // 5$ discount on grouped rides
	val extraCost = 2 // $2 extra to take an individual ride

	val taxiTotalAmountAvgDF = taxiDF.select(avg(col("total_amount")))
	/**
	  * taxiTotalAmountAvgDF.show
	  * +------------------+
	  * | avg(total_amount)|
	  * +------------------+
	  * |15.896471422975342|
	  * +------------------+
	  */

	val avgCostReduction = 0.6 * taxiTotalAmountAvgDF.as[Double].take(1)(0)

	val groupingEstimatedEconomicImpactDF = groupedAttempts5MinRangeDF
		.withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
		.withColumn(
			"acceptedGroupedRidesEconomicImpact",
			col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount) // economic impart \\ profit for accepted grouped rides
		)
		.withColumn("rejectedColumnRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
		.withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedColumnRidesEconomicImpact"))


	/**
	  * groupingEstimatedEconomicImpactDF.show(false)
	  *
	  * +------------+-----------+------------------+--------------------+---------+---------------------+------------------+----------------------------------+---------------------------------+------------------+
	  * |PULocationID|total_trips|total_amount      |approximate_datetime|Borough  |Zone                 |groupedRides      |acceptedGroupedRidesEconomicImpact|rejectedColumnRidesEconomicImpact|totalImpact       |
	  * +------------+-----------+------------------+--------------------+---------+---------------------+------------------+----------------------------------+---------------------------------+------------------+
	  * |237         |115        |1376.199999999999 |2018-01-25 14:15:00 |Manhattan|Upper East Side South|5.75              |7.827847922779476                 |8.049999999999999                |15.877847922779475| => 15$ every five minutes
	  * |236         |110        |1308.1399999999985|2018-01-25 07:05:00 |Manhattan|Upper East Side North|5.5               |7.487506708745586                 |7.699999999999999                |15.187506708745586|
	  * |236         |105        |1128.3999999999992|2018-01-25 14:05:00 |Manhattan|Upper East Side North|5.25              |7.147165494711696                 |7.35                             |14.497165494711695|
	  * |237         |104        |1164.9699999999991|2018-01-25 13:40:00 |Manhattan|Upper East Side South|5.2               |7.079097251904918                 |7.279999999999999                |14.359097251904917|
	  * |142         |103        |1393.9899999999984|2018-01-25 21:10:00 |Manhattan|Lincoln Square East  |5.15              |7.011029009098141                 |7.21                             |14.22102900909814 |
	  * |142         |102        |1410.8599999999985|2018-01-25 21:05:00 |Manhattan|Lincoln Square East  |5.1000000000000005|6.942960766291362                 |7.140000000000001                |14.082960766291363|
	  * |236         |101        |1087.0899999999988|2018-01-25 14:00:00 |Manhattan|Upper East Side North|5.050000000000001 |6.874892523484585                 |7.07                             |13.944892523484585|
	  * |237         |100        |1215.0499999999988|2018-01-25 17:25:00 |Manhattan|Upper East Side South|5.0               |6.806824280677806                 |7.0                              |13.806824280677805|
	  * |142         |99         |1372.2099999999987|2018-01-25 20:35:00 |Manhattan|Lincoln Square East  |4.95              |6.7387560378710285                |6.93                             |13.668756037871027|
	  * |162         |99         |1615.1199999999983|2018-01-25 18:05:00 |Manhattan|Midtown East         |4.95              |6.7387560378710285                |6.93                             |13.668756037871027|
	  * |237         |99         |1224.8099999999993|2018-01-25 18:40:00 |Manhattan|Upper East Side South|4.95              |6.7387560378710285                |6.93                             |13.668756037871027|
	  * |161         |97         |1429.0299999999986|2018-01-25 18:40:00 |Manhattan|Midtown Center       |4.8500000000000005|6.602619552257472                 |6.79                             |13.392619552257472|
	  * |161         |97         |1352.659999999999 |2018-01-25 19:35:00 |Manhattan|Midtown Center       |4.8500000000000005|6.602619552257472                 |6.79                             |13.392619552257472|
	  * |237         |96         |1146.6399999999994|2018-01-25 18:15:00 |Manhattan|Upper East Side South|4.800000000000001 |6.534551309450695                 |6.720000000000001                |13.254551309450695|
	  * |237         |96         |1108.739999999999 |2018-01-25 14:10:00 |Manhattan|Upper East Side South|4.800000000000001 |6.534551309450695                 |6.720000000000001                |13.254551309450695|
	  * |161         |96         |1428.9899999999993|2018-01-25 19:05:00 |Manhattan|Midtown Center       |4.800000000000001 |6.534551309450695                 |6.720000000000001                |13.254551309450695|
	  * |236         |95         |1333.379999999999 |2018-01-25 07:10:00 |Manhattan|Upper East Side North|4.75              |6.466483066643916                 |6.6499999999999995               |13.116483066643916|
	  * |161         |95         |1310.079999999999 |2018-01-25 18:45:00 |Manhattan|Midtown Center       |4.75              |6.466483066643916                 |6.6499999999999995               |13.116483066643916|
	  * |236         |95         |1251.4599999999998|2018-01-25 07:15:00 |Manhattan|Upper East Side North|4.75              |6.466483066643916                 |6.6499999999999995               |13.116483066643916|
	  * |236         |94         |1056.7099999999991|2018-01-25 14:20:00 |Manhattan|Upper East Side North|4.7               |6.398414823837137                 |6.58                             |12.978414823837138|
	  * +------------+-----------+------------------+--------------------+---------+---------------------+------------------+----------------------------------+---------------------------------+------------------+
	  *
	  */

	/**
	  * Total profit eared on jan_25_2018
	  */

	val totalProfitDF = groupingEstimatedEconomicImpactDF.select(sum(col("totalImpact")).as("total"))

	/**
	  * totalProfitDF.show
	  *
	  * +-----------------+
	  * |            total|
	  * +-----------------+
	  * |39987.73868642742|
	  * +-----------------+
	  *
	  * |=> ~ 40k$/day -> 12 million/year
	  */


}

