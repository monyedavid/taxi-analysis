import org.apache.spark.sql

/*
	Scala provides package objects as a convenient container shared across an entire package.
	Package objects can contain arbitrary definitions, not just variable and method definitions.
	For instance, they are frequently used to hold package-wide type aliases and implicit conversions.
	Package objects can even inherit Scala classes and traits.

	Each package is allowed to have one package object.
	Any definitions placed in a package object are considered members of the package itself.
 */

package object constants {
	val jdbcSource = "jdbc"
	val Driver = "org.postgresql.Driver"
	val rtjvmUrl = "jdbc:postgresql://localhost:5432/rtjvm" // [DRIVER:DATABASE]://[host:port]/databaseName
	val rtjvmUser = "docker"
	val rtjvmPassword = "docker"
	val localResourceData = "src/main/resources/data/"
	val localResource = "src/main/resources/"

	def ReadTable(tableName: String, sparkSession: sql.SparkSession): sql.DataFrame = sparkSession.read
		.format(jdbcSource)
		.option("driver", Driver)
		.option("url", rtjvmUrl) // [DRIVER:DATABASE]://[host:port]/databaseName
		.option("user", rtjvmUser)
		.option("password", rtjvmPassword)
		.option("dbtable", s"public.$tableName") // employees table || public . employees
		.load()

	def TransferTablesToSparkSQL(tableNames: List[String], sparkSession: sql.SparkSession, shouldWriteToWareHouse: Boolean = false): Unit = tableNames.foreach {
		tableName =>
			val tableDF = ReadTable(tableName, sparkSession)
			// - creates a spark alias for a table reference in spark-sql || Loading in memory
			tableDF.createOrReplaceTempView(tableName)

			if (shouldWriteToWareHouse)
			// write ->> from tableDF into table(named $tableName) under DB currently in use (default=default) in 'spark.sql.warehouse.dir'
			tableDF.write
				.mode(sql.SaveMode.Overwrite)
				.saveAsTable(tableName)
	}


}

