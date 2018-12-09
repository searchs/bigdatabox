
//MySQL

val jdbcURI = "jdbc:mysql://HOST:PORT/DATABASE_NAME"

val tableName = "TABLE_NAME"
val connectionProperties = new java.util.Properties
connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
connectionProperties.setProperty("user", "USER_NAME") //read from config!
connectionProperties.setProperty("password", "USER_PASSWORD")//read from config!

val jdbcDF = sqlContext.read.jdbc(jdbcURI, tableName, connectionProperties)
jdbcDF.count

//Hive Tables
val hiveDF = sqlContext.read.table("sample_01")

//OR using the SparkSql
val hiveDFSql = sqlContext.sql("SELECT * FROM sample_01")


//Writing Data
//JSON Format
hiveDF.write.format("org.apache.spark.sql.json").save("user/root/json-output")
//Parquet Format
hiveDF.write.format("org.apache.spark.sql.parquet").partitionBy("field_name").save("user/root/parquet-output")
//Write into another table
hiveDF.write.saveAsTable("New_Table_Name")


val students = List(Students(1, "Jacobs", 21, "M"), Students(2, "Jane", 23, "F"),Students(3, "David", 19, "M"),Students(4, "Ade", 20, "M"),Students(5, "Nike", 22, "F"),Students(6, "Britney", 22,"F"))
