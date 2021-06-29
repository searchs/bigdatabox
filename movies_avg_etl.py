import pyspark

spark = (
    pyspark.sql.SparkSession.builder.appName("FromDatabase")
    .config("spark.driver.extraClassPath", "<driver_location>/postgresql-42.2.18.jar")
    .getOrCreate()
)


# Read table from db using Spark JDBC
def extract_movies_to_df():
    movies_df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline")
        .option("dbtable", "movies")
        .option("user", "<username")
        .option("password", "<password>")
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return movies_df


# Read users table from db using Spark JDBC


def extract_users_to_df():
    users_df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline")
        .option("dbtable", "users")
        .option("user", "<username")
        .option("password", "<password>")
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return users_df


# transforming tables
def transform_avg_ratings(movies_df, users_df):
    avg_rating = users_df.groupby("movie_id").mean("rating")
    # join movies_df and avg_rating table on id
    df = movies_df.join(avg_rating, movies_df.id == avg_rating.movies_id)
    df = df.drop("movie_id")
    return df


# Write the result into avg_ratings table in db
def load_df_to_db(df):
    mode = "overwrite"
    url = "jdbc:postgresql://localhost:5432/etl_pipeline"
    spark.write()
    properties = {
        "user": "<username>",
        "password": "<password>",
        "driver": "org.postgresql.Driver",
    }
    df.write.jdbc(url=url, table="avg_ratings", mode=mode, properties=properties)


if __name__ == "__main__":
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    ratings_df = transform_avg_ratings(movies_df, users_df)
    load_df_to_db(ratings_df)
