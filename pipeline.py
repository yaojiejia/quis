from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

cols = ["id","name","city","country_code","position","about","posts","groups","current_company","experience","url","people_also_viewed","educations_details","education","recommendations_count","avatar","courses","languages","certifications","recommendations","volunteer_experience","followers","connections","current_company_company_id","current_company_name","publications","patents","projects","organizations","location","input_url","linkedin_id","activity","linkedin_num_id","banner_image","honors_and_awards","similar_profiles","default_avatar","memorialized_account","bio_links"]
schema = StructType([StructField(c, StringType(), True) for c in cols])

spark = (
    SparkSession.builder
    .appName("csv_to_parquet_stable")
    .config("spark.sql.session.timeZone","UTC")
    .config("spark.driver.memory","8g")
    .config("spark.driver.maxResultSize","2g")
    .config("spark.sql.shuffle.partitions","200")
    .config("spark.hadoop.parquet.enable.dictionary","false")
    .getOrCreate()
)

spark.conf.set("spark.sql.files.maxRecordsPerFile","800000")
spark.conf.set("spark.sql.parquet.compression.codec","snappy")

inp = "rawdata/quis_data.csv"
outp = "parquet"

df = (
    spark.read.format("csv")
    .option("header",True)
    .option("sep",",")
    .option("multiLine",True)
    .option("quote",'"')
    .option("escape",'"')
    .option("unescapedQuoteHandling","STOP_AT_CLOSING_QUOTE")
    .option("ignoreLeadingWhiteSpace",True)
    .option("ignoreTrailingWhiteSpace",True)
    .schema(schema)
    .load(inp)
)

before = df.count()
df = df.dropDuplicates(["linkedin_id"])
after = df.count()
print(f"Total Rows Processed: {after}")

df = df.repartition(200)

(df.write
 .mode("overwrite")
 .option("parquet.enable.dictionary","false")
 .parquet(outp))
