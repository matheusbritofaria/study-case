from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


spark = SparkSession.builder.appName("DesafioEngenheiroQ3").getOrCreate()

df = spark.read.option("multiline", "true").json("data.json")

expanded_column = df.withColumn("ItemList", explode("ItemList")).select(
    "*", "ItemList.*"
)
expanded_column.show(vertical=True, truncate=False)

receipt = df.drop("ItemList")
receipt.show(vertical=True, truncate=False)

item_list = df.withColumn("ItemList", explode("ItemList")).select("NFeID", "ItemList.*")
item_list.show(vertical=True, truncate=False)
