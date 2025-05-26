%md
#### DATA READING
 
dbutils.fs.ls("/FileStore/tables/")
df_json = (
    spark.read.format("json")
    .option("interSchema", True)
    .option("header", True)
    .option("multiline", False)
    .load("/FileStore/tables/drivers.json")
)
df_json.display()
df = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("multiline", False)
    .load("/FileStore/tables/BigMart_Sales.csv")
)
df.display()
%md
#### Schema Definition

df.printSchema
%md
#### DDL SCHEMA

my_ddl_schema = """
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 
                    """
# here we have converted the data types
df = (
    spark.read.format("csv")
    .schema(my_ddl_schema)
    .option("header", True)
    .load("/FileStore/tables/BigMart_Sales.csv")
)
df.display()
from pyspark.sql.types import *
from pyspark.sql.functions import *
my_strct_schema = StructType(
    [
        StructField("Item_Identifier", StringType(), True),
        StructField("Item_Weight", StringType(), True),
        StructField("Item_Fat_Content", StringType(), True),
        StructField("Item_Visibility", StringType(), True),
        StructField("Item_Type", StringType(), True),
        StructField("Item_MRP", StringType(), True),
        StructField("Outlet_Identifier", StringType(), True),
        StructField("Outlet_Establishment_Year", StringType(), True),
        StructField("Outlet_Size", StringType(), True),
        StructField("Outlet_Location_Type", StringType(), True),
        StructField("Outlet_Type", StringType(), True),
        StructField("Item_Outlet_Sales", StringType(), True),
    ]
)
df = (
    spark.read.format("csv")
    .schema(my_strct_schema)
    .option("header", True)
    .load("/FileStore/tables/BigMart_Sales.csv")
)
df.printSchema()
%md
#### TRANSFORMATIONS

%md
##### SELECT
df.display()
df.select(col("Item_Identifier"), col("Item_Weight"), col("Item_Fat_Content")).display()
%md
##### ALIAS
df.select(col("Item_Identifier").alias("Item_ID")).display()
%md
##### FILTER
%md
###### Scenario-1
df.filter(col("Item_Fat_Content") == "Regular").display()
df.filter((col("Item_Type") == "Soft Drinks") & (col("Item_Weight") < 10)).display()
%md
###### Scenario 3

df.filter(
    (col("Outlet_Size").isNull())
    & (col("Outlet_Location_Type").isin("Tier 1", "Tier 2"))
).display()
df.withColumnRenamed("Item_Weight", "Item_Wt").display()
df = df.withColumn("flag", lit("new"))
df.display()
df.withColumn("multiply", col("Item_Weight") * col("Item_MRP")).display()
df = df.withColumn(
    "Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "Regular", "Reg")
).withColumn(
    "Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "Low Fat", "Lf")
)
df.display()
%md
##### Type Casting
df = df.withColumn("Item_Weight", col("Item_Weight").cast(StringType()))
df.sort(col("Item_Weight").desc()).display()
df.sort(col("Item_visibility").asc()).display()
df.sort(["Item_Weight", "Item_Visibility"], ascending=[0, 0]).display()
df.sort(["Item_Weight", "Item_Visibility"], ascending=[0, 1]).display()
%md
#### LIMIT

df.limit(10).display()
%md
#### DROP
df.drop("Item_Visibility").display()
%md
##### DROP_DUPLICATES
df.dropDuplicates().display()
df.drop_duplicates(subset=["Item_Type"]).display()
df.distinct().display()
%md
#### UNION AND UNION BY NAME
%md
##### Preparing Dataframes
data1 = [("1", "sam"), ("2", "smith")]
schema1 = "id STRING,name STRING"
df1 = spark.createDataFrame(data1, schema1)

data2 = [("3", "roshan"), ("4", "romi")]
schema2 = "id STRING,name STRING"
df2 = spark.createDataFrame(data2, schema2)
df1.display()
df2.display()
df1.union(df2).display()
data1 = [("sam", "1"), ("smith", "2")]
schema1 = "name STRING,id STRING"
df1 = spark.createDataFrame(data1, schema1)

data2 = [("3", "roshan"), ("4", "romi")]
schema2 = "id STRING,name STRING"
df2 = spark.createDataFrame(data2, schema2)
df1.unionByName(df2).display()
%md
#### String Functions
%md
##### Initcap()
df.select(upper("Item_Type").alias("upper_Item_Type")).display()
%md
#### Data Functions

%md
##### Current_Date
df = df.withColumn("curr_date", current_date()).display()
