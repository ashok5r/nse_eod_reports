from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from pyspark.sql.functions import col,date_format,to_date
from datetime import datetime,date
from pyspark.sql import functions as F
from pyspark.sql.functions import upper


spark = SparkSession\
    .builder\
        .master('local[1]')\
        .appName('nse_sec_eod_reports.py')\
            .getOrCreate()

""""" 
# If we want to add header manually use this syntax

sec_behave_data  = StructType()\
    .add("SYMBOL",StringType(),True)\
         .add("SERIES",StringType(),True)\
             .add("DATE1",StringType(),True)\
                 .add("PREV_CLOSE",DoubleType(),True)\
                     .add("OPEN_PRICE",StringType(),True)\
                         .add("HIGH_PRICE",StringType(),True)\
                             .add("LOW_PRICE",StringType(),True)\
                                 .add("LAST_PRICE",DoubleType(),True)\
                                     .add("CLOSE_PRICE",DoubleType(),True)\
                                         .add("AVG_PRICE",DoubleType(),True)\
                                             .add("TTL_TRD_QNTY",StringType(),True)\
                                                 .add("TURNOVER_LACS",StringType(),True)\
                                                     .add("NO_OF_TRADES",IntegerType(),True)\
                                                         .add("DELIV_QTY",StringType(),True)\
                                                             .add("DELIV_PER",StringType(),True)          
"""""

# 1.To read csv file into dataframe variable

sec_behave_data = spark.read.format("csv").options(inferSchema = True, header=True).load("./in/20230109/sec_bhavdata_full_09012023.csv")
sec_behave_data = sec_behave_data.select([F.col(col).alias(col.replace(' ', '')) for col in sec_behave_data.columns])
sec_behave_data.printSchema()


"""""
# If we want to add header manually use this syntax

nse_indices = StructType()\
    .add('Index_Name',StringType(),True)\
        .add('Index_Date',StringType(),True)\
            .add('Open_Index_Value',StringType(),True)\
                .add('High_Index_Value',StringType(),True)\
                    .add('Low_Index_Value',StringType(),True)\
                        .add('Closing_Index_Value',DoubleType(),True)\
                            .add('Points_Change',DoubleType(),True)\
                                .add('Change(%)',StringType(),True)\
                                    .add('Volume',StringType(),True)\
                                        .add('Turnover_(Rs._Cr.',StringType(),True)\
                                            .add('P/E',StringType(),True)\
                                                .add('P/B',StringType(),True)\
                                                    .add('Div_Yield',StringType(),True)
"""""
                                                 
# 3. To read csv file into dataframe variable
nse_indices = spark.read.format('csv').options(inferSchema = True,header = True).load("./in/20230109/indices/ind_close_all_09012023.csv")

nse_indices.printSchema()


# 5.In file nse_indicies, rename column name "Index Name" to "SYMBOL"
print("renaming column index name to symbol")
nse_indices.withColumnRenamed("Index Name","SYMBOL").printSchema()


# 6.In file nse_indicies, add new column "SERIES" with default value "INDEX"
print("adding  a new  column series with default value index ")
nse_indices.withColumn("SERIES",lit('INDEX')).printSchema()

# 7.In file nse_indicies, add new column "DATE1" with value "Index Date" in the format 'dd-MM-yyyy' to 'dd-MMM-yyyy', example '09-01-2023' value to '09-Jan-2023'
print ("adding new column 'DATE1' with value 'Index Date' in the format 'dd-MM-yyyy' to 'dd-MMM-yyyy'")
nse_indices.withColumn("DATE1",col("Index Date",)).show()

nse_indices.select(date_format("Index Date","dd-MMM-yyyy").alias("DATE1")).show()

# 8.In file nse_indicies, rename column name "Open Index Value" to "OPEN_PRICE"
print("renaming column open index value to open price ")
nse_indices.withColumnRenamed("Open Index Value","OPEN PRICE").printSchema()

# 9.In file nse_indicies, rename column name "High Index Value" to "HIGH_PRICE"
print("renaming column high index value to high price ")
nse_indices.withColumnRenamed("High Index Value","HIGH PRICE").printSchema()

# 10.In file nse_indicies, rename column name "Low Index Value" to "LOW_PRICE"
print("renaming column low index value to low price")
nse_indices.withColumnRenamed("Low Index Value","LOW PRICE").printSchema()

# 11.In file nse_indicies, rename column name "Closing Index Value" to "CLOSE_PRICE"
print("renaming column closing index value to close price")
nse_indices.withColumnRenamed("Closing Index Value","CLOSE PRICE").printSchema()

# 12.In file nse_indicies, add new column "LAST_PRICE" with value "Closing Index Value"
print("adding a new column last price with value closing index value")
nse_indices.withColumn("LAST PRICE",col("Closing Index Value")).printSchema()
# nse_indices.withColumn("LAST_PRICE",nse_indices["Closing Index Value"]).show()

# 13.In file nse_indicies, add new column "AVG_PRICE" with value "Closing Index Value"
print("adding a new  column avg price with value closing index value ")
nse_indices.withColumn("AVG_PRICE",col("Closing Index Value")).show()
# nse_indices.withColumn("AVG_PRICE",nse_indices["Closing Index Value"]).show()

# 14.In file nse_indicies, rename column name "Volume" to "TTL_TRD_QNTY"
print("renaming a column volume to ttl trd qnty")
nse_indices.withColumnRenamed("Volume","TTL_TRD_QNTY").printSchema()

# 15.In file nse_indicies, add new column "TURNOVER_LACS" with value "Turnover (Rs. Cr.)" multiply by 10

nse_indices.withColumn("TURNOVER_LACS",col("`Turnover (Rs. Cr.)`")*10).show()

# 16.In file nse_indicies, add new column "NO_OF_TRADES" with default value 0
print("adding a new column no of trades")
nse_indices.withColumn("NO_OF_TrADES",lit(0)).printSchema()

# 17.In file nse_indicies, add new column "DELIV_QTY" with default value 0
print("adding a new column deliv qty")
nse_indices.withColumn("DELIV_QTY",lit(0)).printSchema()

# 18.In file nse_indicies, add new column "DELIV_PER" with default value 0
print("adding a new column deliv per")
nse_indices.withColumn("DELIV_PER",lit(0)).printSchema()


# 19.In file nse_indicies, add new column "PREV_CLOSE" with value "Closing Index Value" - "Points Change" (substract 'Points Change' from 'Closing Index Value')
print("adding a new column prev close")
nse_indices.withColumn("PREV_CLOSE",col("Closing Index Value") - col("Points Change")).printSchema()

# 20.Into new dataframe variable nse_indices_new, from nse_indicies -> select SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER
print("adding a new dataframe variable")
df = nse_indices.createOrReplaceTempView("nse_indices")

nse_indices_new = spark.sql("select * from nse_indices")
nse_indices_new.printSchema()

# 21.Union By Name dataframe sec_behave_data and nse_indices_new as variable sec_behave_data_all, using -> sec_behave_data.unionByName(nse_indices_new)
print("merging two dataframes")

sec_behave_data_all = sec_behave_data.unionByName(nse_indices_new,allowMissingColumns=True)

# 22.print distinct SERIES from sec_behave_data_all, ie -> sec_behave_data_all.distinct.select("SERIES").show(false
sec_behave_data_all.distinct().select("SERIES").show()


# 23.save sec_behave_data_all to the location "./out/20230109" as csv with repartition to 1, sec_behave_data_all.repartition(1).format("csv").save("./out/20230109")

sec_behave_data_all.repartition(1).write.mode("overwrite").format("csv").save("./out/20230109")
