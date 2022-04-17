# Cleaning-Data-with-PySpark

**Working with real world datasets (6 datasets `Dallas Council Votes / Dallas Council Voters / Flights - 2014 / Flights - 2015 / Flights - 2016 / Flights - 2017`), with missing fields, bizarre formatting, and orders of magnitude more data. Knowing what’s needed to prepare data processes using Python with Apache Spark. Practicing and Discover  terminologies, methods, and some best practices to create a performant, maintainable, and understandable data processing platform.**

________________________________________________
- Define a new schema using the StructType method.
- Define a StructField for name, age, and city. Each field should correspond to the correct datatype and not be nullable.
- The Name and City columns are StringType() and the Age column is an IntegerType().

``` 
# Import the pyspark.sql.types library
from pyspark.sql.types import *

# Define a new schema using the StructType method
people_schema = StructType([
  # Define a StructField for each field
  StructField('Name', StringType(), False),
  StructField('Age', IntegerType(), False),
  StructField('City', StringType(), False)
])
```
________________________________________________

- Define a new schema using the StructType method.
- Define a StructField for name, age, and city. Each field should correspond to the correct datatype and not be nullable.
- The Name and City columns are StringType() and the Age column is an IntegerType().

``` 
# Imports
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Load the CSV file
aa_dfw_df = spark.read.format('csv').options(Header=True).load('AA_DFW_2018.csv.gz')
# Add the airport column using the F.lower() method
aa_dfw_df = aa_dfw_df.withColumn('airport', F.lower(aa_dfw_df['Destination Airport']))
# Drop the Destination Airport column
aa_dfw_df = aa_dfw_df.drop(aa_dfw_df['Destination Airport'])

# Show the DataFrame
aa_dfw_df.show()
```
________________________________________________

- View the row count of df1 and df2.
- Combine df1 and df2 in a new DataFrame named df3 with the union method.
- Save df3 to a parquet file named AA_DFW_ALL.parquet.
- Read the AA_DFW_ALL.parquet file and show the count.

> The spark object and the df1 and df2 DataFrames have been setup for you.

``` 
# Imports
import pyspark.sql.functions as F
from pyspark.sql.types import *

# View the row count of df1 and df2
print("df1 Count: %d" % df1.count())
print("df2 Count: %d" % df2.count())

# Combine the DataFrames into one
df3 = df1.union(df2)

# Save the df3 DataFrame in Parquet format
df3.write.parquet('AA_DFW_ALL.parquet', mode='overwrite')

# Read the Parquet file into a new DataFrame and run a count
print(spark.read.parquet('AA_DFW_ALL.parquet').count())
```
________________________________________________
- Import the AA_DFW_ALL.parquet file into flights_df.
- Use the createOrReplaceTempView method to alias the flights table.
- Run a Spark SQL query against the flights table.

``` 
# Imports
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Read the Parquet file into flights_df
flights_df = spark.read.parquet('AA_DFW_ALL.parquet')

# Register the temp table
flights_df.createOrReplaceTempView('flights')

# Run a SQL query of the average flight duration
avg_duration = spark.sql('SELECT avg(flight_duration) from flights').collect()[0]
print('The average flight time is: %d' % avg_duration)
```
________________________________________________
