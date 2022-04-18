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

- Show the distinct VOTER_NAME entries.
- Filter voter_df where the VOTER_NAME is 1-20 characters in length.
- Filter out voter_df where the VOTER_NAME contains an _.
- Show the distinct VOTER_NAME entries again.
``` 
# Imports
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Show the distinct VOTER_NAME entries
voter_df.select('VOTER_NAME').distinct().show(40, truncate=False)

# Filter voter_df where the VOTER_NAME is 1-20 characters in length
voter_df = voter_df.filter('length(VOTER_NAME) > 0 and length(VOTER_NAME) < 20')

# Filter out voter_df where the VOTER_NAME contains an underscore
voter_df = voter_df.filter(~ F.col('VOTER_NAME').contains('_'))

# Show the distinct VOTER_NAME entries again
voter_df.select('VOTER_NAME').distinct().show(40, truncate=False)
```
________________________________________________

> your manager has asked you to create two new columns - first_name and last_name. She asks you to split the VOTER_NAME column into words on any space character. You'll treat the last word as the last_name, and the first word as the first_name.

- Add a new column called splits holding the list of possible names.
- Use the getItem() method and create a new column called first_name.
- Get the last entry of the splits list and create a column called last_name.
- Drop the splits column and show the new voter_df.

``` 
# Imports
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Add a new column called splits separated on whitespace
voter_df = voter_df.withColumn('splits', F.split(voter_df.VOTER_NAME, '\s+'))

# Create a new column called first_name based on the first item in splits
voter_df = voter_df.withColumn('first_name', voter_df.splits.getItem(0))

# Get the last entry of the splits list and create a column called last_name
voter_df = voter_df.withColumn('last_name', voter_df.splits.getItem(F.size('splits') - 1))

# Drop the splits column
voter_df = voter_df.drop('splits')

# Show the voter_df DataFrame
voter_df.show()
```
________________________________________________

- Add a column to voter_df named random_val with the results of the F.rand() method for any voter with the title Councilmember.
- Show some of the DataFrame rows, noting whether the .when() clause worked.

``` 
# Imports
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Add a column to voter_df for any voter with the title **Councilmember**
voter_df = voter_df.withColumn('random_val',
                               when(voter_df.TITLE == 'Councilmember', F.rand()))

# Show some of the DataFrame rows, noting whether the when clause worked
voter_df.show()
```
________________________________________________

- Add a column to voter_df named random_val with the results of the F.rand() method for any voter with the title Councilmember. Set random_val to 2 for the Mayor. Set any other title to the value 0.
- Show some of the Data Frame rows, noting whether the clauses worked.
- Use the .filter clause to find 0 in random_val.

``` 
# Imports
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Add a column to voter_df for a voter based on their position
voter_df = voter_df.withColumn('random_val',
                               when(voter_df.TITLE == 'Councilmember', F.rand())
                               .when(voter_df.TITLE=='Mayor', 2)
                               .otherwise(0))

# Show some of the DataFrame rows
voter_df.show()

# Use the .filter() clause with random_val
voter_df.filter(voter_df.random_val==0).show()
```
________________________________________________

- Edit the getFirstAndMiddle() function to return a space separated string of names, except the last entry in the names list.
- Define the function as a user-defined function. It should return a string type.
- Create a new column on voter_df called first_and_middle_name using your UDF.
- Show the Data Frame.

``` 
# Imports
import pyspark.sql.functions.udf as F
from pyspark.sql.types import *

def getFirstAndMiddle(names):
  # Return a space separated string of names
  return ' '.join(names)

# Define the method as a UDF
udfFirstAndMiddle = F.udf(getFirstAndMiddle, StringType())

# Create a new column using your UDF
voter_df = voter_df.withColumn('first_and_middle_name', udfFirstAndMiddle('splits'))

# Show the DataFrame
voter_df.show()
```
________________________________________________

- Select the unique entries from the column VOTER NAME and create a new DataFrame called voter_df.
- Count the rows in the voter_df DataFrame.
- Add a ROW_ID column using the appropriate Spark function.
- Show the rows with the 10 highest ROW_IDs.

``` 
# Imports
import pyspark.sql.functions.udf as F
from pyspark.sql.types import *

# Select all the unique council voters
voter_df = df.select(df["VOTER NAME"]).distinct()

# Count the rows in voter_df
print("\nThere are %d rows in the voter_df DataFrame.\n" % voter_df.count())

# Add a ROW_ID
voter_df = voter_df.withColumn('ROW_ID', F.monotonically_increasing_id())

# Show the rows with 10 highest IDs in the set
voter_df.orderBy(voter_df.ROW_ID.desc()).show(10)
```
________________________________________________

- Print the number of partitions on each DataFrame.
- Add a ROW_ID field to each DataFrame.
- Show the top 10 IDs in each DataFrame.

``` 
# Imports
import pyspark.sql.functions.udf as F
from pyspark.sql.types import *

# Print the number of partitions in each DataFrame
print("\nThere are %d partitions in the voter_df DataFrame.\n" % voter_df.rdd.getNumPartitions())
print("\nThere are %d partitions in the voter_df_single DataFrame.\n" % voter_df_single.rdd.getNumPartitions())

# Add a ROW_ID field to each DataFrame
voter_df = voter_df.withColumn('ROW_ID', F.monotonically_increasing_id())
voter_df_single = voter_df_single.withColumn('ROW_ID', F.monotonically_increasing_id())

# Show the top 10 IDs in each DataFrame 
voter_df.orderBy(voter_df.ROW_ID.desc()).show(10)
voter_df_single.orderBy(voter_df_single.ROW_ID.desc()).show(10)
```
________________________________________________

- Determine the highest ROW_ID in voter_df_march and save it in the variable previous_max_ID. The statement .rdd.max()[0] will get the maximum ID.
- Add a ROW_ID column to voter_df_april starting at the value of previous_max_ID.
- Show the ROW_ID's from both Data Frames and compare.

``` 
# Imports
import pyspark.sql.functions.udf as F
from pyspark.sql.types import *

# Determine the highest ROW_ID and save it in previous_max_ID
previous_max_ID = voter_df_march.select('ROW_ID').rdd.max()[0]

# Add a ROW_ID column to voter_df_april starting at the desired value
voter_df_april = voter_df_april.withColumn('ROW_ID', F.monotonically_increasing_id() + previous_max_ID)

# Show the ROW_ID from both DataFrames and compare
voter_df_march.select('ROW_ID').show()
voter_df_april.select('ROW_ID').show()
```
________________________________________________

- Cache the unique rows in the departures_df DataFrame.
- Perform a count query on departures_df, noting how long the operation takes.
- Count the rows again, noting the variance in time of a cached DataFrame.
- Check the caching status on the departures_df DataFrame.
- Remove the departures_df DataFrame from the cache.
- Validate the caching status again.
``` 
# Imports
import pyspark.sql.functions.udf as F
from pyspark.sql.types import *

start_time = time.time()

# Add caching to the unique rows in departures_df
departures_df = departures_df.distinct().cache()

# Count the unique rows in departures_df, noting how long the operation takes
print("Counting %d rows took %f seconds" % (departures_df.count(), time.time() - start_time))

# Count the rows again, noting the variance in time of a cached DataFrame
start_time = time.time()
print("Counting %d rows again took %f seconds" % (departures_df.count(), time.time() - start_time))

# Determine if departures_df is in the cache
print("Is departures_df cached?: %s" % departures_df.is_cached)
print("Removing departures_df from cache")

# Remove departures_df from the cache
departures_df.unpersist()

# Check the cache status again
print("Is departures_df cached?: %s" % departures_df.is_cached)
```
________________________________________________

### Importing

- Import the departures_full.txt.gz file and the departures_xxx.txt.gz files into separate DataFrames.
- Run a count on each DataFrame and compare the run times.
``` 
# Imports
import pyspark.sql.functions.udf as F
from pyspark.sql.types import *

# Import the full and split files into DataFrames
full_df = spark.read.csv('departures_full.txt.gz')
split_df = spark.read.csv('departures_0*.txt.gz')

# Print the count and run time for each DataFrame
start_time_a = time.time()
print("Total rows in full DataFrame:\t%d" % full_df.count())
print("Time to run: %f" % (time.time() - start_time_a))

start_time_b = time.time()
print("Total rows in split DataFrame:\t%d" % split_df.count())
print("Time to run: %f" % (time.time() - start_time_b))
```
________________________________________________

- Check the name of the Spark application instance ('spark.app.name').
- Determine the TCP port the driver runs on ('spark.driver.port').
- Determine how many partitions are configured for joins.
- Show the results.

``` 
# Imports
import pyspark.sql.functions.udf as F
from pyspark.sql.types import *

# Name of the Spark application instance
app_name = spark.conf.get('spark.app.name')

# Driver TCP port
driver_tcp_port = spark.conf.get('spark.driver.port')

# Number of join partitions
num_partitions = spark.conf.get('spark.sql.shuffle.partitions')

# Show the results
print("Name: %s" % app_name)
print("Driver TCP port: %s" % driver_tcp_port)
print("Number of partitions: %s" % num_partitions)

```
________________________________________________

- Store the number of partitions in departures_df in the variable before.
- Change the spark.sql.shuffle.partitions configuration to 500 partitions.
- Recreate the departures_df DataFrame reading the distinct rows from the departures file.
- Print the number of partitions from before and after the configuration change.

``` 
# Imports
import pyspark.sql.functions.udf as F
from pyspark.sql.types import *

# Store the number of partitions in variable
before = departures_df.rdd.getNumPartitions()

# Configure Spark to use 500 partitions
print('spark.sql.shuffle.partitions', spark.conf.set('spark.sql.shuffle.partitions',500))

# Recreate the DataFrame using the departures data file
departures_df = spark.read.csv('departures.txt.gz').distinct()
# Print the number of partitions for each instance
print("Partition count before change: %d" % before)
print("Partition count after change: %d" % departures_df.rdd.getNumPartitions())

```
________________________________________________
