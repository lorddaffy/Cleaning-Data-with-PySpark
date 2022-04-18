[AA_DFW_2017_Departures_Short.csv.gz](https://github.com/lorddaffy/Cleaning-Data-with-PySpark/files/8507487/AA_DFW_2017_Departures_Short.csv.gz)
[AA_DFW_2016_Departures_Short.csv.gz](https://github.com/lorddaffy/Cleaning-Data-with-PySpark/files/8507488/AA_DFW_2016_Departures_Short.csv.gz)
[AA_DFW_2015_Departures_Short.csv.gz](https://github.com/lorddaffy/Cleaning-Data-with-PySpark/files/8507489/AA_DFW_2015_Departures_Short.csv.gz)
[AA_DFW_2014_Departures_Short.csv.gz](https://github.com/lorddaffy/Cleaning-Data-with-PySpark/files/8507490/AA_DFW_2014_Departures_Short.csv.gz)
[DallasCouncilVoters.csv.gz](https://github.com/lorddaffy/Cleaning-Data-with-PySpark/files/8507491/DallasCouncilVoters.csv.gz)
[DallasCouncilVotes.csv.gz](https://github.com/lorddaffy/Cleaning-Data-with-PySpark/files/8507492/DallasCouncilVotes.csv.gz)
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

- Create a new DataFrame normal_df by joining flights_df with airports_df.
- Determine which type of join is used in the query plan.
``` 
# Imports
import pyspark.sql.functions

# Join the flights_df and aiports_df DataFrames
normal_df = flights_df.join(airports_df, flights_df["Destination Airport"] == airports_df["IATA"])    

# Show the query plan
normal_df.explain()

```
________________________________________________

- Import the broadcast() method from pyspark.sql.functions.
- Create a new DataFrame broadcast_df by joining flights_df with airports_df, using the broadcasting.
- Show the query plan and consider differences from the original.

``` 
# Import the broadcast method from pyspark.sql.functions
from pyspark.sql.functions import broadcast

# Join the flights_df and airports_df DataFrames using broadcasting
broadcast_df = flights_df.join(broadcast(airports_df), \
    flights_df["Destination Airport"] == airports_df["IATA"] )

# Show the query plan and compare against the original
broadcast_df.explain()

```
________________________________________________

- Import the file 2015-departures.csv.gz to a DataFrame. Note the header is already defined.
- Filter the DataFrame to contain only flights with a duration over 0 minutes. Use the index of the column, not the column name (remember to use .printSchema() to see the column names / order).
- Add an ID column.
- Write the file out as a JSON document named output.json.

``` 
# Import the broadcast method from pyspark.sql.functions
import pyspark.sql.functions as F

# Import the data to a DataFrame
departures_df = spark.read.csv('2015-departures.csv.gz', header=True)

# Remove any duration of 0
departures_df = departures_df.filter(departures_df[3]> 0)

# Add an ID column
departures_df = departures_df.withColumn('id', F.monotonically_increasing_id())

# Write the file out to JSON format
departures_df.write.json('output.json', mode='overwrite')

```
________________________________________________

- Import the annotations.csv.gz file to a DataFrame and perform a row count. Specify a separator character of |.
- Query the data for the number of rows beginning with #.
- Import the file again to a new DataFrame, but specify the comment character in the options to remove any commented rows.
- Count the new DataFrame and verify the difference is as expected.

``` 
# Import the broadcast method from pyspark.sql.functions
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Import the file to a DataFrame and perform a row count
annotations_df = spark.read.csv('annotations.csv.gz', sep='|')
full_count = annotations_df.count()

# Count the number of rows beginning with '#'
comment_count = annotations_df.filter(annotations_df._c0.startswith('#')).count()

# Import the file to a new DataFrame, without commented rows
no_comments_df = spark.read.csv('annotations.csv.gz', sep='|', comment='#')

# Count the new DataFrame and verify the difference is as expected
no_comments_count = no_comments_df.count()
print("Full count: %d\nComment count: %d\nRemaining count: %d" % (full_count, comment_count, no_comments_count))

```
________________________________________________

- Create a new variable tmp_fields using the annotations_df DataFrame column '_c0' splitting it on the tab character.
- Create a new column in annotations_df named 'colcount' representing the number of fields defined in the previous step.
- Filter out any rows from annotations_df containing fewer than 5 fields.
- Count the number of rows in the DataFrame and compare to the initial_count.

``` 
# Import the broadcast method from pyspark.sql.functions
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Split _c0 on the tab character and store the list in a variable
tmp_fields = F.split(annotations_df['_c0'], '\t')

# Create the colcount column on the DataFrame
annotations_df = annotations_df.withColumn('colcount', F.size(tmp_fields))

# Remove any rows containing fewer than 5 fields
annotations_df_filtered = annotations_df.filter(~ (annotations_df["colcount"] < 5))

# Count the number of rows
final_count = annotations_df_filtered.count()
print("Initial count: %d\nFinal count: %d" % (initial_count, final_count))

```
________________________________________________

- Split the content of the '_c0' column on the tab character and store in a variable called split_cols.
- Add the following columns based on the first four entries in the variable above: folder, filename, width, height on a DataFrame named split_df.
- Add the split_cols variable as a column.

``` 
# Import the broadcast method from pyspark.sql.functions
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Split the content of _c0 on the tab character (aka, '\t')
split_cols = F.split(annotations_df["_c0"], '\t')

# Add the columns folder, filename, width, and height
split_df = annotations_df.withColumn('folder', split_cols.getItem(0))
split_df = split_df.withColumn('filename', split_cols.getItem(1))
split_df = split_df.withColumn('width', split_cols.getItem(2))
split_df = split_df.withColumn('height', split_cols.getItem(3))

# Add split_cols as a column
split_df = split_df.withColumn('split_cols', split_cols)

```
________________________________________________

- Rename the _c0 column to folder on the valid_folders_df DataFrame.
- Count the number of rows in split_df.
- Join the two DataFrames on the folder name, and call the resulting DataFrame joined_df. Make sure to broadcast the smaller DataFrame.
- Check the number of rows remaining in the DataFrame and compare.

``` 
# Import the broadcast method from pyspark.sql.functions
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Rename the column in valid_folders_df
valid_folders_df = valid_folders_df.withColumnRenamed('_c0', 'folder')

# Count the number of rows in split_df
split_count = split_df.count()

# Join the DataFrames
joined_df = split_df.join(F.broadcast(valid_folders_df), "folder")

# Compare the number of rows remaining
joined_count = joined_df.count()
print("Before: %d\nAfter: %d" % (split_count, joined_count))

```
________________________________________________

- Determine the row counts for each DataFrame.
- Create a DataFrame containing only the invalid rows.
- Validate the count of the new DataFrame is as expected.
- Determine the number of distinct folder rows removed.

``` 
# Import the broadcast method from pyspark.sql.functions
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Determine the row counts for each DataFrame
split_count = split_df.count()
joined_count = joined_df.count()

# Create a DataFrame containing the invalid rows
invalid_df = split_df.join(F.broadcast(joined_df), 'folder', 'left_anti')

# Validate the count of the new DataFrame is as expected
invalid_count = invalid_df.count()
print(" split_df:\t%d\n joined_df:\t%d\n invalid_df: \t%d" % (split_count, joined_count, invalid_count))

# Determine the number of distinct folder rows removed
invalid_folder_count = invalid_df.select('folder').distinct().count()
print("%d distinct invalid folders found" % invalid_folder_count)

```
________________________________________________

- Select the column representing the dog details from the DataFrame and show the first 10 un-truncated rows.
- Create a new schema as you've done before, using breed, start_x, start_y, end_x, and end_y as the names. Make sure to specify the proper data types for each field in the schema (any number value is an integer).
``` 
# Import the broadcast method from pyspark.sql.functions
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Select the dog details and show 10 untruncated rows
print(joined_df.select('dog_list').show(10, truncate=False))

# Define a schema type for the details in the dog list
DogType = StructType([
	StructField("breed", StringType(), False),
    StructField("start_x", IntegerType(), False),
    StructField("start_y", IntegerType(), False),
    StructField("end_x", IntegerType(), False),
    StructField("end_y", IntegerType(), False)
])

```
________________________________________________

- Create a Python function to split each entry in dog_list to its appropriate parts. Make sure to convert any strings into the appropriate types or the DogType will not parse correctly.
- Create a UDF using the above function.
- Use the UDF to create a new column called dogs. Drop the previous column in the same command.
- Show the number of dogs in the new column for the first 10 rows.

``` 
# Import the broadcast method from pyspark.sql.functions
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Create a function to return the number and type of dogs as a tuple
def dogParse(doglist):
  dogs = []
  for dog in doglist:
    (breed, start_x, start_y, end_x, end_y) = dog.split(',')
    dogs.append((breed, int(start_x), int(start_y), int(end_x), int(end_y)))
  return dogs

# Create a UDF
udfDogParse = F.udf(dogParse, ArrayType(DogType))

# Use the UDF to list of dogs and drop the old column
joined_df = joined_df.withColumn('dogs', udfDogParse('dog_list')).drop('dog_list')

# Show the number of dogs in the first 10 rows
joined_df.select(F.size('dogs')).show(10)

```
________________________________________________

- Define a Python function to take a list of tuples (the dog objects) and calculate the total number of "dog" pixels per image.
- Create a UDF of the function and use it to create a new column called 'dog_pixels' on the DataFrame.
- Create another column, 'dog_percent', representing the percentage of 'dog_pixels' in the image. Make sure this is between 0-100%. Use the string name of the column alone (ie, "columnname" rather than df.columnname).
- Show the first 10 rows with more than 60% 'dog_pixels' in the image. 

``` 
# Import the broadcast method from pyspark.sql.functions
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Define a UDF to determine the number of pixels per image
def dogPixelCount(doglist):
  totalpixels = 0
  for dog in doglist:
    totalpixels += (dog[3] - dog[1]) * (dog[4] - dog[2])
  return totalpixels

# Define a UDF for the pixel count
udfDogPixelCount = F.udf(dogPixelCount, IntegerType())
joined_df = joined_df.withColumn('dog_pixels', udfDogPixelCount('dogs'))

# Create a column representing the percentage of pixels
joined_df = joined_df.withColumn('dog_percent', (joined_df.dog_pixels / (joined_df.width * joined_df.height)) * 100)

# Show the first 10 annotations with more than 60% dog
joined_df.filter('dog_percent > 60').show(10)

```
________________________________________________
