#!/usr/bin/python
import datetime
from collections import namedtuple

'''
Spark Project

SPARK Data Flights analysis

1. Parse the rows in the dataset

'''

airlines = sc.textFile('airlines.csv')
flights = sc.textFile('flights.csv')


flights_fields = ('flight_date', 'airlineline_code' ,'flight_num', 'source_airport', 'destination_airport', 'departure_time', 'departure_delay','arrival_time','arrival_delay', 'airtime' , 'distance')
flights_namedtuples = namedtuple('Flight', flights_fields, verbose = 'true')
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

'''--Was removed cos datetime.datetime was not well defined
def parse(row):
	row[0] = datetime.strptime(row[0], DATE_FMT).date()
	row[5] = datetime.strptime(row[5], TIME_FMT).date()
	row[6] = float(row[6])
	row[7] = datetime.strptime(row[7], TIME_FMT).date()
	row[8] = float (row[8])
	row[9] = float (row[9])
	row[10] = float (row[10])
	return Flight(*row[:11])'''


def parse(row):
	row[0] = datetime.datetime.strptime(row[0], DATE_FMT).date()
	row[5] = datetime.datetime.strptime(row[5], TIME_FMT).date()
	row[6] = float(row[6])
	row[7] = datetime.datetime.strptime(row[7], TIME_FMT).date()
	row[8] = float (row[8])
	row[9] = float (row[9])
	row[10] = float (row[10])
	return Flight(*row[:11])

#Map the fields with the flights RDD
parsed_flights = flights.map(lambda x: x.split(',')).map(parse)
#Since we will be using parsed_flights RDD a lot, it makes sense to cache it
parsed_flights.persist()  # Remeber to remove the RDD from memory after using it
#parsed_flights.unpersist()
parsed_flights.first

#Access individual columns, for example the distance
parsed_flights.map(lambda x: x.distance)

# Calculate the average distance travelled by a all flights
#Also calculate the average distance travelled by a particular flight

#Formula is to divide the total distance by the number of flights
total_distance = parsed_flights.map(lambda x: x.distance).reduce(lambda x, y : x+y)
Average_distance = total_distance/parsed_flights.count()
priint(Average_distance )


#Compute the % of flights which had delays
# Find out how well airlines and airports perform in terms of delay
#First start by counting the number of flights which were delayed at the airport
number_of_delayed_flights = parsed_flights.filter(lambda x: x.departure_delay > 0).count()
perc_of_flightswith_delay = number_of_delayed_flights/float(parsed_flights.count()) #I think this should contain * 100 to give accurate percentage


#Compute the frequency distribution of departure delays. i.e the Average departure delay of flights
total_delay = parsed_flights.map(lambda x: x.departure_delay).reduce(lambda x, y : x+y)
Average_delay = total_delay/parsed_flights.count()
priint(Average_delay )

#But to improve performance, i will be using aggregate to reduce the number of actions used to compute the average delay. instead of using both reduce and count in one operation
sumCountTuple = parsed_flights.map(lambda x: x.departure_delay).aggregate((0,0),
																		(lambda acc, value: (acc[0] + value, acc[1] + 1)) #Calculation on individual nodes
																		(lambda acc1, acc2: (acc1[0] + acc1[0], acc2[1]+ acc2[1])))#Calculation on result from all the nodes
print(sumCountTuple)
average_delay = sumCountTuple[0]/float(sumCountTuple[1])

#Frequency distribution of flight delays - Calculate the number of flights that occured within every hour. (Note that the result has been rounded off to integer type)
freq_dist_delays= parsed_flights.map(lambda x: x.departure_delay/60).countByValue()
# Special operations performed on Pair RDDS
#Keys -- returns all the keys for each key  --Transformation
airportDelays.keys().take(10)
#Values -- returns all the values for each key  --Transformation
airportDelays.values().take(10)
#mapValues --Accepts a function that operates only on the values of the key value pair. It leaves the Key as is
	#Count the number of aiports that have delays
numberOfAirportWithDelays = airportDelays.mapValues(lambda x: 1).reduceByKey(lambda x, y: x + y)  #Notice the 1 at mapValues, it replaces all the values with the value "1"

#GroupByKey -- Accepts a function that groups all the values which have the same key into a list/collection
#cogroup -- Works exactly like groupByKey, only that it works with multiply key value RDDs. Unlike groupByKey that works with only 1 key value RDD
#reduceByKey --Just like reduce, accepts a function to combine 2 values and returns the value of the same type, but only values with the same key  --Transformation
	#For example, to calculate the total delay per airport
	#First get a pair RDD for airport, and delay
airportDelays =  parsed_flights.map(lambda x, y : (x.source_airport, y.departure_delay))
totalDelaysPerAirport = airportDelays.reduceByKey(lambda x, y : x + y)
#combineByKey -- This works as aggregate in basic RDDs but the difference is that it works on pair RDDs.

#Reduce and aggregate are Actions, WHILE reduceByKey and combineByKey are transformations on pair RDDS

#Other Pair RDD Actions
#countByKey  -- counts the number of values for each unique keys put together



#Merge (Join operation)
# To calculate the average delay per airport, we need the sum of all the delays per airport "totalDelaysPerAirport" and the number of number of airports with delay "numberOfAirportWithDelays "
airportsSumCount = totalDelaysPerAirport.join(numberOfAirportWithDelays)
#Now calculate the average delay per airport. remember that our airportSumCount is now a pair RDD but its value part is a list that contains SUM, and COUNT values
averageDelayPerAirport= airportsSumCount.mapValues(lambda x: x[0]/float(x[1]))
#In as much as we have achieved this in 3 steps, we could do even better by achieving our result in one step by using "combineByKey" 
#combineByKey -- This works as aggregate in basic RDDs but the difference is that it works on pair RDDs. Note that it works only on the value. the key remain unchanged. - Transformation
airportsSumCount2 = airportDelays.combineByKey((lambda value : (value, 1)), #createCombiner function here initializes a value when a key is first seen within a partition
												(lambda acc, value: (acc[0]+ value, acc[1] +1)), # The merge function here specifies how values with the same key should be combined in the same partition
												(lambda acc1, acc2: (acc1[0]+ acc2[0], acc1[1]+acc2[1])) #mergeCombiners function here specifies how the result from each partition should be combined
												)
averageDelayPerAirport2= airportsSumCount2.mapValues(lambda x: x[0]/float(x[1]))

#Find the top 10 airports based on delay. Simply sort the averageDelayPerAirport RDD in descending order using sortBy()
#sortBy() can be used with both basic and pair RDDs -- transformation
tenWorstAirports = averageDelayPerAirport.sortBy(lambda x: -x[1]).take(10)
#Its time to use the airporsts.csv to get the full name of the airport. We need a look up here...
#lookup -- returns all the values for a specific key. takes the key as parameter and retuns all the values for that key.
airports = sc.textFile('airports.csv'),filter(notHeader).map(split) # notHeader and split are custom functions 
#there are 3 ways to get the airport name from airports pair RDD and use it inside tenWorstAirports
#Lookup - An action specific to pair RDDs
##collectAsMap  -- returns a dictionary of all the key value pairs  -- A.K.A map -- Action
#To use 2nd option we need 2 steps. First is create a dictionay that will serve as the lookup
airportslookup = airports.collectAsmap()
tenWorstAirportsDetailed = tenWorstAirports.map(lambda x: (airportslookup[x[0]], x[1]))
#broadcast -- allows us to cache a variable (or lookup data) on each node in the cluster. instead or transporting the variable over the network each time it is used. they have 3 characteristics.
#Immutable, distributed and In-memory
airportsBC = sc.broadcast(airportslookup)
tenWorstAirportsDetailed2 = tenWorstAirports.map(lambda x: (airportsBC.value[x[0]], x[1]))

#Accumulators
logs = sc.textFile(logs.txt)   
#Create a python function to parse the lof file and return datefield and logfield. It will also contain a global variable (same name as spark accumulator variable)that accumulates over a condition
#process the log file and save to a file
logs.map(processLogs).saveAsTextFile(destination)  #-- This is an action because it materializes logs RDD after transformation through processLogs function
errorsCount = sc.accumulators(0)
print errorsCount.value()


Meeting with Andy


--Security related support
KMS/KTS 
Kerberos -- User creation and management?
SSL -- do we need to do anything to enable SSL for an application?
Rest encryption/ Nav encrypt  -- To create an encryption zone, how can we support this?


--walk me through postgres database configuration
kts nodes used for internal repo
add kts through terraform
---what happens to size of the embeded databases when they grow?

--RDS is not HA by default --verify this..

















































