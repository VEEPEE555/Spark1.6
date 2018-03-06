Problem:
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)File format - text fileDelimiter - “,”Get monthly count of primary crime type, sorted by month in ascending and number of crimes per type in descending orderStore the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_monthOutput File Format: TEXTOutput Columns: Month in YYYYMM format, crime count, crime typeOutput Delimiter: \t (tab delimited)Output Compression: gzip


Solution:

#Read crime CSV file into an RDD
crimeRDD=sc.textFile('/public/crime/csv/crime_data.csv')


#Strip header and project/map only the needed columns from the RDD
crimeRDDMap=crimeRDD.filter(lambda a: a.split(",")[0]!='ID').map(lambda b: (b.split(",")[2],b.split(",")[5]))


#Mapp the RDD to transform Date into a format 'YYYYMM' 
crimeRDDMaptrim=crimeRDDMap.map(lambda c: ((c[0][6:10]+c[0][3:5],c[1]),1))


#Do aggregation using reduceByKey(add) where Key is (formatted date and Crime type)
crimeRDDMapRbk=crimeRDDMaptrim.reduceByKey(lambda a,b: a+b) 


#Sort the RDD elements with Formatted date ASC, Crime Type Count DESC #Also Map the RDD so the final elements are (Date Format, crime #type count, crime type), all tab seperated
crimeRDDMapsort=crimeRDDMapRbk.map(lambda d: ((d[0][0],-d[1]),(d[0][0],d[1],d[0][1]))).sortByKey() \
.map(lambda e: e[1][0]+"\t"+str(e[1][1])+"\t"+e[1][2]) 


# Save the RDD as a textFile , using Gzip Compression
crimeRDDMapsort.coalesce(1).saveAsTextFile("/user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_monthOutput", \ compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
