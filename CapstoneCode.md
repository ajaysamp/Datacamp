Sample code for the Capstone Project. The project is a simple exploratory analysis for precipitation data from 58 weather stations in the state of Washington.

Note that this is a sample code to give a general idea about the difficutly level. The actual project and dataset may change. However, the code and difficulty will be similar to what is shown here. This code was created on Databricks and will be adapted for Datacamp's native spark environment. 

#read the data  
df = sqlContext.read.format('csv').options(header='true',inferSchema='true').load("dbfs:/FileStore/tables/PrecipData.csv")

#convert the string  column DATE to timestamp and add a new column DATE_New  
df = df.withColumn('DATE_New', unix_timestamp('DATE', "yyyyMMdd HH:mm").cast('timestamp'))

#get the number of distinct stations   
df.select('STATION').distinct().count()

#check the length of record for each station and put it into a new data frame.  
#note that each station should have 96 records per day\
station_records = df.groupby('STATION').agg(count('STATION').alias('Count'))

#convert the data to a pandas dataframe\
station_records = station_records.toPandas()

#sort the station records\
station_records = station_records.sort_values(by='Count',ascending=False)

#plot the station with the top 10 records. 
fig,ax = plt.subplots(figsize=(10,4)). 
ax = sns.barplot(x='STATION',y='Count',data=station_records.iloc[0:9,]). 
ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ','))). 
plt.xticks(fontsize=12, fontweight='bold',rotation=90). 
plt.yticks(fontweight='bold'). 
plt.xlabel(''). 
plt.ylabel(''). 
plt.title('Station Record Count',fontsize = 14,fontweight='bold'). 
plt.tight_layout(). 
display(plt.show()). 

#select the station with the maximum number of records for further analysis. 
selected_station = df.filter(col('STATION') == 'COOP:451759')

#look at the summary statistics for the data.  
selected_station.describe().show()

#plot the QPCP for the selected station.   
fig,ax = plt.subplots(figsize=(6,4))
ax = sns.boxplot(x = 'QPCP',data=selected_station.toPandas())
plt.tight_layout()
display(plt.show())

#remove the outliers ion QGAG\
selected_station = selected_station.filter((col('QPCP') > 0) & (col('QPCP') < 900))

#plot QPCP after removing the outliers\ 
fig,ax = plt.subplots(figsize=(6,4))\
ax = sns.boxplot(x = 'QPCP',data=selected_station.toPandas())\
plt.tight_layout()\
display(plt.show())\

 #plot the time series for the QPCP at the selected station\
fig,ax = plt.subplots(figsize=(12,6))
sns.pointplot(data=selected_station.toPandas(),x="DATE_New", y='QPCP')
plt.tight_layout()
display(plt.show())


