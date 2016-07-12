## Cube Materialization using Apache Spark

---

#### Aim

To create a sample data cube with minimal dimensions using Apache Spark

#### Description

* A sample csv file containing the sales data of real estate was taken and imported in the mongoDB database using `mongoimport` . 
The csv file can be found at `src/main/resources/salesdata.csv`. It has the following structure :

|street	|city	|zip	|state	|beds	|baths	|sq__ft	|type	|sale_date	|price	|latitude	|longitude |
| ----- | --- | --- | ----- | --- | ----- | ----- | --- | --------- | ----- | ------- | -------- |
|3526 HIGH ST	|SACRAMENTO	|95838	|CA	|2	|1	|836	|Residential	|Wed May 21 00:00:00 EDT 2008	|59222	|38.631913	|-121.434879 |
|51 OMAHA CT	|SACRAMENTO	|95823	|CA	|3	|1	|1167	|Residential	|Wed May 21 00:00:00 EDT 2008	|68212	|38.478902	|-121.431028 |
|2796 BRANCH ST	|SACRAMENTO	|95815	|CA	|2	|1	|796	|Residential	|Wed May 21 00:00:00 EDT 2008	|68880	|38.618305	|-121.443839 |


* The cube properties - **dimensions, measures** and **facts** are stored in `src/main/resources/cubeproperties.xml` file. 
For building the sample cube, two dimensions are considered - **city** and **apartment type**. The fact in this data is **price**
and the measure **sum** will be applied to sum all the prices of all the records across both dimensions.

##### Lines 67- 72

We need to mention which fields to retrieve from each document i.e specifying to columns to retrieve in a typical SQL query. The actual mongo query looks like:

`db.sales.find( { matching condition if any }, { city: 1, type: 1, _id:0 } )`

where we want to retreive only city and apartment type and dont want to retrieve _id.

##### Lines 90 - 94

Here Object would have be the document_id if we had retrieved the _id else it is null. The BSONObject is the document with specified fields.

The output of the query looks like:
```
(null){ "city" : "SACRAMENTO" , "type" : "Residential" , "price" : 59222}
(null){ "city" : "SACRAMENTO" , "type" : "Residential" , "price" : 68212}
(null){ "city" : "SACRAMENTO" , "type" : "Residential" , "price" : 68880}
```

##### Line 100 : DIMENSIONFACT_MAPPER

We need to do string manipulation on the retrieved query to extract dimensions and fact value. The dimension values of each 
document are concat together in a string separated by * and stored with respective fact value of that document as a PairRDD<String,Integer>.
The JavaPairRDD **String Integer** looks like:

```
SACRAMENTO*Residential 59222
SACRAMENTO*Residential 68212
SACRAMENTO*Residential 68880
```

##### Line 104: DIMENSIONFACT_REDUCER

Now we need to reduce this JavaPairRDD by its keys(String) similar to a group by query on a normal db. 
After reducing, the new JavaPairRDD **String Integer** looks like:

```
SACRAMENTO*Residential 80946553
RANCHO CORDOVA*Condo 94905
CAMERON PARK*Residential 2292500
```

##### Line 110: DIMENSIONFACT_BSON_MAPPER

We now need to store this RDD back to mongodb. For that we create documents with:
* "dimensions" : String concat of dimensions e.g. "SACRAMENTO*Residential"
* "fact_value" : grouped by fact value of that combination of dimensions e.g. 80946553
