## Cube Materialization using Hadoop MapReduce

---

#### Aim

To create a sample data cube with minimal dimensions using MapReduce framework

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

* All the documents with the specified dimensions and fact are retrieved from the mongodb. The query output looks like :
  ```
  { "city" : "SACRAMENTO", "type" : "Residential", "price" : 91002 }  
  { "city" : "RANCHO CORDOVA", "type" : "Condo", "price" : 94905 }  
  { "city" : "RIO LINDA", "type" : "Residential", "price" : 98937 }  
  ...
  ```

* The retrieved documents are stored in the form of Map in java.
  * **key** is String concatenation of all the dimension values separated by "-" <br />
  e.g. Residential-SACRAMENTO <br />
  * **value** is the fact value for that respective dimension field values. <br />
  e.g. 91002
  
* This Map is then stored in an text file which would be read by the hadoop mapper method. The file is `queryResults.txt`

* The mapper is run with following example parameters :
  * **Input key :** Residential-SACRAMENTO
  * **Input Value :** 91002
  * **Output key :** "Total_Sum"
  * **Output Value :** 91002
  
* The reducer then reads all the key, value pairs and gives a single key, value stored in `out` folder:
  * **key :** Total_Sum
  * **Value :** 15221210
