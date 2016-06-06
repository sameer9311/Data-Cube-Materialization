## Cube Materialization using Hadoop MapReduce

---

#### Aim

To create a sample data cube with minimal dimensions using MapReduce framework

#### Description

* A sample csv file containing the sales data of real estate was taken and imported in the mongoDB database using `mongoimport` . 
The csv file can be found at `src/main/resources/salesdata.csv`.

* The cube properties - **dimensions, measures** and **facts** are stored in `src/main/resources/cubeproperties.xml` file. 
For building the sample cube, two dimensions are considered - **city** and **apartment type**. The fact in this data is **price**
and the measure **sum** will be applied to sum all the prices of all the records across both dimensions.

* All the documents with the specified dimensions and fact are retrieved from the mongodb. The query output looks like :

  { "city" : "SACRAMENTO", "type" : "Residential", "price" : 91002 }  <br /> 
  { "city" : "RANCHO CORDOVA", "type" : "Condo", "price" : 94905 }  <br />
  { "city" : "RIO LINDA", "type" : "Residential", "price" : 98937 }  <br />
  ...

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
  
* The reducer then reads all the key, value pairs and gives a single key, value :
  * **key :** Total_Sum
  * **Value :** 15221210
