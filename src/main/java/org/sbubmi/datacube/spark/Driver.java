package org.sbubmi.datacube.spark;

import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.bson.BSONObject;
import org.jets3t.service.impl.soap.axis._2006_03_01.User;
import org.sbubmi.datacube.utils.CubeProperties;
import org.sbubmi.datacube.utils.QueryDB;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.BSONFileOutputFormat;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.MongoUpdateWritable;

import scala.Tuple2;

/**
 * Driver class of entire spark based cube computation. 
 * Makes calls to perform the following actions : 
 * - read cube properties(dimensions,measures,facts)from XML file 
 * - query the mongodb for specified dimensions and facts and store in RDD 
 * - extract dimensions and fact from the RDD to generate key,value Pairs 
 * - perform groupby by reducing the key value pairs 
 */

public class Driver {
	
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		long startTime = System.currentTimeMillis();

		// Read the dimensions of the cube, the fact and the measure to be
		// applied, from XML file
		CubeProperties cubePropertyObj = CubeProperties.readCubeProperties();

		// Map of dbColumn name and the name appearing of that column in Cube
		Map<String, String> dimensionMap = cubePropertyObj.getDimensionMap();
		String measure = cubePropertyObj.getMeasure();
		String fact = cubePropertyObj.getFact();
		
		// Set configuration options for the MongoDB Hadoop Connector.
		//Configuration mongodbConfig = new Configuration();

		// MongoInputFormat allows us to read from a live MongoDB instance.
		// We could also use BSONFileInputFormat to read BSON snapshots.
		//mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");

		// MongoDB connection string naming a collection to use.
		// If using BSON, use "mapred.input.dir" to configure the directory
		// where BSON files are located instead.
		//mongodbConfig.set("mongo.input.uri", "mongodb://localhost:27017/randdb1.randcoll1");
		
		//SparkConf conf = new SparkConf().setAppName("org.sparkexample.MongoSpark2").setMaster("local");
				
		Configuration mongodbConfig = new Configuration();
		mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
		mongodbConfig.set("mongo.input.uri", "mongodb://localhost:27017/input.results");
	    SparkConf conf = new SparkConf().setMaster("local").setAppName("org.sparkexample.MongoSpark2");

	    JavaSparkContext sc = new JavaSparkContext(conf);		

		// Now query the mongodb database 
	    // We can also provide the conditions on the fields
	    // This will be provided by the user at runtime
	    
	    Scanner conditionScanner = new Scanner(System.in);
	    System.out.println("Enter the conditions for the query:");	 
	    System.out.print("For e.g {'color':'blue','features.Area':{$lt:100}}\nEnter {} for no condition\nCondition:");
	    String condtions = conditionScanner.nextLine();
	    mongodbConfig.set("mongo.input.query", condtions);
	    conditionScanner.close();  
	    
	    
		// We need to set which fields(dimensions) to retrieve/project/select from query
		// We need to mention the dimensions as well as the fact
		String fields = "{";
		for (String Key : dimensionMap.keySet()) {
			fields = fields.concat(Key + ":1,");
		}
		fields = fields.concat(fact + ":1,"); // add the fact field to the query
		fields = fields.concat("_id:0}"); // set to 1 if we want to retrieve _id also
											
		mongodbConfig.set("mongo.input.fields", fields);
		
		
		/*JavaRDD<BSONObject> documentsRetreived = sc.newAPIHadoopRDD(mongodbConfig,
	            MongoInputFormat.class, Object.class, BSONObject.class).map(
	            new Function<Tuple2<Object, BSONObject>, BSONObject>() {
	                @Override
	                public BSONObject call(Tuple2<Object, BSONObject> doc) throws Exception {
	                    return doc._2;
	                }
	            }
	        );*/

		// Create an RDD backed by the MongoDB collection.
		JavaPairRDD<Object, BSONObject> documentsRetreived = sc.newAPIHadoopRDD(mongodbConfig, // Configuration
				MongoInputFormat.class, // InputFormat: read from a live cluster										
				Object.class, // Key class
				BSONObject.class // Value class
		);
		
		documentsRetreived.foreach(
				new VoidFunction<Tuple2<Object, BSONObject>>() {
                    public void call(Tuple2<Object, BSONObject> T) {
                        
                    	System.out.println(T._1.toString() + T._2.toString());
                    }
                }
				
				);
		
		
		// We now need to need to extract dimensions and fact from each document by string manipulation
		// and generate key value pair from it
		// key is string concatenation of all dimensions, each dimension separated by "$"
		// value is respective fact value for those set of dimensions
		JavaPairRDD<String, Double> dimensionFactRDD = documentsRetreived.mapToPair(DIMENSIONFACT_MAPPER);
		
		/*dimensionFactRDD.foreach(
		new VoidFunction<Tuple2<String, Double>>() {
            public void call(Tuple2<String, Double> T) {
                
            	System.out.println("Mapper output "+T._1.toString() +" v "+ T._2.toString());
            }
        }
		
		);*/
		
		// We now need to perform groupby on the dimensions and fact
		// i.e perform reduce operation on the keys
		JavaPairRDD<String, Double> groupedDimensionFactRDD = dimensionFactRDD.reduceByKey(DIMENSIONFACT_REDUCER);
		
		/*groupedDimensionFactRDD.foreach(
				new VoidFunction<Tuple2<String, Integer>>() {
                    public void call(Tuple2<String, Integer> T) {
                        
                    	System.out.println(T._1 + T._2.toString());
                    }
                }
				
				);*/
		
		// Convert the pairRDD into RDD with a tuple
		JavaRDD<Tuple2<String,Double>> dimFactTupleRDD = JavaRDD.fromRDD(JavaPairRDD.toRDD(groupedDimensionFactRDD), groupedDimensionFactRDD.classTag());
		
		// Create new RDD with the bson object and _id to store in mongodb
		JavaPairRDD<Object, BSONObject> outputRDD = dimFactTupleRDD.mapToPair(DIMENSIONFACT_BSON_MAPPER);
		
		// Create a separate Configuration for saving data back to MongoDB.
		Configuration outputConfig = new Configuration();
		outputConfig.set("mongo.output.uri",
				                 "mongodb://localhost:27017/output.mysalescube");

		// Save this RDD as a Hadoop "file".
		// The path argument is unused; all documents will go to 'mongo.output.uri'.
		outputRDD.saveAsNewAPIHadoopFile(
				    "file:///this-is-completely-unused",
				    Object.class,
				    BSONObject.class,
				    MongoOutputFormat.class,
				    outputConfig
				);		
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Total Execution Time in MiliSeconds is "+totalTime);
		
	}
	
	/**
	 * Returns Key value pairs of the dimensions and fact 
	 * after extracting from query results by string manipulations. 
	 *  
	 * @param tuple The first parameter is an ObjectId instance, which is the Mongo object ID of the document. 
	 * 				The second parameter contains the BSON document. 
	 * 
	 * @return Tuple2 first element is string concatenation of all dimensions
	 * 				  second element is respective fact Integer value for those dimensions
	 */

	private static final PairFunction<Tuple2<Object, BSONObject>, String, Double> DIMENSIONFACT_MAPPER = 
			new PairFunction<Tuple2<Object, BSONObject>, String, Double>() {

		public Tuple2<String, Double> call(Tuple2<Object, BSONObject> tuple) throws Exception {

			String dimensions = ""; 	//this will hold all dimensions
			BSONObject bsonobj = tuple._2;	//get bsonobject from query 
			Set<String> dimFactsSets = bsonobj.keySet();	//names of all dimensions and fact
			int len = dimFactsSets.size();
			
			String[] dimensionsFacts = dimFactsSets.toArray(new String[dimFactsSets.size()]);
			
			String fact = CubeProperties.fact;			
			Double factValue = 0.0;
		    
			
			for (int i = 0; i < len; i++) {
				
				
				if(!dimensionsFacts[i].equals(fact))
				{
					// for each dimension, concat its value
					//separate each value of dimension by a "*"
					dimensions = dimensions.concat(bsonobj.get(dimensionsFacts[i]).toString().concat("$")); 
				}
				else
				{
					factValue = Double.parseDouble(bsonobj.get(dimensionsFacts[i]).toString());
					//System.out.println("the fact is "+fact+" and value is "+factValue);
				}
				
				
			}
			return new Tuple2<String, Double>(dimensions,factValue);
		}

	};
	
	/**
	 * Returns Key value pairs of the dimensions and fact after reducing them by addition
	 * i.e after performing a group by
	 */

	private static final Function2<Double, Double, Double> DIMENSIONFACT_REDUCER = 
			new Function2<Double, Double, Double>() {

		public Double call(Double a, Double b) throws Exception {
			return a + b;
		}
	};	
	
	/**
	 * Creates a new RDD to with bson object and _id from the tuple to store in mongodb
	 */
	private static final PairFunction<Tuple2<String, Double>, Object, BSONObject> DIMENSIONFACT_BSON_MAPPER = 
			new PairFunction<Tuple2<String, Double>, Object, BSONObject>() {
		
		public Tuple2<Object, BSONObject> call(Tuple2<String, Double> dimFactTuple) throws Exception {
			
            DBObject doc = BasicDBObjectBuilder.start()
                .add("dimensions", dimFactTuple._1)
                .add("fact_value", dimFactTuple._2)                
                .get();
            // null key means an ObjectId will be generated on insert
            return new Tuple2<Object, BSONObject>(null, doc);
        }
		
	};
	
	private static final Integer foo(Integer a)
	{
		return 0;
	}
	
	


}
