package org.sbubmi.datacube.spark;

import java.util.Map;
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
	
		
	public static void main(String[] args) {
		// Read the dimensions of the cube, the fact and the measure to be
		// applied, from XML file
		CubeProperties cubePropertyObj = CubeProperties.readCubeProperties();

		// Map of dbColumn name and the name appearing of that column in Cube
		Map<String, String> dimensionMap = cubePropertyObj.getDimensionMap();
		String measure = cubePropertyObj.getMeasure();
		String fact = cubePropertyObj.getFact();

		// Set configuration options for the MongoDB Hadoop Connector.
		Configuration mongodbConfig = new Configuration();

		// MongoInputFormat allows us to read from a live MongoDB instance.
		// We could also use BSONFileInputFormat to read BSON snapshots.
		mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");

		// MongoDB connection string naming a collection to use.
		// If using BSON, use "mapred.input.dir" to configure the directory
		// where BSON files are located instead.
		mongodbConfig.set("mongo.input.uri", "mongodb://localhost:27017/salesdb.sales");

		// Now query the mongodb database 
		// We need to set which fields(dimensions) to retrieve from query
		// We need to mention the dimensions as well as the fact
		String fields = "{";
		for (String Key : dimensionMap.keySet()) {
			fields = fields.concat(Key + ":1,");
		}
		fields = fields.concat(fact + ":1,"); // add the fact field to the query
		fields = fields.concat("_id:0}"); // set to 1 if we want to retrieve _id also
											
		mongodbConfig.set("mongo.input.fields", fields);

		SparkConf conf = new SparkConf().setAppName("org.sparkexample.MongoSpark2").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
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
		
		// We now need to need to extract dimensions and fact from each document by string manipulation
		// and generate key value pair from it
		// key is string concatenation of all dimensions, each dimension separated by "*"
		// value is respective fact value for those set of dimensions
		JavaPairRDD<String, Integer> dimensionFactRDD = documentsRetreived.mapToPair(DIMENSIONFACT_MAPPER);
		
			
		// We now need to perform groupby on the dimensions and fact
		// i.e perform reduce operation on the keys
		JavaPairRDD<String, Integer> groupedDimensionFactRDD = dimensionFactRDD.reduceByKey(DIMENSIONFACT_REDUCER);
		
		groupedDimensionFactRDD.foreach(
				new VoidFunction<Tuple2<String, Integer>>() {
                    public void call(Tuple2<String, Integer> T) {
                        
                    	System.out.println(T._1 + T._2.toString());
                    }
                }
				
				);
		
		// Convert the pairRDD into RDD with a tuple
		JavaRDD<Tuple2<String,Integer>> dimFactTupleRDD = JavaRDD.fromRDD(JavaPairRDD.toRDD(groupedDimensionFactRDD), groupedDimensionFactRDD.classTag());
		
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

	private static final PairFunction<Tuple2<Object, BSONObject>, String, Integer> DIMENSIONFACT_MAPPER = 
			new PairFunction<Tuple2<Object, BSONObject>, String, Integer>() {

		public Tuple2<String, Integer> call(Tuple2<Object, BSONObject> tuple) throws Exception {

			String dimensions = ""; 	//this will hold all dimensions
			BSONObject bsonobj = tuple._2;	//get bsonobject from query 
			Set<String> dimFactsSets = bsonobj.keySet();	//names of all dimensions and fact
			int len = dimFactsSets.size();
			String[] dimensionsFacts = dimFactsSets.toArray(new String[dimFactsSets.size()]);
			for (int i = 0; i < len - 1; i++) {
				
				// for each dimension, concat its value
				//separate each value of dimension by a "*"
				dimensions = dimensions.concat(bsonobj.get(dimensionsFacts[i]).toString().concat("*")); 
				
			}
			return new Tuple2<String, Integer>(dimensions,
					Integer.parseInt(bsonobj.get(dimensionsFacts[len - 1]).toString()));
		}

	};
	
	/**
	 * Returns Key value pairs of the dimensions and fact after reducing them by addition
	 * i.e after performing a group by
	 */

	private static final Function2<Integer, Integer, Integer> DIMENSIONFACT_REDUCER = 
			new Function2<Integer, Integer, Integer>() {

		public Integer call(Integer a, Integer b) throws Exception {
			return a + b;
		}
	};	
	
	/**
	 * Creates a new RDD to with bson object and _id from the tuple to store in mongodb
	 */
	private static final PairFunction<Tuple2<String, Integer>, Object, BSONObject> DIMENSIONFACT_BSON_MAPPER = 
			new PairFunction<Tuple2<String, Integer>, Object, BSONObject>() {
		
		public Tuple2<Object, BSONObject> call(Tuple2<String, Integer> dimFactTuple) throws Exception {
            DBObject doc = BasicDBObjectBuilder.start()
                .add("dimensions", dimFactTuple._1)
                .add("fact_value", dimFactTuple._2)                
                .get();
            // null key means an ObjectId will be generated on insert
            return new Tuple2<Object, BSONObject>(null, doc);
        }
		
	};
	
	


}
