package org.sbubmi.datacube.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.DBCursor;

import com.mongodb.ServerAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles all the mongodb database operations.
 */

public class QueryDB {

	/**
	 * Makes query into the mongodb database according to the dimension columns
	 * and facts specified
	 * 
	 * @param dimensions Map of dimension column name in db and representative name in cube            
	 * @param fact The fact of the cube	  
	 * @return Map<String, Integer> key is string concatenation of all dimension
	 *         values and value is corresponding fact value
	 */

	public Map<String, Integer> readDbData(Map<String, String> dimensions, String fact) {

		// This map has key as string concatenation of all dimension values and
		// value is corresponding fact value
		Map<String, Integer> dimensionFactMap = new HashMap<String, Integer>();

		try {
			Mongo mongo = new Mongo("localhost", 27017);
			DB db = mongo.getDB("salesdb");// Use DB

			DBCollection collection = db.getCollection("sales");

			DBObject query = new BasicDBObject(); // can be used to specify matching conditions if any
			DBObject field = new BasicDBObject(); // stores the fields to be retrieved from the query

			// Specify which fields/columns to be retrieved from the query
			for (Map.Entry<String, String> entry : dimensions.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				field.put(key, 1); // stores the fields to be retrieved from the query
									
			}
			field.put(fact, 1);
			field.put("_id", 0);

			DBCursor cursor = collection.find(query, field);

			String str;

			// Iterate over all retrieved documents
			while (cursor.hasNext()) {

				BasicDBObject obj = (BasicDBObject) cursor.next();

				String dimensionsValues = "";
				
				//Get values of specified columns
				for (Map.Entry<String, String> entry : dimensions.entrySet()) {
					String key = entry.getKey();

					str = obj.get(key).toString();

					dimensionsValues = dimensionsValues.concat(str).concat("-");

				}
				dimensionsValues = dimensionsValues.substring(0, dimensionsValues.length() - 1); 
				// to remove last "-"
																									
				System.out.println(dimensionsValues + obj.get(fact));
				dimensionFactMap.put(dimensionsValues, (Integer) obj.get(fact));

			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return dimensionFactMap;
	}
}