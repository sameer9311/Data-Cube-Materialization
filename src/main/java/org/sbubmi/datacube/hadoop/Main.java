package org.sbubmi.datacube.hadoop;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.sbubmi.datacube.utils.CubeProperties;
import org.sbubmi.datacube.utils.QueryDB;
import org.sbubmi.datacube.utils.Utils;


/**
 * 
 * @author sameer
 * 
 * Driver class of entire hadoop based cube computation.  
 * Makes calls to perform the following actions :
 * - read cube properties(dimensions,measures,facts) from XML file
 * - query the mongodb for specified dimensions and facts
 * - initiate hadoop map reduce job
 */
 

class Main {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		// Read the dimensions of the cube, the fact and the measure to be applied, from XML file
		CubeProperties cubePropertyObj = CubeProperties.readCubeProperties();
		
		// Map of dbColumn name and the name appearing of that column in Cube
		Map <String, String> dimensionMap = cubePropertyObj.getDimensionMap();
		String measure = cubePropertyObj.getMeasure();
		String fact = cubePropertyObj.getFact();
		
		// Query the mongodb to retrieve the dimension fields and fact
		QueryDB queryObj = new QueryDB();
		
		// Map of string concatenation of all dimension field values and the respective fact value
		Map<String,Integer> dimensionFactMap=  queryObj.readDbData(dimensionMap, fact);	
		
		//Path of file to store the query result later to be read by hadoop mapper 
		Path queryResultFilePath = new Path("queryResults.txt");
		
		// Write the query result in the path specified
		Utils utilsobj = new Utils();
		utilsobj.writeQueryResultToFile(queryResultFilePath, dimensionFactMap);			
		
        // Create a new hadoop map reduce job
        Job job = new Job();

        // Set job name to locate it in the distributed environment
        job.setJarByClass(Main.class);
        job.setJobName("Sum across all dimensions");
        
        // Path of folder to store the results of the hadoop reducer 
        String reducerOutputFolderPath = "out";
        
        // Set input and output Path, note that we use the default input format
        // which is TextInputFormat (each record is a line of input)
        FileInputFormat.addInputPath(job, queryResultFilePath);
        FileOutputFormat.setOutputPath(job, new Path(reducerOutputFolderPath));

        // Set Mapper and Reducer class
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        // Set Output key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}

} 
