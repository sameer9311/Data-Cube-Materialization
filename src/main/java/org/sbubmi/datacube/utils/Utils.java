package org.sbubmi.datacube.utils;


import java.util.Map;

import org.apache.hadoop.fs.Path;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 
 * @author sameer
 * 
 * Contains all the helper methods required for the application
 *
 */

public class Utils {
	
	/**
	 * Writes the contents of the Map to a file specified by path
	 * 
	 * @param path The path to the file being written
	 * @param dimensionFactMap Map whose contents are to be written to file
	 */
	
	public void writeQueryResultToFile(Path path , Map<String,Integer> dimensionFactMap)
	{
		try {
			
			File file = new File(path.toString());

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(fw);
			
			for (Map.Entry<String,Integer> entry : dimensionFactMap.entrySet()) {
				
	    		  String dimensionValues = entry.getKey();
	    		  Integer factValue = entry.getValue();
	    		  
	    		  bw.write(dimensionValues+","+factValue+"\n");    		  
	    		  
	    		}
			
			bw.close();

			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
