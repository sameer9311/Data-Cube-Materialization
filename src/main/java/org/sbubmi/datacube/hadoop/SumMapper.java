package org.sbubmi.datacube.hadoop;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author sameer
 * 
 * Mapper class of hadoop which computes total sum over all dimensions
 *
 */

public class SumMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] dimensionValues = line.split(",");
        
            word.set("total_sum"); 
            IntWritable sum = new IntWritable(Integer.parseInt(dimensionValues[1]));
            context.write(word, sum );
        
    }
}
