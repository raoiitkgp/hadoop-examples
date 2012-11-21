
package com.codeofzen.hadoop.apachelog;

import java.io.IOException;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;
import java.util.Iterator;


/**
 * Implementation of the Reduce-step for analysing Apache-logfile statistics
 */ 
public class StatisticsReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
   
   
   /**
    *  Implements interface Reducer
    */
   @Override
   public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter ) throws IOException {
      
   }
   
}
