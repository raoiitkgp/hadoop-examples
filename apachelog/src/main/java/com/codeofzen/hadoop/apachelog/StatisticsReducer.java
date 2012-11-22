
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
public class StatisticsReducer extends MapReduceBase implements Reducer<Text, LogData, Text, Statistics> {
   
   
   /**
    *  Implements interface Reducer
    */
   @Override
   public void reduce(Text key, Iterator<LogData> values, OutputCollector<Text, Statistics> output, Reporter reporter ) throws IOException {
   
      // extract information from values
      long counter = 0;
      long errorCounter = 0;
      long sum = 0;
      long min = Long.MAX_VALUE;
      long max = 0;
      
      while(values.hasNext()) {
         
         LogData data = values.next();
         ++counter;
         
         long length = data.getLength();
         
         sum += data.getLength();
                  
         if(length < min) { min = length; }
         if(length > max) { max = length; }
       
         if( 200 > data.getStatusCode() || 399 < data.getStatusCode()) { ++errorCounter; }
      }
      
      output.collect(key, new Statistics(counter, errorCounter, min, max, (sum / counter) ));
   }
   
}
