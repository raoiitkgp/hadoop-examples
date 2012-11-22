package com.codeofzen.hadoop.apachelog;

import com.sun.java.swing.plaf.windows.WindowsTreeUI;
import java.io.IOException;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;



/**
 * Implementation of the Map-step for analysing Apache-logfile statistics
 */
public class StatisticsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LogData> {

   
   private final LogParser logParser = new LogParser();
   
   
   /**
    * Implements interface Mapper
    */
   @Override
   public void map(LongWritable key, Text value, OutputCollector<Text, LogData> output, Reporter reporter ) throws IOException {
    
      
      LogData logData = this.logParser.parseLogLine(value.toString());
      
      if(null != logData) {
         String date = logData.getDate();
         output.collect(new Text(date), logData);
      }
         
   }
   
   
}
