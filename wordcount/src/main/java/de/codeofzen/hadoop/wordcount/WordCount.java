package de.codeofzen.hadoop.wordcount;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/**
 * Simple Hadoop-program to count the number of words in given file 
 *
 */
public class WordCount {

   /**
    * Implementation of the Mapper - step
    */
   public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      /**
       * Implements the map-method of the Mapper-interface 
       */
      @Override
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
         
         String line = value.toString();
         StringTokenizer tokenizer = new StringTokenizer(line);
      
         while (tokenizer.hasMoreTokens()) {
            
            String token = tokenizer.nextToken();
            
            if(token.length() > 3) {
               word.set(token);
               output.collect(word, one);
            }
         }
         
      }
      
   }

   /**
    * Implementation of the Reducer - step
    */
   public static class ReduceClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

      
      /**
       * Implements the reduce-method of the Reducer-interface
       */
      @Override
      public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
         
         int sum = 0;
         
         while(values.hasNext()) {
            sum += values.next().get();
         }
         
         output.collect(key, new IntWritable(sum));
      }
      
   }

   
   
   /**
    * @param args the command line arguments
    */
   public static void main(String[] args) throws IOException {
      
      JobConf conf = new JobConf(WordCount.class);
      conf.setJobName("WordCount");
      
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(IntWritable.class);
      
      conf.setMapperClass(MapClass.class);
      conf.setCombinerClass(ReduceClass.class);
      conf.setReducerClass(ReduceClass.class);
      
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
      
      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
      
      JobClient.runJob(conf);
      
   }
}
