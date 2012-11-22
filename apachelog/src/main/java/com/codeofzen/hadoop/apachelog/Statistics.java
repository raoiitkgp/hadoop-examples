package com.codeofzen.hadoop.apachelog;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



/**
 * Implements the final output produced by the Reduce-step.
 */
public class Statistics implements Writable {
   
   private long requests;
   private long errors;
   private long minLength;
   private long maxLength;
   private long averageLength;
   
   
   public Statistics() {}
   
   public Statistics(long requests, long errors, long minLength, long maxLength, long averageLength) {
      this.requests  = requests;
      this.errors    = errors;
      this.minLength = minLength;
      this.maxLength = maxLength;
      this.averageLength = averageLength;
   }
   
   
   @Override
   public void readFields(DataInput input) throws IOException {
      this.requests  = input.readLong();
      this.errors    = input.readLong();
      this.minLength = input.readLong();
      this.maxLength = input.readLong();
      this.averageLength = input.readLong();
   }
   
   
   @Override
   public void write(DataOutput output) throws IOException {
      output.writeLong(this.requests);
      output.writeLong(this.errors);
      output.writeLong(this.minLength);
      output.writeLong(this.maxLength);
      output.writeLong(this.averageLength);
   }
   
   
   @Override
   public String toString() {
      return this.requests + "," + this.errors + "," + this.minLength + "," + this.maxLength + "," + this.averageLength;
   }
   
   
   public static Statistics read(DataInput input) throws IOException {
      Statistics s = new Statistics();
      s.readFields(input);
      return s;
   }
   
   
}
