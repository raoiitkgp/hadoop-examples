package com.codeofzen.hadoop.apachelog;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



/**
 * Implements the final output produced by the Reduce-step.
 */
public class Statistics implements Writable {
   
   private long minLength;
   private long maxLength;
   private long averageLength;
   
   
   
   @Override
   public void readFields(DataInput input) throws IOException {
      this.minLength = input.readLong();
      this.maxLength = input.readLong();
      this.averageLength = input.readLong();
   }
   
   
   @Override
   public void write(DataOutput output) throws IOException {
      output.writeLong(this.minLength);
      output.writeLong(this.maxLength);
      output.writeLong(this.averageLength);
   }
   
   
   public static Statistics read(DataInput input) throws IOException {
      Statistics s = new Statistics();
      s.readFields(input);
      return s;
   }
   
   
}
