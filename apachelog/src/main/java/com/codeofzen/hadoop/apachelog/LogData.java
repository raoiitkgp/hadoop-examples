package com.codeofzen.hadoop.apachelog;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



/**
 * Implements the output produced by the map-step.
 */
public class LogData implements Writable {
   
   private String date;
   private String time;
   private long length;
   private int  statusCode;
   
   
   
   private LogData() {}
      
   
   
   public LogData(String date, String time, long length, int statusCode) {
      this.date       = date;
      this.time       = time;
      this.length     = length;
      this.statusCode = statusCode;
   }
      
   
   
   public String getDate() { return date; }
      
   public String getTime() { return time; }
      
   public long getLength() { return length; }
    
   public int getStatusCode() { return statusCode; }
   
   
   
   @Override
   public void readFields(DataInput input) throws IOException {
      this.date       = input.readUTF();
      this.time       = input.readUTF();
      this.length     = input.readLong();
      this.statusCode = input.readInt();
   }
   
   
   
   @Override
   public void write(DataOutput output) throws IOException {
      output.writeUTF(this.date);
      output.writeUTF(this.time);
      output.writeLong(this.length);
      output.writeInt(this.statusCode);
   }
   
   
   @Override
   public boolean equals(Object other) {
      
      if( other == this) return true;
      
      if( other instanceof LogData ) {
         
         LogData otherData = (LogData) other;
                 
         if( otherData.getDate().equals(this.date) &&
             otherData.getTime().equals(this.time) &&
             otherData.getLength()     == this.length &&
             otherData.getStatusCode() == this.statusCode ) {
            System.out.println("EQL: " + other.toString() );
            return true;
         }
         
      }
   
      return false;
   } 
   
   
   @Override
   public String toString() {
      return this.date + "  " + this.time + " - L:" + this.length + " - S:" + this.statusCode;
   }
   
   
   public static LogData read(DataInput input) throws IOException {
      LogData s = new LogData();
      s.readFields(input);
      return s;
   }
   
   
}
