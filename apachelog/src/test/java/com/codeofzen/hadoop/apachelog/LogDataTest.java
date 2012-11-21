/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.codeofzen.hadoop.apachelog;

import java.io.DataInput;
import java.io.DataOutput;
import junit.framework.TestCase;

/**
 *
 * @author muc0001m
 */
public class LogDataTest extends TestCase {
   
   
   public LogDataTest(String testName) {
      super(testName);
   }

   
   public void testEquals() {
      LogData one = new LogData("Date", "Time", 100, 200);
      LogData two = new LogData("Date", "Time", 100, 200);

      assertEquals(one, two);
   }

   
   public void testNotEquals() {
      LogData one = new LogData("Date", "Time", 100, 200);
      LogData two = new LogData("other", "info", 100, 200);

      assertNotSame(one, two);
   }

   
}
