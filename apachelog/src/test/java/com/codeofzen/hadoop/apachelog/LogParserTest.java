/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.codeofzen.hadoop.apachelog;

import java.util.regex.Pattern;
import junit.framework.TestCase;


/**
 *
 * @author muc0001m
 */
public class LogParserTest extends TestCase {
   
   
   public LogParserTest(String testName) {
      super(testName);
   }
   
   
   
   @Override
   protected void setUp() throws Exception {
      super.setUp();
   }
   
   
   @Override
   protected void tearDown() throws Exception {
      super.tearDown();
   }

  
   /**
    * Test of parseLogLine method, of class LogParser.
    */
   public void testParseLogLine() {
      
      String line = "123.45.67.89 - - [27/Oct/2012:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\"";
      LogData expResult = new LogData("2012-10-27", "09:27", 10450, 200);
                  
      LogParser instance = new LogParser();      
      LogData result = instance.parseLogLine(line);
      System.out.println("Eql: " + (expResult.equals(result)));
      assertEquals(expResult, result);
      
   }
   
   
}
