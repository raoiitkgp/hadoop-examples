
package com.codeofzen.hadoop.apachelog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * 
 * Parser for extracting information from Apache-Logfiles. 
 */
public class LogParser {
 
   private static final int NUM_FIELDS = 9;
   private static Pattern apacheLogRegEx = null;
   
   private static final SimpleDateFormat dateParser = new SimpleDateFormat("dd/MMM/yyyy", Locale.US);
      
   
   /**
    * Singleton-method for creating the RegEx used for parsing log-lines
    * @return 
    */
   public static Pattern getApacheLogRegEx() {
      if(apacheLogRegEx == null) {
         apacheLogRegEx = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
      }
      return apacheLogRegEx;
   }

   
   /**
    * Parses a single line from a Apache-Logifle
    * 
    * @param line Line of Apache-Logfile
    * @return Extracted information from line
    */
   public LogData parseLogLine(String line) {
      
      Pattern pattern = LogParser.getApacheLogRegEx();
      Matcher matcher = pattern.matcher(line);
      
      // check if line has been parsed correctly
      if( true == matcher.matches() && LogParser.NUM_FIELDS == matcher.groupCount() ) {
         String ipAddress = matcher.group(1);
         String dateTime  = matcher.group(4);
         String response  = matcher.group(6);
         String length    = matcher.group(7);
         
         try {
            
            // extract individual parts from string
            String[] dateTimeParts = dateTime.split(":");
           
            
            // format date
            String date = dateTimeParts[0];
            Date parsedDate = dateParser.parse(date);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(parsedDate);
            
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            date = format.format(calendar.getTime());

            
            // format time
            String time = dateTimeParts[1] + ":" + dateTimeParts[2];

            
            return new LogData(date, time, Long.parseLong(length, 10), Integer.parseInt(response, 10));
            
            
         } catch(ParseException e) {
            System.out.println("Could not parse date: "+ e );
         }
         
      }
      
      return null;
   }
   
}
