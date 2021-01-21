package org.ymx;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss-07:00");
//        Date date=new Date();
//        String dateString=simpleDateFormat2.format(date);
        try {
            Date parse = simpleDateFormat2.parse("2020-09-06T20:44:27-07:00");
            System.out.println(parse);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
