/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package urlcountermapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TimeZone;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * The mapper class for URL counter
 * @author ankur
 */
public class URLMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println(value);
        StringTokenizer st=new StringTokenizer(value.toString());
        while(st.hasMoreTokens()){
            String[] s = st.nextToken().split("\\|");
            context.write(new Text(epochToDateConverter(Long.parseLong(s[0]))), new Text(s[1]));
        }
    }
    
    private static String epochToDateConverter(Long timestamp){
        Date epochDate = new Date(timestamp*1000);
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/YYYY");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        String dateString = sdf.format(epochDate);
        return dateString;
    }
    
}
