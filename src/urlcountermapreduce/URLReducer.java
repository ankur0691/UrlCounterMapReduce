/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package urlcountermapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer class for URL counter
 * @author ankur
 */
public class URLReducer extends Reducer<Text, Text, Text, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        HashMap<String, Integer> urlCount=new HashMap<>();
        String url=null;
        Iterator<Text> it=values.iterator();
        while (it.hasNext()) {
            url=it.next().toString();
            if(urlCount.get(url)==null)
                urlCount.put(url, 1);
            else
                urlCount.put(url,urlCount.get(url)+1);
        }
        for(Map.Entry<String, Integer> k:urlCount.entrySet())
            context.write(key, new Text(k.getKey()+" "+k.getValue()));
    }
}
