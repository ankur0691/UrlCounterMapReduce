
package urlcountermapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author ankur
 * The driver class for mapreduce job
 * Time Complexity O(nlogn)
 */
public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Driver(), args);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("urlcount");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(URLMapper.class);
        job.setReducerClass(URLReducer.class);  
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path("output"+System.currentTimeMillis()));

        job.setJarByClass(Driver.class);
        job.submit();

        return 1;
    }

}
