/**
 * Problem6.java
 */

import java.io.IOException;
import java.time.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/* 
 * interfaces and classes for Hadoop data types that you may 
 * need for some or all of the problems from PS 4
 */
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Problem6 {
    
    public static class MutualFriendMapper extends Mapper<Object, Text, Text, Text>{
        
        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException 
        {        
         String line = value.toString();
         String[] splitArr = line.split(";");
         String user = line.split(",")[0];
             
         if(splitArr.length > 1){
             
             String[] friends = splitArr[1].split(",");
             String mutualKey = null;
             String mutualValue = splitArr[1];
             
             for(String friend : friends){
                 Integer userId = Integer.parseInt(user);
                 Integer friendId = Integer.parseInt(friend);
                 int compare = userId.compareTo(friendId);
                 if(compare < 0){
                    mutualKey = user + "," + friend; 
                 } else {
                    mutualKey = friend + "," + user; 
                 }
                 context.write(new Text(mutualKey), new Text(mutualValue));
             }
         }
            
        }
    }
    
     public static class MutualFriendReducer extends
        Reducer<Text, Text, Text, Text> 
    {
        
        public void reduce(Text key, Iterable<Text> values, Context context) 
             throws IOException, InterruptedException 
        {
            Set<String> valueSet = new HashSet<>();
            String mutuals = new String();
            
            for(Text val : values){
                String valStr = val.toString(); 
                String[] valArr = valStr.split(",");
                
                for(String singleVal : valArr){
                    if(valueSet.contains(singleVal)){
                        if(mutuals.length() == 0){
                            mutuals += singleVal;
                        } else {
                            mutuals += ", " + singleVal;
                        }
                    } else {
                       valueSet.add(singleVal);  
                    }
                }
            }
            
            context.write(key, new Text(mutuals));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "problem 6");
        job.setJarByClass(Problem6.class);


        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem3.java for comments describing the calls.

        job.setMapperClass(MutualFriendMapper.class);
        job.setReducerClass(MutualFriendReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
