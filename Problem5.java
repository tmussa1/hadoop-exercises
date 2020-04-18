/**
 * Problem5.java
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


public class Problem5 {
    
     
    public static class MaxFriendFinderMapper extends Mapper<Object, Text, Text, Text>{
        
        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException 
        {        
         String line = value.toString();
         String[] splitArr = line.split(";");
         String id = line.split(",")[0];
         Integer numOfFriends = 0;
         
         if(splitArr.length > 1){
             numOfFriends = splitArr[1].split(",").length;  
         }
        
         String idAppended = id + "\t" + numOfFriends;
             
         context.write(new Text("aggregator"), new Text(idAppended));
        }
    }
    
     public static class MaxFriendFinderReducer extends
        Reducer<Text, Text, Text, IntWritable> 
    {
        public void reduce(Text key, Iterable<Text> values, Context context) 
             throws IOException, InterruptedException 
        {
          String maxId = null;
          Integer maxCount = 0;
          
            for(Text value : values){
              String line = value.toString();
              if(Integer.parseInt(line.split("\t")[1]) > maxCount){
                 maxId = line.split("\t")[0];
                 maxCount = Integer.parseInt(line.split("\t")[1]);
              }
            }
            context.write(new Text(maxId), new IntWritable(maxCount));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "problem 5");
        job.setJarByClass(Problem5.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem3.java for comments describing the calls.

        job.setMapperClass(MaxFriendFinderMapper.class);
        job.setReducerClass(MaxFriendFinderReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //   job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
