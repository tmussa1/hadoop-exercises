/**
 * Problem4.java
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


public class Problem4 {
      
    public static class EmailDomainMapper extends Mapper<Object, Text, Text, IntWritable>{
        
        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException 
        {        
         String line = value.toString();
         line = line.split(";")[0];
         String[] words = line.split(",");
         
         if(words.length >= 5 && words[4].indexOf("@") != -1){
             String domain = words[4].split("@")[1];
             context.write(new Text(domain), new IntWritable(1));
         }     
        }
    }
    
     public static class EmailDomainReducer extends
        Reducer<Text, IntWritable, Text, LongWritable> 
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
             throws IOException, InterruptedException 
        {
            long domainCount = 0;
            
            for (IntWritable val : values) {
                domainCount += val.get();
            }

            context.write(key, new LongWritable(domainCount));
        }
    }
     
       
    public static class EmailDomainMaxFinderMapper extends Mapper<Object, Text, Text, Text>{
        
        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException 
        {        
         context.write(new Text("aggregate"), value);
        }
    }
    
     public static class EmailDomainMaxFinderReducer extends
        Reducer<Text, Text, Text, LongWritable> 
    {
        public void reduce(Text key, Iterable<Text> values, Context context) 
             throws IOException, InterruptedException 
        {
            Long max = 0L;
            String maxKey = null;
                
            for(Text value : values){
                String[] token = value.toString().split("\t");
                
                if(Long.parseLong(token[1]) > max){
                  max = Long.parseLong(token[1]);
                  maxKey = token[0];
                }    
            }

            context.write(new Text(maxKey), new LongWritable(max));
        }
    }
    
    public static void main(String[] args) throws Exception {
        /*
  * First job in a chain of two jobs
  */
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "problem 4-1");
        job1.setJarByClass(Problem4.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem3.java for comments describing the calls.

        job1.setMapperClass(EmailDomainMapper.class);
        job1.setReducerClass(EmailDomainReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        //   job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);


        /*
  * Second job in a chain of two jobs
  */
        conf = new Configuration();
        Job job2 = Job.getInstance(conf, "problem 4-2");
        job2.setJarByClass(Problem4.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem3.java for comments describing the calls.

        job2.setMapperClass(EmailDomainMaxFinderMapper.class);
        job2.setReducerClass(EmailDomainMaxFinderReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        //job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);
    }
}
