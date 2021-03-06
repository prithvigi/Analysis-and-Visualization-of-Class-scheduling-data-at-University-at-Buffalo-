/* problem6
Authors:-> Prithvi Gollu Indrakumar
           UBId:pgolluin
           Person#:50169089

        -> Oshin Sanjay Patwa
           UBId:oshinsan
           Person#:50169203  

*/

//objective---> To find the top 5 halls having the maximum capacity for the year 2015

import java.io.IOException;
import java.util.*;
import java.io.StringReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class problem6 {

  public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
	private Text t1;
	private Text t2;  
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	  try {
        	String[] line = value.toString().split(",");
		String[] ln1 = line[1].split("\\s+");
		String[] ln2 = line[2].split("\\s+");
		int ln1yr= Integer.parseInt(ln1[1].trim());
		String hall;
		int reg=Integer.parseInt(line[7]);
		int cap=Integer.parseInt(line[8]);
                int sz = line.length;
		if(reg >=0){
		if((!ln2[0].equalsIgnoreCase("Unknown")) && (!ln2[0].equalsIgnoreCase("Arr")) && (sz<10) && (ln1yr ==2015))// Data cleaning 
                 {                                                                                                 //and data only for the year 2015 
			hall=(ln2[0]);
    			t1=new Text(hall); 
       			IntWritable val= new IntWritable(cap);
      			context.write(t1,val);   //(key:hall, value:capacity)
      		}
		}

     	  } catch (Exception e) {}
 	}


 }


 public static class Reducer1 extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
      	sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }


 public static class Mapper2
   extends Mapper<Object, Text, IntWritable, Text>{
   	Text t1 = new Text();	
	public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {

	try {
		String[] line = value.toString().split("\\s+");
     		t1=new Text(":"+line[0]);
    		IntWritable problem6= new IntWritable(Integer.parseInt(line[1].trim()));
        	context.write(problem6, t1);  //swapping key and value such that its sorted based on value
  	}catch (Exception e) {}
 	}
 }


 public static class Reducer2
   extends Reducer<IntWritable,Text,IntWritable,Text> {
	public void reduce(IntWritable key, Text values,
                  Context context
                  ) throws IOException, InterruptedException {
		
    		context.write(key,values);
	}
 }


 public static class Mapper3
       extends Mapper<Object, Text, IntWritable, Text>{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		try {
			context.write(new IntWritable(1),value);
  		}catch (Exception e) {}
 	}

 }


 public static class Reducer3 extends
    Reducer<IntWritable, Text, IntWritable, Text> {
 	public void reduce(IntWritable key, Iterable<Text> values,
        Context context)
        throws IOException , InterruptedException{   
 		List<String> ArrList = new ArrayList<String>();
		Iterator<Text> ite = values.iterator();
  		while(ite.hasNext()) {
   			Text t= ite.next();
 			ArrList.add(t.toString());
 		}
		for(int i=0;i<= 5;i++) {   // the halls are in sorted order now based on capacity 
  			String[] str =ArrList.get(i).split(":"); //displaying only top 5 halls with maximum capacity for the year 2015
			String valstr;
			valstr=str[1];
			Text t1 = new Text(valstr);
			context.write(new IntWritable(Integer.parseInt(str[0].trim())),t1);
		}
	}
 }


  public static void main(String[] args) throws Exception {
  	String temp1="prb6.1";
	String temp2="prb6.2";
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "To find the capacity for each hall");
    	job.setJarByClass(problem6.class);
    	job.setMapperClass(Mapper1.class);
    	job.setCombinerClass(Reducer1.class);
    	job.setReducerClass(Reducer1.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(temp1));
    	job.waitForCompletion(true);

    	Configuration conf2 = new Configuration();
   	Job job2 = Job.getInstance(conf2, "To sort the records on values ");
   	job2.setJarByClass(problem6.class);
    	job2.setMapperClass(Mapper2.class);
    	job2.setReducerClass(Reducer2.class);
    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job2, new Path(temp1));
    	FileOutputFormat.setOutputPath(job2, new Path(temp2));
 	job2.waitForCompletion(true);

	Configuration conf3 = new Configuration();
    	Job job3 = Job.getInstance(conf3, ": To display top 5 records");
    	job3.setJarByClass(problem6.class);
    	job3.setMapperClass(Mapper3.class);
    	job3.setReducerClass(Reducer3.class);
    	job3.setOutputKeyClass(IntWritable.class);
    	job3.setOutputValueClass(Text.class);
   	FileInputFormat.addInputPath(job3, new Path(temp2));
   	FileOutputFormat.setOutputPath(job3, new Path(args[1]));
   
	System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
