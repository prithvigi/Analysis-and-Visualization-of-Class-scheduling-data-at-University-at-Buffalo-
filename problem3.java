/* problem3
Authors:-> Prithvi Gollu Indrakumar
           UBId:pgolluin
           Person#:50169089

        -> Oshin Sanjay Patwa
           UBId:oshinsan
           Person#:50169203  

*/

//objective---> To find which hall utilized the maximum time since the year 2000


import java.util.*;
import java.io.*;
import java.io.StringReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class problem3 {

  public static class Mapper1
       		extends Mapper<Object, Text, Text, IntWritable>{
	private Text t1;
	private Text t2;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	try {
       		String[] line = value.toString().split(",");  
    		String yearstr = line[1].replaceAll("\\D+","");
    		String timestr=line[4].trim();
              	String[] hall =line[2].split(" ");
                if(hall[1].equals("Arr")) return;
    		int yr=Integer.parseInt(yearstr);
		if(line[2].equals("Unknown") || line[2].equals("Arr Arr")) return;//Data cleaning
		if(line[1].equals("Unknown")||line[7].equals("")|| (!StringUtils.isNumeric(line[7])))return;
 		if(timestr.equals("Before 8:00AM") || timestr.equals("Unknown"))return;
		if(yr>1999) { //considered only years since 1999
  			String halls = hall[0]+":";    
    			t1=new Text(halls);
    			context.write(t1,new IntWritable(1));
		}
	} catch (Exception e) {}
 	}
  }


  public static class Reducer1 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
 	private IntWritable result = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
	private Text t1;
	private Text t2;
   	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	try {
		String[] line = value.toString().split("\\:");
     		int size=line.length;
 		//Swap the key value pairs
 		t1=new Text(":"+line[0]);
    		IntWritable reg= new IntWritable(Integer.parseInt(line[1].trim()));
        	context.write(reg, t1);
  	} catch (Exception e) {}
 	}
  }


  public static class Reducer2 extends Reducer<IntWritable, Text, Text, IntWritable> {
  	private IntWritable result = new IntWritable();
 	public void reduce(IntWritable key, Text values, 
                       Context context
                       ) throws IOException, InterruptedException {
     		//display sorted by values
      		context.write(values, key);  
    	}                               
 }


  public static class Mapper3
       extends Mapper<Object, Text, IntWritable, Text>{
	private Text t1;
	private Text t2;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	try {
		context.write(new IntWritable(1),value); // 1,time:hall
                                                  	//1,time1:hall1
  	} catch (Exception e) {}
 	}
  }


  public static class Reducer3 extends
    Reducer<IntWritable, Text, IntWritable, Text> {
	public void reduce(IntWritable key, Iterable<Text> values,
        	Context context)
        	throws IOException , InterruptedException{   	//Finding the max , accessing the elements of the array
                                                      		//arr->time:hall,time1:hall1  
 		List<String> vArrayList = new ArrayList<String>();
		Iterator<Text> ite = values.iterator();
  		while(ite.hasNext()) {
   			Text t= ite.next();
 			vArrayList.add(t.toString());
 		}
		int i=0;
 		String[] str =vArrayList.get(0).split(":");
		String valstr;
		valstr=str[1];
		Text t1 = new Text(valstr);
		context.write(new IntWritable(Integer.parseInt(str[0].trim())),t1);
	}
 }


   public static void main(String[] args) throws Exception {
   	String temp1="prb3.1";
	String temp2="prb3.2";
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "Time utilized by each hall");
    	job.setJarByClass(problem3.class);
    	job.setMapperClass(Mapper1.class);
    	job.setCombinerClass(Reducer1.class);
    	job.setReducerClass(Reducer1.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(temp1));
    	job.waitForCompletion(true);

    	Configuration conf2 = new Configuration();
    	Job job2 = Job.getInstance(conf2, "sort by values");
   	job2.setJarByClass(problem3.class);
    	job2.setMapperClass(Mapper2.class);
    	job2.setReducerClass(Reducer2.class);
    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job2, new Path(temp1));
    	FileOutputFormat.setOutputPath(job2, new Path(temp2));
 	job2.waitForCompletion(true);

 	Configuration conf3 = new Configuration();
    	Job job3 = Job.getInstance(conf3, "display the max value i.e the hall that has utilized the maximum ");
    	job3.setJarByClass(problem3.class);
    	job3.setMapperClass(Mapper3.class);
    	job3.setReducerClass(Reducer3.class);
    	job3.setOutputKeyClass(IntWritable.class);
    	job3.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job3, new Path(temp2));
   	FileOutputFormat.setOutputPath(job3, new Path(args[1]));
   
   	 System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
