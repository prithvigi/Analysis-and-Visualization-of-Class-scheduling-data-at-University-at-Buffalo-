/* problem9
Authors:-> Prithvi Gollu Indrakumar
           UBId:pgolluin
           Person#:50169089

        -> Oshin Sanjay Patwa
           UBId:oshinsan
           Person#:50169203  

*/

//objective---> to find out for which semster do stduents generally register i.e if maximum students register usually register in which semester 


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
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;


public class problem9 {

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{
	private Text t1;
	private Text t2;   
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	try {
       		String[] line = value.toString().split(",");
     		int size=line.length;
     		String classes;
     		if(size>9) return; //cleaning of data
     		if(line[1].equals("Unknown")||line[7].equals("")||(!StringUtils.isNumeric(line[7])))return;
     		if(Integer.parseInt(line[7])>0){
      			if(line[2].contains("")){		
        			t1=new Text(line[1].split(" ")[0]+":");
          			IntWritable regstrd= new IntWritable(Integer.parseInt(line[7].trim()));
           			context.write(t1,regstrd); //sesmter, number  regsired ex. fall,2000 winter,1000 etc..
         		}
      		}
	} catch (Exception e) {}
 	}
  }


  public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
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
	private Text t1;
	private Text t2;  
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	try {
      		
       		String[] line = value.toString().split(":");
        	int size=line.length;
     		t1=new Text(":"+line[0]);
      		IntWritable reg= new IntWritable(Integer.parseInt(line[1].trim()));
        	context.write(reg, t1);   //swap key value pairs
  	}  catch (Exception e) {}
 	}
  }


  public static class Reducer2 extends Reducer<IntWritable, Text, Text, IntWritable> {
  	private IntWritable result = new IntWritable();
 	public void reduce(IntWritable key, Text values, 
                       Context context
                       ) throws IOException, InterruptedException {
     
      		context.write(values, key);  // 5:   general chemistry
                                    	    // 10: psychology 
    	}                                      //12:  AI
 }


  public static class Mapper3
       extends Mapper<Object, Text, IntWritable, Text>{
	private Text t1;
	private Text t2;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	try {  
       		 context.write(new IntWritable(1),value); // 1,studentnum:course
  	}  catch (Exception e) {}
 	}
  }


  public static class Reducer3 extends
    Reducer<IntWritable, Text, IntWritable, Text> {
	public void reduce(IntWritable key, Iterable<Text> values,
        		Context context)
        		throws IOException , InterruptedException{   //20:geneyevmh 3:ai
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
       		context.write(new IntWritable(Integer.parseInt(str[0].trim())),t1); //number of stduents registered, fall
    	}
  }


  public static void main(String[] args) throws Exception {
   	String temp1="prb9.1";
   	String temp2="prb9.2";
	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "get total number of students regsitered for each semester over the years ");
    	job.setJarByClass(problem9.class);
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
    	job2.setJarByClass(problem9.class);
    	job2.setMapperClass(Mapper2.class);
    	job2.setReducerClass(Reducer2.class);
    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job2, new Path(temp1));
    	FileOutputFormat.setOutputPath(job2, new Path(temp2));
 	job2.waitForCompletion(true);

 	Configuration conf3 = new Configuration();
    	Job job3 = Job.getInstance(conf3, "Finding the semster for which maximum students have registered so far");
    	job3.setJarByClass(problem9.class);
    	job3.setMapperClass(Mapper3.class);
    	job3.setReducerClass(Reducer3.class);
    	job3.setOutputKeyClass(IntWritable.class);
    	job3.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job3, new Path(temp2));
    	FileOutputFormat.setOutputPath(job3, new Path(args[1]));
   
    	System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
