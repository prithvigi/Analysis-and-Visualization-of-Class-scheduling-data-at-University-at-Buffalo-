/* problem4
Authors:-> Prithvi Gollu Indrakumar
           UBId:pgolluin
           Person#:50169089

        -> Oshin Sanjay Patwa
           UBId:oshinsan
           Person#:50169203  

*/

//objective---> To find the trends of time utilized by Baldy hall for consecutive years since 2000

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class problem4 {

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{
	private Text word = new Text();
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    		String[] fields=value.toString().split(","); //new array of 9 elements
    		if(fields[1].equals("Unknown")||fields[7].equals("")||!StringUtils.isNumeric(fields[7]))return;// Data  cleaning
       		int yr =Integer.parseInt(fields[1].split(" ")[1]);
  		if(yr<2000) return;
		String hallnly = fields[2].split(" ")[0];
      		if(hallnly.equals("Baldy")){
           		word.set(fields[1].split(" ")[1]);
    			context.write(word,new IntWritable(1)); //year, time utilized
       		}
    	}
 }


  public static class Reducer1
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    	private IntWritable result = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    		int sum=0;
    		for(IntWritable val:values){
    			sum+=val.get();
    		}
    		result.set(sum);
    		context.write(key,result);
    	}
  }


  public static class Mapper2
  extends Mapper<Object, Text, Text, IntWritable>{
  	Text word=new Text();
	public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
		String[] fields=value.toString().split("\\t"); // set the key to consecutive years to find the trends such that year1-year2
		String year1=fields[0]+"-"+(Integer.parseInt(fields[0])+1);
		String year2=Integer.toString(Integer.parseInt(fields[0])-1)+"-"+fields[0];
		word.set(year1);
		context.write(word,new IntWritable(Integer.parseInt(fields[1])));
		word.set(year2);
		context.write(word,new IntWritable(Integer.parseInt(fields[1])));
	}	
 }


  public static class Reducer2
  	extends Reducer<Text,IntWritable,Text,IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
		Iterator<IntWritable> iterator=values.iterator();// find the change in time for consecutive years
		int time1=iterator.next().get();
		if(!iterator.hasNext())return;
		int time2=iterator.next().get();
		time2=time2-time1;
		context.write(key,new IntWritable(time2));   //year1-year2,time utilized
	}
 }


  public static void main(String[] args) throws Exception {
	String temp="prb4.2";
   	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "get total number of students for every year");
    	job.setJarByClass(problem4.class);
    	job.setMapperClass(Mapper1.class);
    	job.setCombinerClass(Reducer1.class);
    	job.setReducerClass(Reducer1.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(temp));
    	job.waitForCompletion(true);
    	
	Configuration conf2 = new Configuration();
    	Job job2 = Job.getInstance(conf2, "get # of students increasing between consecutive years");
    	job2.setJarByClass(problem4.class);
    	job2.setMapperClass(Mapper2.class);
    	job2.setReducerClass(Reducer2.class);
    	job2.setOutputKeyClass(Text.class);
   	job2.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job2, new Path(temp));
    	FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    
	System.exit(job2.waitForCompletion(true) ? 0 : 1);   
  }
}
