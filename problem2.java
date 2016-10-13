/* problem2
Authors:-> Prithvi Gollu Indrakumar
           UBId:pgolluin
           Person#:50169089

        -> Oshin Sanjay Patwa
           UBId:oshinsan
           Person#:50169203  

*/

//objective---> To find the trends of students registered for General Chemistry  over the years for various semesters

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

public class problem2 {

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{
	private Text course;
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    		String[] fields=value.toString().split(","); //new array of 9 elements
   		int yr = Integer.parseInt(fields[1].split(" ")[1]);
		String[] ar=fields[2].split("\\s+");
		if((ar[0].equalsIgnoreCase("Unknown")) || (ar[0].equals("Arr"))) return;
    		if(fields[1].equals("Unknown")||fields[7].equals("")||(!StringUtils.isNumeric(fields[7])))return;
       		if (fields[6].equals("General Chemistry")) {
        		if(Integer.parseInt(fields[7].trim())<=0)return;
        		String yrsm = fields[1]+"_"+fields[6]+":"; 
       			int reg=Integer.parseInt(fields[7]);
      			IntWritable registrd= new IntWritable(reg);
   			Text year_semester= new Text(yrsm);
    			context.write(year_semester,registrd);  //semester year,students registered
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
  	extends Mapper<Object, Text, IntWritable, Text>{
	private Text t1;
	private Text t2;
   	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	try {
		//swapping the key value pairs
       		String[] line = value.toString().split("\\:");
     		int size=line.length;
 		t1=new Text(":"+line[0]);
		IntWritable maxcap= new IntWritable(Integer.parseInt(line[1].trim()));
        	context.write(maxcap, t1);
  	} catch (Exception e) {}
 	}
  }


  public static class Reducer2 extends Reducer<IntWritable, Text, Text, IntWritable> {
  	private IntWritable result = new IntWritable();
 	public void reduce(IntWritable key, Text values, 
                       Context context
                       ) throws IOException, InterruptedException {
    		//display in sorted order sorted by values 
      		context.write(values, key);
    	}
 }


  public static void main(String[] args) throws Exception {
    	String temp="prb2";
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "get total number of students registered for General Chemistry each year");
    	job.setJarByClass(problem2.class);
    	job.setMapperClass(Mapper1.class);
    	job.setCombinerClass(Reducer1.class);
    	job.setReducerClass(Reducer1.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(temp));
    	job.waitForCompletion(true);

    	Configuration conf2 = new Configuration();
    	Job job2 = Job.getInstance(conf2, "sort by values");
    	job2.setJarByClass(problem2.class);
   	job2.setMapperClass(Mapper2.class);
   	job2.setReducerClass(Reducer2.class);
    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job2, new Path(temp));
    	FileOutputFormat.setOutputPath(job2, new Path(args[1]));
   
    	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
