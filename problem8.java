/* problem8
Authors:-> Prithvi Gollu Indrakumar
           UBId:pgolluin
           Person#:50169089

        -> Oshin Sanjay Patwa
           UBId:oshinsan
           Person#:50169203  

*/

//objective---> To find which hall was most efficiently utilized for each semester in each year since the year 2000

import java.io.IOException;
import java.util.*;
import java.io.StringReader;
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

 public class problem8 {

  public static class Mapper1 
       extends Mapper<Object, Text, Text, IntWritable>{
	private Text t1;
	private Text t2;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		try {
			String[] line = value.toString().split(",");
			String[] ln1 = line[1].split("\\s+");
			String[] ln2 = line[2].split("\\s+");
			int ln1yr= Integer.parseInt(ln1[1].trim());
			String yearhall = (ln1[1]+"_"+ln1[0]+":"+line[2]+":");     
    			t1=new Text(yearhall);
			int sz = line.length;
			int l6=Integer.parseInt(line[7].trim());
			int l7=Integer.parseInt(line[8].trim());
			int diff= l7-l6;
			IntWritable l6w = new IntWritable(diff);
			if(l6 >=0){
			if((!ln2[0].equalsIgnoreCase("Unknown")) && (!ln2[0].equalsIgnoreCase("Arr")) && (sz<10) && (ln1yr >= 2000)){// data cleaning
				if( l6<l7 && diff>10){   // find difference between capacity of a room and registered number of students 
					context.write(t1,l6w);
				}
			}
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
       extends Mapper<Object, Text, Text, IntWritable>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		String[] line = value.toString().split(":");
		String[] ln1 =line[1].split(" ");
		if(Integer.parseInt(line[2].trim()) <= 25) {  // considering halls_rooms with difference less than or equal to25 
        		String yearhall = (line[0]+":"+ln1[0]+":");    // because lesser the difference, more efficiently it was atilized
    			Text t1=new Text(yearhall);   // only need number of rooms in a hall that are efficiently utilized 
    			context.write(t1,new IntWritable(1)); //hence passing value=1
		}
  		
 	}

 }


 public static class Reducer2 
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

 public static class Mapper3
       extends Mapper<Object, Text, IntWritable, Text>{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		try {
			context.write(new IntWritable(1),value);
  		}catch (Exception e) {}
 	}

 }


 public static class Reducer3 extends
    Reducer<IntWritable, Text, Text, IntWritable> {
	Text ky;
 	public void reduce(IntWritable key, Iterable<Text> values,
        Context context)
        throws IOException , InterruptedException{   
 		List<String> ArrList = new ArrayList<String>();
		Iterator<Text> ite = values.iterator();
  		while(ite.hasNext()) {
   			Text t= ite.next();
 			ArrList.add(t.toString());
 		}
		String yeartime;
  		int size = ArrList.size();
		String[] str =ArrList.get(size-1).split(":");
		String tempyr = str[0].trim();   // Initializing with first arraylist element values
		String temphall = str[1].trim();
		int max=Integer.parseInt(str[2].trim());
		for(int i=size-2;i >=0;i--) {    // parsing the entire arraylist one element at a time
			String ln[]=ArrList.get(i).split(":");
			if((ln[0].trim()).equals(tempyr)){   //is previoue element year and current element year is same  
				int v=Integer.parseInt(ln[2].trim());
				if( v > max){    // check which has maximum number of classes for 2 different time slots
					max=Integer.parseInt(ln[2].trim());
					temphall=ln[1].trim();

				}
                           }
			else {	
				yeartime=(tempyr+"_"+temphall);
    				ky=new Text(yeartime);
				IntWritable val= new IntWritable(max); 
				context.write(ky,val);   // writing the hall which was most efficiently utilized for that year
				tempyr=ln[0].trim();    // based on the hall which has the most number of rooms with difference less than 25
				temphall=ln[1].trim();
				max=Integer.parseInt(ln[2].trim());
			}
		}
	}
 }



 
  public static void main(String[] args) throws Exception {
  	String temp1="prb8.1";
	String temp2="prb8.2";
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "To calculate the difference between max capacity-registered students");
    	job.setJarByClass(problem8.class);
    	job.setMapperClass(Mapper1.class);
    	job.setCombinerClass(Reducer1.class);
    	job.setReducerClass(Reducer1.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(temp1));
    	job.waitForCompletion(true);

    	Configuration conf2 = new Configuration();
   	Job job2 = Job.getInstance(conf2, "calculated on the basis of difference between max capacity-registered students(diff<=25)");
   	job2.setJarByClass(problem8.class);
    	job2.setMapperClass(Mapper2.class);
    	job2.setReducerClass(Reducer2.class);
    	job2.setOutputKeyClass(Text.class);
    	job2.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job2, new Path(temp1));
    	FileOutputFormat.setOutputPath(job2, new Path(temp2));
	job2.waitForCompletion(true);

	Configuration conf3 = new Configuration();
    	Job job3 = Job.getInstance(conf3, "To find a hall for each year with  maximum number of most efficiently utilized rooms");
    	job3.setJarByClass(problem8.class);
    	job3.setMapperClass(Mapper3.class);
    	job3.setReducerClass(Reducer3.class);
    	job3.setOutputKeyClass(IntWritable.class);
    	job3.setOutputValueClass(Text.class);
   	FileInputFormat.addInputPath(job3, new Path(temp2));
   	FileOutputFormat.setOutputPath(job3, new Path(args[1]));
   
	System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
