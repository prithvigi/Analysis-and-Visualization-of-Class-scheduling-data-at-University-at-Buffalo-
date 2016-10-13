/* problem5
Authors:-> Prithvi Gollu Indrakumar
           UBId:pgolluin
           Person#:50169089

        -> Oshin Sanjay Patwa
           UBId:oshinsan
           Person#:50169203  

*/

//objective---> What is the peak time for which most number of classes are held in year amongst all the days

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

public class problem5 {

 public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
	private Text ky;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	  try {
        	String[] line=value.toString().split(","); //new array of 9 elements
		String yeartime;
		int lnth;
		String[] ln2 = line[2].split("\\s+");
		String ln3 = line[3].trim();
		int reg=Integer.parseInt(line[7]);
        	int sz = line.length;
		if((!ln2[0].equalsIgnoreCase("Unknown")) && (!ln2[0].equalsIgnoreCase("Arr")) && (sz<10) && (reg>0)) { // data cleaning
			if(!ln3.equals("UNKWN") && (!ln3.equals("Arr"))){
				if(ln3.equals("M-F")) {lnth=5;}  //monday to friday= 5 days
				else if(ln3.equals("M-S")) {lnth=6;}// monday to saturday= 6 days
				else{lnth=ln3.length();} // MWF= length = 3days
				if((!line[4].equalsIgnoreCase("Unknown")) && (!line[4].equalsIgnoreCase("Before 8:00AM"))){	
				     yeartime=(line[1].split(" ")[1]+"_"+line[4]+"_");//adding all classes held after 8am forthat year at that time
    				     ky=new Text(yeartime);
				     IntWritable val= new IntWritable(lnth);    	
    				     context.write(ky,val);
				}
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

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		try {
			context.write(new IntWritable(1),value);
  		}catch (Exception e) {}
 	}
 }


  public static class Reducer2 extends
  	  Reducer<IntWritable, Text, Text, IntWritable> {
	Text ky;
 	public void reduce(IntWritable key, Iterable<Text> values,
        Context context)
        throws IOException , InterruptedException{   
 	try{	
		List<String> ArrList = new ArrayList<String>();
		Iterator<Text> ite = values.iterator();
  		while(ite.hasNext()) {
   			Text t= ite.next();
 			ArrList.add(t.toString());
 		}
		String yeartime;
  		int size = ArrList.size();
		String[] str =ArrList.get(size-1).split("_");// Initializing with first arraylist element values
		String tempyr = str[0].trim();
		String temproblem5 = str[1].trim();
		int max=Integer.parseInt(str[2].trim());
		for(int i=size-2;i >=0;i--) { // parsing the entire arraylist one element at a time
			String ln[]=ArrList.get(i).split("_");
			if((ln[0].trim()).equals(tempyr)){ //is previoue element year and current element year is same  
				int v=Integer.parseInt(ln[2].trim());
				if( v > max){          // check which has maximum number of classes for 2 different time slots
					max=Integer.parseInt(ln[2].trim());
					temproblem5=ln[1].trim();

				}
                           }
			else {	
				yeartime=(tempyr+"_"+temproblem5);
    				ky=new Text(yeartime);
				IntWritable val= new IntWritable(max); 
				context.write(ky,val); // writing the year with time slot which has max number of classes held  
				tempyr=ln[0].trim();  // as the current element is for the next year
				temproblem5=ln[1].trim();
				max=Integer.parseInt(ln[2].trim());
			}
		}
	}catch (Exception e) {}
	}
	
 }

 
  public static void main(String[] args) throws Exception {
  	String temp="prb5";
	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "To get how many classes are held respective to each time period");
    	job.setJarByClass(problem5.class);
    	job.setMapperClass(Mapper1.class);
    	job.setCombinerClass(Reducer1.class);
    	job.setReducerClass(Reducer1.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(temp));
    	job.waitForCompletion(true);

    	Configuration conf2 = new Configuration();
   	Job job2 = Job.getInstance(conf2, "To get the time for which maximum classes are held in each year respectively");
   	job2.setJarByClass(problem5.class);
    	job2.setMapperClass(Mapper2.class);
    	job2.setReducerClass(Reducer2.class);
    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job2, new Path(temp));
    	FileOutputFormat.setOutputPath(job2, new Path(args[1]));
 	
	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}



