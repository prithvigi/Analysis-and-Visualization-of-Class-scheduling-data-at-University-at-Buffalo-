/* problem7
Authors:-> Prithvi Gollu Indrakumar
           UBId:pgolluin
           Person#:50169089

        -> Oshin Sanjay Patwa
           UBId:oshinsan
           Person#:50169203  

*/

//objective---> To find the trends and compare the number of students registered for online course v/s those on campus for a particular course for each year

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
import org.apache.hadoop.io.ArrayWritable;

public class problem7 {

  
  public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
	private Text t1;
	private Text t2;  
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	  try {
        	String[] line = value.toString().split(",");
		String[] ln1 = line[1].split("\\s+");
		String[] ln2 = line[2].split("\\s+");
		int ln1yr= Integer.parseInt(ln1[1].trim());
		String Crsyrhall;
		int reg=Integer.parseInt(line[7]);
		int sz = line.length;
		if(reg >=0){
		if((!ln2[0].equalsIgnoreCase("Unknown")) && (!ln2[0].equalsIgnoreCase("Arr")) && (sz<10) && (ln1yr >= 2000)){// Data cleaning
			if((ln2[0].trim()).equalsIgnoreCase("Online")){ 
				context.write(new Text(line[6]+"_"+ln1[1].trim()+"_"+"Online"+"_"),new IntWritable(reg)); 
			}else {   // other than online then Write as OnCampus
       				context.write(new Text(line[6]+"_"+ln1[1].trim()+"_"+"OnCampus"+"_"),new IntWritable(reg)); 
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
    		
			context.write(new IntWritable(1),value);
  		
 	}

 }


  public static class Reducer2 extends
    Reducer<IntWritable, Text, Text, Text> {
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
		String Crsyr;
  		int size = ArrList.size();
		String[] str =ArrList.get(size-1).split("_");
		String tcrsyr = (str[0]+"_"+str[1]);   // Initializing with first arraylist element values
		String thall = str[2].trim();
		int treg=Integer.parseInt(str[3].trim());
		for(int i=size-2;i >=0;i--) {    // parsing the entire arraylist one element at a time
			String ln[]=ArrList.get(i).split("_");
			String crsyr = (ln[0]+"_"+ln[1]);
			if((crsyr.equalsIgnoreCase(tcrsyr))&& (thall.equalsIgnoreCase("OnCampus")) && ((ln[2].trim()).equalsIgnoreCase("Online"))){
				// If a course is both Online and Oncampus only then display both it for each year above 2000			
				Text ky=new Text(crsyr);     
				String val=("Online:"+ln[3].trim()+"	OnCampus:"+treg);
				Text value= new Text(val); 
				context.write(ky,value);
			} else {
				tcrsyr = crsyr;
				thall = ln[2].trim();
				treg=Integer.parseInt(ln[3].trim());
			}
		}
	}catch (Exception e) {}
	}
 }

 
  public static void main(String[] args) throws Exception {
  	String temp="prb71";
	
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "To find the number of students registered for online and on campus course (for each course)");
    	job.setJarByClass(problem7.class);
    	job.setMapperClass(Mapper1.class);
    	job.setCombinerClass(Reducer1.class);
    	job.setReducerClass(Reducer1.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(temp));
    	job.waitForCompletion(true);

    	Configuration conf2 = new Configuration();
   	Job job2 = Job.getInstance(conf2, "to filter courses which offers both online and oncampus for each year");
   	job2.setJarByClass(problem7.class);
    	job2.setMapperClass(Mapper2.class);
    	job2.setReducerClass(Reducer2.class);
    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job2, new Path(temp));
    	FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	
	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
