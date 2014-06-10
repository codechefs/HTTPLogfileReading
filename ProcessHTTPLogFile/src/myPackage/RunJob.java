package myPackage;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.io.*;

public class RunJob {

	 public static void main(String[] args) throws Exception {
		 JobConf conf1 = new JobConf(RunJob.class);
		 JobConf conf2 = new JobConf(RunJob.class);
		 JobConf conf3 = new JobConf(RunJob.class);
	     
	      conf1.setJobName("HostName Counting");
	      conf2.setJobName("Browser Counting");
	      conf3.setJobName("Status Code");
	
	      conf1.setOutputKeyClass(Text.class);
	      conf1.setOutputValueClass(IntWritable.class);
	      conf2.setOutputKeyClass(Text.class);
	      conf2.setOutputValueClass(IntWritable.class);
	      conf3.setOutputKeyClass(Text.class);
	      conf3.setOutputValueClass(IntWritable.class);
	      	      
	      conf1.setMapperClass(MyMapper_HostName.class);
	      conf1.setCombinerClass(MyReducer_HostName.class);
	      conf1.setReducerClass(MyReducer_HostName.class);
	
	      conf2.setMapperClass(MyMapper_Browser.class);
	      conf2.setCombinerClass(MyReducer_Browser.class);
	      conf2.setReducerClass(MyReducer_Browser.class);
	      
	      conf3.setMapperClass(MyMapper_StatusCode.class);
	      conf3.setCombinerClass(MyReducer_StatusCode.class);
	      conf3.setReducerClass(MyReducer_StatusCode.class);
	      
	      conf1.setInputFormat(TextInputFormat.class);
	      conf1.setOutputFormat(TextOutputFormat.class);
	      conf2.setInputFormat(TextInputFormat.class);
	      conf2.setOutputFormat(TextOutputFormat.class);
	      conf3.setInputFormat(TextInputFormat.class);
	      conf3.setOutputFormat(TextOutputFormat.class);
	
	      FileInputFormat.setInputPaths(conf1, new Path(args[0]));
	      FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
	      FileInputFormat.setInputPaths(conf2, new Path(args[0]));
	      FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
	      FileInputFormat.setInputPaths(conf3, new Path(args[0]));
	      FileOutputFormat.setOutputPath(conf3, new Path(args[3]));
	try{
	      JobClient.runJob(conf1);
	      JobClient.runJob(conf2);
	      JobClient.runJob(conf3);	  
	      
	      
	}catch(Exception e){
		System.out.println("Error in Running Jobs...");
	}
	
	Display(args[1],args[2]);
	CalculateSuccessRate(args[3]);
	      
	}
	 
	 private static void Display(String str1,String str2){
		 String path=null,line=null;
		 path=str1+"/part-00000";
		 BufferedReader br=null;
		 FileReader file=null;
		 try{
			 
			 file=new FileReader(path);
			 br=new BufferedReader(file);
			 line=null;
			 System.out.println("\t\t########## HostName call count ##########");
			 try {
				while((line=br.readLine())!=null)
				 	System.out.println(line);	
				br.close();
				 }catch(IOException e){					 
				 }
			  
			 
		 }catch(FileNotFoundException e){}
		 
		 path=str2+"/part-00000";
		 try{
			 
			 file=new FileReader(path);
			 br=new BufferedReader(file);
			 line=null;
			 System.out.println("\t\t########## Browsers call count ##########");
			 try {
				while((line=br.readLine())!=null)
				 	System.out.println(line);	
				br.close();
				 }catch(IOException e){					 
				 }
			  
			 
		 }catch(FileNotFoundException e){}
	 }
	 private static void CalculateSuccessRate(String str){
		 String path=str+"/part-00000";
		 int success=0,redirect=0,error=0;
		 try{
			 System.out.println("\n\t\t########Success Rate Stats #######");
			 FileReader file=new FileReader(path);
			 BufferedReader br=new BufferedReader(file);
			 String line=null;
			 try {
				while((line=br.readLine())!=null)
				 {
					 String[] tokens=line.split("\t");
					 int statuscode=Integer.parseInt(tokens[0]);
					 switch(statuscode/100 % 10){
					 case 2:success+=Integer.parseInt(tokens[1]);break;
					 case 3:redirect+=Integer.parseInt(tokens[1]);break;
					 case 4:error+=Integer.parseInt(tokens[1]);break;
					 }					 
				 }
				br.close();
				System.out.println("No. of Successes:"+success);
				System.out.println("No. of Redirects:"+redirect);
				System.out.println("No. of Errors:"+error);
				System.out.println("Success Rate(success/error): "+(float)success/(float)error);
				
			} catch (IOException e) {				
			}
			 
		 }catch(FileNotFoundException e){
			 
		 }
		 
	 }
}
