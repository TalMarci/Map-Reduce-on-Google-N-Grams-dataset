package jobs.fourthJob;


import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//import adapters.AwsBundle;
import adapters.Consts;


public class FourthStep {

	public static class MapValToKey extends Mapper<LongWritable, Text, FourthStepKey, Text>{

		@Override
		public void map(LongWritable filename, Text line, Context output) throws IOException, InterruptedException
		{	
            String[] parsedLine = line.toString().split("\t");
			String[] inputWords = parsedLine[0].split(" ");		
            output.write(new FourthStepKey(inputWords[0]+" "+inputWords[1]+" "+inputWords[2]+"\t"+parsedLine[1]), new Text(""));
        }
	}



	public static class ReducerSort extends Reducer<FourthStepKey, Text, Text, Text>{
		
		@Override
		public void reduce(FourthStepKey key, Iterable<Text> values, Context output) throws IOException, InterruptedException
		{
			String[] keyVal = key.getKey().toString().split("\t");
			output.write(new Text(keyVal[0]), new Text(keyVal[1]));
	    }
	}

	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		String uuid = args[0];
		Path inputPath = new Path(String.format("s3://%s/thirdStep-output/%s/", Consts.Bucket_name, uuid));
		Path output=new Path(String.format("s3://%s/fourthStep-output/%s/",Consts.Bucket_name, uuid));//provide bucket name
		String tempFilesPath = String.format("s3://%s/temp-files/%s/",Consts.Bucket_name, uuid);//provide bucket name
		conf.set("tempFilesPath", tempFilesPath);

		Job job = Job.getInstance(conf,"fourth-step");
		job.setJarByClass(FourthStep.class);
		job.setMapperClass(MapValToKey.class);
		job.setReducerClass(ReducerSort.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(FourthStepKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);

	}
}