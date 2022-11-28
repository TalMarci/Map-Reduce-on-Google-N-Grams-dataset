package jobs.firstJob;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.regions.Regions;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//import adapters.AwsBundle;
import adapters.Consts;


public class FirstStep {

	public static class MapWordCount extends Mapper<LongWritable, Text, FirstStepKey, LongWritable>{

		private FirstStepKey initialKey;
		private final Pattern legalPattern = Pattern.compile("[\u05D0-\u05EB]{2,}?");

		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException
		{
			// value format is : ngram TAB year TAB match_count TAB volume_count NEWLINE
			String valueSplitted[] = value.toString().split("\t");
			String ngram[] = valueSplitted[0].split(" ");
			if(ngram.length != 3) return;
			String firstWord = ngram[0], secondWord = ngram[1], thirdWord = ngram[2];
			if(legalPattern.matcher(firstWord).find() && 
					legalPattern.matcher(secondWord).find() &&
			 			legalPattern.matcher(thirdWord).find())
			{		 
			LongWritable numberOfAppearances = new LongWritable (Long.parseLong(valueSplitted[2]));	

			initialKey = new FirstStepKey(firstWord,secondWord, thirdWord);
			output.write(initialKey, numberOfAppearances);

			initialKey = new FirstStepKey(secondWord, thirdWord,"*");
			output.write(initialKey, numberOfAppearances);

			initialKey = new FirstStepKey(firstWord,secondWord, "*");
			output.write(initialKey, numberOfAppearances);


			initialKey = new FirstStepKey(firstWord,"*", "*");
			output.write(initialKey, numberOfAppearances);

			initialKey = new FirstStepKey(secondWord,"*", "*");
			output.write(initialKey, numberOfAppearances);

			initialKey = new FirstStepKey(thirdWord, "*", "*");
			output.write(initialKey, numberOfAppearances);

			initialKey = new FirstStepKey("C 0", "*", "*");
			output.write(initialKey, numberOfAppearances);
			

			}
		}
	}
	public static class CombinerWordCount extends Reducer<FirstStepKey, LongWritable, FirstStepKey, LongWritable>{
	
		@Override
		public void reduce(FirstStepKey key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException
		{
			long sum = 0L;
			for(LongWritable value : values)
			{
				System.out.println(value);
				sum += value.get();
			}
			output.write(key, new LongWritable(sum));
		}
	}

	public static class ReduceWordCount extends Reducer<FirstStepKey, LongWritable, Text, LongWritable>{
		 
		private void sendDen(Context output, Long sum){
			AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
			String tempPath = output.getConfiguration().get("tempFilesPath");
			String sumString = String.valueOf(sum);
			InputStream stream = new ByteArrayInputStream( sumString.getBytes());
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(sumString.getBytes().length);
			PutObjectRequest request = new PutObjectRequest(tempPath, "C0", stream ,metadata);   
			s3.putObject(request); 
		}
		@Override
		public void reduce(FirstStepKey key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException
		{
			
			Long sum = 0L;
			for(LongWritable value : values)
			{
				System.out.println(value);
				sum += value.get();
			}
			if(key.getFirstWord().toString().equals("C 0")){
				sendDen(output, sum);
			}
			else	
				output.write(new Text(key.getFirstWord().toString() + " " + key.getSecondWord().toString() + " " +key.getThirdWord().toString()), 
							new LongWritable(sum));
		}
	}

	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		Path input=new Path(args[0]);
		String uuid = args[1];
		Path output=new Path(String.format("s3://%s/firstStep-output/%s/",Consts.Bucket_name, uuid));//provide bucket name
		String tempFilesPath = String.format("%s/temp-files/%s",Consts.Bucket_name, uuid);//provide bucket name
		conf.set("tempFilesPath", tempFilesPath);

		Job job = Job.getInstance(conf,"first-step");
		job.setJarByClass(FirstStep.class);
		job.setMapperClass(MapWordCount.class);
		job.setReducerClass(ReduceWordCount.class);
		job.setCombinerClass(CombinerWordCount.class);
		job.setMapOutputKeyClass(FirstStepKey.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}




