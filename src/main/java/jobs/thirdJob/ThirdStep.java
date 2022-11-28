package jobs.thirdJob;


import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.regions.Regions;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//import adapters.AwsBundle;
import adapters.Consts;


public class ThirdStep {

	public static class MapGetParams extends Mapper<LongWritable, Text, ThirdStepKey, Text>{

		@Override
		public void map(LongWritable filename, Text line, Context output) throws IOException, InterruptedException
		{	
            String[] parsedLine = line.toString().split("\t");
			String[] inputWords = parsedLine[0].split(" ");		
            output.write(new ThirdStepKey(inputWords[0], inputWords[1], inputWords[2]), new Text(parsedLine[1]));
        }
	}

	public static class ReducerCalculate extends Reducer<ThirdStepKey, Text, Text, DoubleWritable>{
		Long c0;
		public void setup(Context output) throws IOException {
			AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
			S3Object s3Object = s3.getObject(new GetObjectRequest(output.getConfiguration().get("tempFilesPath"), "C0"));
			BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
			c0 = Long.parseLong(reader.readLine());
		}
		@Override
		public void reduce(ThirdStepKey key, Iterable<Text> values, Context output) throws IOException, InterruptedException
		{
            Double num;
			Double[] params = {0d,0d,0d,0d,0d};// c1, n1, c2, n2, n3
            for(Text t : values){
              String[] input = t.toString().split(" ");
              for(int i=0;i<5;i++){
                  num=Double.parseDouble((input[i]));
                  if(num!=-1)
                    params[i]=num;
              }
            }
            // calculate the probability of P(W3|w1,w2)
            Double n1 = params[1], n2=params[3], n3= params[4], c1=params[0], c2=params[2];
            double k2=(Math.log(n2+1)+1) / (Math.log(n2+1)+2);
            double k3=(Math.log(n3+1)+1) / (Math.log(n3+1)+2); 
            double p = k3*n3/c2 + (1-k3)*k2*n2/c1 + (1-k3)*(1-k2)*n1/c0;
			output.write(new Text(key.toString()), new DoubleWritable(p));
	    }
	}

	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		String uuid = args[0];
		Path inputPath = new Path(String.format("s3://%s/secondStep-output/%s/", Consts.Bucket_name, uuid));
		Path output=new Path(String.format("s3://%s/thirdStep-output/%s/",Consts.Bucket_name, uuid));//provide bucket name
		String tempFilesPath = String.format("%s/temp-files/%s",Consts.Bucket_name, uuid);//provide bucket name
		conf.set("tempFilesPath", tempFilesPath);

		Job job = Job.getInstance(conf,"third-step");
		job.setJarByClass(ThirdStep.class);
		job.setMapperClass(MapGetParams.class);
		job.setReducerClass(ReducerCalculate.class);
		job.setMapOutputKeyClass(ThirdStepKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);

	}
}

