package jobs.secondJob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import adapters.Consts;

public class SecondStep {

	public static class MapGetParams extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable filename, Text line, Context output) throws IOException, InterruptedException {
			String[] parsedLine = line.toString().split("\t");
			Long occ = Long.parseLong(parsedLine[1]);
			String[] inputWords = lineToStringArray(parsedLine[0]);
			Text key1;
			Text key2;
			Text newValue;

			if (inputWords.length == 3) {

				key1 = new Text(inputWords[0] + " " + inputWords[1]);
				key2 = new Text(inputWords[1] + " " + inputWords[2]);
				newValue = new Text(stringArrayToString(inputWords) + "-" + "true" + "-" + occ.toString());
				output.write(key1, newValue);
				if (!key1.toString().equals(key2.toString()))
					output.write(key2, newValue);

				// Single words keys
				key1 = new Text(inputWords[1]);
				key2 = new Text(inputWords[2]);

				output.write(key1, newValue);
				if (!key1.toString().equals(key2.toString()))
					output.write(key2, newValue);
			} else {
				String input = stringArrayToString(inputWords);
				key1 = new Text(input);
				newValue = new Text(input + "-" + "false" + "-" + occ.toString());
				output.write(key1, newValue);
			}
		}

		private String[] lineToStringArray(String line) {
			String[] words = line.split(" ");
			return Arrays.stream(words).filter(s -> !s.equals("*")).toArray(String[]::new);

		}

		private String stringArrayToString(String[] words) {
			String output = "";
			String delimiter = " ";
			for (String word : words)
				output = output + word + delimiter;
			return output.trim();
		}
	}

	public static class GetParamsPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int red_num) {
			String words = key.toString();
			return Math.abs(words.hashCode()) % red_num;
		}
	}

	public static class ReduceGetParams extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
			Long occ = 0L;
			List<String[]> tripletsValues = new ArrayList<>();
			Long[] outputValue;
			Text outputKey;
			String[] parsedKey = key.toString().split(" ");

			for (Text value : values) {
				String[] parsedValue = value.toString().split("-");
				if (parsedValue[1].equals("true"))
					tripletsValues.add(parsedValue);
				else
					occ = Long.parseLong(parsedValue[2]);
			}
			for (String[] parsedValue : tripletsValues) {
				Long[] c1n1 = initArray(2);
				Long[] c2n2 = initArray(2);
				Long tripletOcc = Long.parseLong(parsedValue[2]);
				String[] orgWords = parsedValue[0].split(" ");
				if (parsedKey.length == 1) {
					for (int i = 1; i < 3; i++) {
						if (orgWords[i].equals(parsedKey[0]))
							c1n1[i - 1] = occ;
					}
				} else {
					for (int i = 0; i < 2; i++) {
						if (orgWords[i].equals(parsedKey[0]) &&
								orgWords[i + 1].equals(parsedKey[1]))
							c2n2[i] = occ;
					}
				}
				outputValue = finalizeArray(c1n1, c2n2, tripletOcc);
				outputKey = finalizeKey(orgWords);
				String valueString = Arrays.toString(outputValue).replace("[", "").replace("]", "").replace(",", "");
				output.write(outputKey, new Text(valueString));
			}

		}

		private Text finalizeKey(String[] triplets) {
			return new Text(triplets[0] + " " + triplets[1] + " " + triplets[2]);
		}

		private Long[] finalizeArray(Long[] c1n1, Long[] c2n2, Long occ) {
			Long[] array = new Long[5];
			for (int i = 0; i < 2; i++) {
				array[i] = c1n1[i];
				array[i + 2] = c2n2[i];
			}
			array[4] = occ;
			return array;
		}

		private Long[] initArray(int size) {
			Long[] array = new Long[size];
			for (int i = 0; i < size; i++) {
				array[i] = (long) (-1);
			}
			return array;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String uuid = args[0];
		Path inputPath = new Path(String.format("s3://%s/firstStep-output/%s/", Consts.Bucket_name, uuid));
		Path output = new Path(String.format("s3://%s/secondStep-output/%s/", Consts.Bucket_name, uuid));// provide
																											// bucket
																											// name
		String tempFilesPath = String.format("s3://%s/temp-files/%s/", Consts.Bucket_name, uuid);// provide bucket name
		conf.set("tempFilesPath", tempFilesPath);

		Job job = Job.getInstance(conf, "second-step");
		job.setJarByClass(SecondStep.class);
		job.setMapperClass(MapGetParams.class);
		job.setPartitionerClass(GetParamsPartitioner.class);
		job.setReducerClass(ReduceGetParams.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
