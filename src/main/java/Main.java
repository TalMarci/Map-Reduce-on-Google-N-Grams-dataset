


import java.util.UUID;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

import adapters.Consts;

public class Main{

	public static void main(String[] args) {

		String uuid = UUID.randomUUID().toString();
		String inputFile = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";;

		// get AWS credentials
		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

		AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
				.standard()
				.withRegion(Regions.US_EAST_1)
				.withCredentials(credentialsProvider)
				.build();

		HadoopJarStepConfig hadoopJarFirstStep = new HadoopJarStepConfig()
				.withJar("s3://"+Consts.Bucket_name+"/First-step.jar")
				.withArgs(inputFile, uuid, inputType);

		StepConfig stepConfig1 = new StepConfig()
				.withName("FirstStep")
				.withHadoopJarStep(hadoopJarFirstStep)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig hadoopJarSecondStep = new HadoopJarStepConfig()
				.withJar("s3n://"+Consts.Bucket_name+"/Second-step.jar")
				.withArgs(uuid);

		StepConfig stepConfig2 = new StepConfig()
				.withName("SecondStep")
				.withHadoopJarStep(hadoopJarSecondStep)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

				HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
				.withJar("s3n://"+Consts.Bucket_name+"/Third-step.jar")
				.withArgs(uuid);

		StepConfig stepConfig3 = new StepConfig()
				.withName("ThirdStep")
				.withHadoopJarStep(hadoopJarStep3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		
				HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
				.withJar("s3n://"+Consts.Bucket_name+"/Fourth-step.jar")
				.withArgs(uuid);

		StepConfig stepConfig4 = new StepConfig()
				.withName("FourthStep")
				.withHadoopJarStep(hadoopJarStep4)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(8)
				.withMasterInstanceType(InstanceType.M1Xlarge.toString())
				.withSlaveInstanceType(InstanceType.M1Xlarge.toString())
				.withHadoopVersion("2.6.0")
				.withKeepJobFlowAliveWhenNoSteps(false)
				.withEc2KeyName(Consts.Key)
				.withPlacement(new PlacementType("us-east-1a"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
				.withName("MapReduceDsc202")
				.withReleaseLabel("emr-5.14.0")
				.withInstances(instances)
				.withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4)
				.withLogUri("s3://"+Consts.Bucket_name+"/logs/")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withServiceRole("EMR_DefaultRole");

		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId + " uuid: " + uuid);
	}
}
