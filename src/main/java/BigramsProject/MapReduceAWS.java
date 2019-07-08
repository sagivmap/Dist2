package BigramsProject;

import com.amazonaws.auth.*;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class MapReduceAWS {

    public static void main(String[] args) throws FileNotFoundException, IOException{

        AWSCredentialsProvider creds = new AWSStaticCredentialsProvider(
                new EnvironmentVariableCredentialsProvider().getCredentials());

        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(creds);
        mapReduce.setRegion(Region.getRegion(Regions.US_EAST_1));

        String locationOfJar = "s3n://yairandsagivass2bucket/Ass2.jar";

        HadoopJarStepConfig stepConfig = new HadoopJarStepConfig()
                .withJar(locationOfJar)
                .withMainClass("BigramsProject.CollocationExtraction")
                .withArgs(args[0], args[1], args[2]);

    }
}
