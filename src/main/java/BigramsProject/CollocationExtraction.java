package BigramsProject;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import sun.applet.Main;

public class CollocationExtraction {
    private static String PathToFirstOutput = "s3n://yairandsagivass2bucket/first_output";
    private static String PathToSecondOutput = "s3n://yairandsagivass2bucket/second_output";
    private static String PathToThirdOutput = "s3n://yairandsagivass2bucket/third_output";
    private static String PathToFourthOutput = "s3n://yairandsagivass2bucket/fourth_output";
    private static String PathToFinalOutput = "s3n://yairandsagivass2bucket/final_output";

    public static boolean setMapNReduceTaskAndRun(Configuration conf,
                                                  String jobName,
                                                  Class<CollocationExtraction> MapReduceClass,
                                                  Class Mapper,
                                                  Class Reducer,
                                                  Class MapOutputKey,
                                                  Class MapOutputValue,
                                                  Class ReduceOutputKey,
                                                  Class ReduceOutputValue,
                                                  String Input,
                                                  String Output,
                                                  boolean isLZO,
                                                  Class partitionerClass
                                                  ) throws Exception{
        Job myJob = new Job(conf, jobName);
        myJob.setJarByClass(MapReduceClass);
        if(Mapper != null) myJob.setMapperClass(Mapper);
        if(partitionerClass != null) myJob.setPartitionerClass(partitionerClass);
        if(Reducer != null) myJob.setReducerClass(Reducer);

        //Mapper`s output
        myJob.setMapOutputKeyClass(MapOutputKey);
        myJob.setMapOutputValueClass(MapOutputValue);

        //Reducer`s output
        if(ReduceOutputKey != null) myJob.setOutputKeyClass(ReduceOutputKey);
        if(ReduceOutputValue != null) myJob.setOutputValueClass(ReduceOutputValue);

        if(isLZO) myJob.setInputFormatClass(SequenceFileInputFormat.class);

        TextInputFormat.addInputPath(myJob, new Path(Input));
        TextOutputFormat.setOutputPath(myJob, new Path(Output));
        return myJob.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception{
        String input = args[0];
        String minPMI = args[1];
        String relMinPMI = args[2];

        Configuration conf = new Configuration();
        conf.set("minPMI", minPMI);
        conf.set("relMinPMI", relMinPMI);


        boolean completed = setMapNReduceTaskAndRun(conf, "firstMapReduce",
                BigramsProject.CollocationExtraction.class, MapReduces.FirstCycle.FirstMapper.class,
                MapReduces.FirstCycle.FirstReducer.class, Bigrams.Bigram.class, LongWritable.class,
                Bigrams.Bigram.class, LongWritable.class, input, PathToFirstOutput, true, null);

        if(completed){
            completed = setMapNReduceTaskAndRun(conf, "secondMapReduce",
                    BigramsProject.CollocationExtraction.class, MapReduces.SecondCycle.SecondMapper.class,
                    MapReduces.SecondCycle.SecondReducer.class, Bigrams.Bigram.class, LongWritable.class,
                    Bigrams.Bigram.class, LongWritable.class, PathToFirstOutput, PathToSecondOutput, false,
                    MapReduces.SecondCycle.SecondPartioner.class);

            if(completed){
                completed = setMapNReduceTaskAndRun(conf, "thirdMapReduce",
                        BigramsProject.CollocationExtraction.class, MapReduces.ThirdCycle.ThirdMapper.class,
                        MapReduces.ThirdCycle.ThirdReducer.class, Bigrams.Bigram.class, Text.class,
                        Bigrams.Bigram.class, Text.class, PathToSecondOutput, PathToThirdOutput, false,
                        MapReduces.ThirdCycle.ThirdPartioner.class);

                if(completed){
                    completed = setMapNReduceTaskAndRun(conf, "fourthMapReduce",
                            BigramsProject.CollocationExtraction.class, MapReduces.FourthCycle.FourthMapper.class,
                            MapReduces.FourthCycle.FourthReducer.class, Bigrams.Bigram.class, Text.class,
                            Bigrams.Bigram.class, Text.class, PathToThirdOutput, PathToFourthOutput, false,
                            MapReduces.FourthCycle.FourthPartioner.class);

                    if(completed){
                        completed = setMapNReduceTaskAndRun(conf, "finalMapReduce",
                                BigramsProject.CollocationExtraction.class, MapReduces.FinalCycle.FinalMapper.class,
                                MapReduces.FinalCycle.FinalReducer.class, Bigrams.Bigram.class, Text.class,
                                Bigrams.Bigram.class, Text.class, PathToFourthOutput, PathToFinalOutput, false,
                                MapReduces.FinalCycle.FinalPartioner.class);

                        if(completed)
                            return;

                    }
                }
            }
        }


    }
}
