package MapReduces;

import Bigrams.Bigram;
import Bigrams.FinalBigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class FinalCycle {
    public static class FinalMapper extends Mapper<LongWritable, Text, FinalBigram, Text> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer strTok = new StringTokenizer(value.toString());
            Text first = new Text(strTok.nextToken());
            Text second = new Text(strTok.nextToken());
            Text decade = new Text(strTok.nextToken());
            Text npmi = new Text(strTok.nextToken());

            FinalBigram fBig = new FinalBigram(first, second, decade, npmi);
            FinalBigram fBigDecade = new FinalBigram(new Text("*"), new Text("*"), decade, new Text("*"));

            context.write(fBig, npmi);
            context.write(fBigDecade, npmi);
        }

    }

    public class FinalReducer extends Reducer<FinalBigram, Text, FinalBigram, Text> {
        private double sumOfAllNormalized;
        @Override
        public void reduce(FinalBigram key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            if(key.getFirst().toString().equals("*") && key.getSecond().toString().equals("*")){
                sumOfAllNormalized = 0;
                for(Text val: values){
                    StringTokenizer strTok = new StringTokenizer(val.toString());
                    sumOfAllNormalized += Double.parseDouble(strTok.nextToken());
                }
            }else{
                double npmi = Double.parseDouble(key.getNpmi().toString());
                double relPmi = npmi / sumOfAllNormalized;
                double relMinPMI = Double.parseDouble(context.getConfiguration().get("relMinPMI"));
                double minPMI = Double.parseDouble(context.getConfiguration().get("minPMI"));

                if(npmi > minPMI && relPmi > relMinPMI){
                    context.write(new FinalBigram(key.getFirst(), key.getSecond(), key.getDecade(), new Text("")),
                            new Text(String.valueOf(npmi)));
                }
            }
        }

    }

    public class FinalPartioner extends Partitioner<Bigram, LongWritable> {


        @Override
        public int getPartition(Bigram bigram, LongWritable longWritable, int i) {
            return Integer.parseInt(bigram.getDecade().toString()) % i;
        }
    }
}
