package MapReduces;

import Bigrams.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class ThirdCycle {
    public static class ThirdMapper extends Mapper<LongWritable, Text, Bigram, Text> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer strTok = new StringTokenizer(value.toString());
            Text first = new Text(strTok.nextToken());
            Text second = new Text(strTok.nextToken());
            Text decade = new Text(strTok.nextToken());
            Text dataToTransfer = new Text(strTok.nextToken() + " " + strTok.nextToken());

            Bigram big = new Bigram(second, first, decade, new Text(""));
            Bigram bigWithStar = new Bigram(second, new Text("*"), decade, new Text(""));

            context.write(big, dataToTransfer);
            context.write(bigWithStar, dataToTransfer);
        }

    }

    public class ThirdReducer extends Reducer<Bigram, Text, Bigram, Text> {
        private long counterForSecondWord = 0;

        @Override
        public void reduce(Bigram key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            if(key.getSecond().toString().equals("*")){
                long sum = 0;
                for(Text val : values){
                    StringTokenizer strToken = new StringTokenizer(val.toString());
                    sum += Long.parseLong(strToken.nextToken());
                }
                counterForSecondWord = sum;
            }else{
                String valAsString = values.iterator().next().toString() + " " + counterForSecondWord;
                context.write(new Bigram(key.getSecond(), key.getFirst(), key.getDecade(), new Text("")), new Text(valAsString));
            }
        }

    }

    public class ThirdPartioner extends Partitioner<Bigram, LongWritable> {
        @Override
        public int getPartition(Bigram bigram, LongWritable longWritable, int i) {
            return Integer.parseInt(bigram.getDecade().toString()) % i;
        }
    }
}
