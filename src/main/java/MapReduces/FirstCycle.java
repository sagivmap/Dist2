package MapReduces;

import Bigrams.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class FirstCycle {


    public static class FirstMapper extends Mapper<LongWritable, Text, Bigram, LongWritable>{


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer strTok = new StringTokenizer(value.toString());

            if(strTok.countTokens() == 6){
                Text first = new Text(strTok.nextToken());
                Text second = new Text(strTok.nextToken());
                Text decade = new Text(strTok.nextToken());

                Bigram big = new Bigram(first, second, decade, new Text(""));
                context.write(big, new LongWritable(Integer.parseInt(strTok.nextToken())));
            }
        }

    }

    public class FirstReducer extends Reducer<Bigram, LongWritable, Bigram, LongWritable>{

        public void reduce(Bigram key, Iterable<LongWritable> valuse, Context context) throws IOException, InterruptedException{
            long sum = 0;
            for(LongWritable val: valuse){
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }

    }
}
