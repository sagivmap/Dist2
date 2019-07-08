package MapReduces;

import Bigrams.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class SecondCycle {

    public static class SecondMapper extends Mapper<LongWritable, Text, Bigram, LongWritable> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer strTok = new StringTokenizer(value.toString());
            Text first = new Text(strTok.nextToken());
            Text second = new Text(strTok.nextToken());
            Text decade = new Text(strTok.nextToken());
            LongWritable numOfOccu = new LongWritable(Integer.parseInt(strTok.nextToken()));
            Bigram big = new Bigram(first, second, decade, new Text(""));
            Bigram bigWithStar = new Bigram(first, new Text("*"), decade, new Text(""));

            context.write(big, numOfOccu);
            context.write(bigWithStar, numOfOccu);
        }

    }

    public class SecondReducer extends Reducer<Bigram, LongWritable, Bigram, Text> {
        private long counterForFirstWord = 0;

        public void reduce(Bigram key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
            if(key.getSecond().toString().equals("*")){
                long sum = 0;
                for(LongWritable val : values)
                    sum += val.get();
                counterForFirstWord = sum;

            }else{
                Text cW1W2 = new Text(values.iterator().next().toString());
                Text cW1 = new Text(String.valueOf(counterForFirstWord));
                context.write(key, new Text(cW1W2.toString() + " " + cW1.toString()));
            }
        }

    }

    public class SecondPartioner extends Partitioner<Bigram, LongWritable> {
        @Override
        public int getPartition(Bigram bigram, LongWritable longWritable, int i) {
            return Integer.parseInt(bigram.getDecade().toString()) % i;
        }
    }
}
