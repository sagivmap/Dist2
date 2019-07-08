package MapReduces;

import Bigrams.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class FourthCycle {

    public static class FourthMapper extends Mapper<LongWritable, Text, Bigram, Text> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer strTok = new StringTokenizer(value.toString());
            Text first = new Text(strTok.nextToken());
            Text second = new Text(strTok.nextToken());
            Text decade = new Text(strTok.nextToken());
            // data = cW1W2 cW1 cW2
            Text data = new Text(strTok.nextToken() + " " + strTok.nextToken() + " " + strTok.nextToken());

            Bigram big = new Bigram(first, second, decade, new Text(""));
            Bigram bigWithStarStar = new Bigram(new Text("*"), new Text("*"), decade, new Text(""));

            context.write(big, data);
            context.write(bigWithStarStar, data);
        }

    }

    public class FourthReducer extends Reducer<Bigram, Text, Bigram, Text> {
        private double N;
        public void reduce(Bigram key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            if(key.getFirst().toString().equals("*") && key.getSecond().toString().equals("*")){
                N = 0;
                for(Text val: values){
                    N++;
                }
            }else{
                StringTokenizer strTok = new StringTokenizer(values.iterator().next().toString());
                double cW1W2 = Double.parseDouble(strTok.nextToken());
                double cW1 = Double.parseDouble(strTok.nextToken());
                double cW2 = Double.parseDouble(strTok.nextToken());
                double logcW1W2 = Math.log(cW1W2);
                double logcW1 = Math.log(cW1);
                double logcW2 = Math.log(cW2);
                double logN = Math.log(N);
                double pmi = logcW1W2 + logN - logcW1 - logcW2;
                double pW1W2 = cW1W2 / N;
                double negLogpW1W2 = -1 * Math.log(pW1W2);
                double npmi = pmi / negLogpW1W2;

                context.write(key, new Text(String.valueOf(npmi)));

            }
        }

    }

    public class FourthPartioner extends Partitioner<Bigram, LongWritable> {


        @Override
        public int getPartition(Bigram bigram, LongWritable longWritable, int i) {
            return Integer.parseInt(bigram.getDecade().toString()) % i;
        }
    }
}
