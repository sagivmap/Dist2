package Bigrams;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bigram implements WritableComparable<Bigram>{

    private Text first;
    private Text second;
    private Text decade;
    private Text npmi;

    public Bigram(Text first, Text second, Text decade, Text npmi){
        this.first = first;
        this.second = second;
        this.decade = decade;
        this.npmi = npmi;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public Text getDecade() {
        return decade;
    }

    public Text getNpmi() {
        return npmi;
    }

    @Override
    public int compareTo(Bigram toCompare) {
        if(this.getDecade().compareTo(toCompare.getDecade()) == 0){
            if(this.getFirst().compareTo(toCompare.getFirst())==0){
                return this.getSecond().compareTo(toCompare.getSecond());
            }
            return this.getFirst().compareTo(toCompare.getFirst());
        }
        return this.getDecade().compareTo(toCompare.getDecade());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.first.write(dataOutput);
        this.second.write(dataOutput);
        this.decade.write(dataOutput);
        this.npmi.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first.readFields(dataInput);
        this.second.readFields(dataInput);
        this.decade.readFields(dataInput);
        this.npmi.readFields(dataInput);
    }
}
