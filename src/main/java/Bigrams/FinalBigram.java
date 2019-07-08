package Bigrams;

import org.apache.hadoop.io.Text;

public class FinalBigram extends Bigram {
    public FinalBigram(Text first, Text second, Text decade, Text npmi) {
        super(first, second, decade, npmi);
    }

    @Override
    public int compareTo(Bigram toCompare) {
        if(this.getDecade().compareTo(toCompare.getDecade()) == 0){
            if(this.getNpmi().compareTo(toCompare.getNpmi()) == 0){
                if(this.getFirst().compareTo(toCompare.getFirst())==0){
                    return this.getSecond().compareTo(toCompare.getSecond());
                }
                return this.getFirst().compareTo(toCompare.getFirst());
            }
            return this.getNpmi().compareTo(toCompare.getNpmi());
        }
        return this.getDecade().compareTo(toCompare.getDecade());
    }
}
