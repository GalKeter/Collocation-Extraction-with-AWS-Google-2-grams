import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComparableKey implements WritableComparable<ComparableKey>{
    private Text decade;
    private Text w1;
    private Text w2;
    private Text npmi;

    public ComparableKey(){
        this.decade=new Text("");
        this.w1=new Text("");
        this.w2=new Text("");
        this.npmi=new Text("");
    }

    public ComparableKey(String decade, String w1,String w2,String npmi){
        this.decade=new Text(decade);
        this.w1=new Text(w1);
        this.w2=new Text(w2);
        this.npmi=new Text(npmi);
    }

    public Text getDecade() {
        return decade;
    }

    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    public Text getNpmi() {
        return npmi;
    }

    public Double getNpmiAsDouble(){
        return Double.parseDouble(npmi.toString());
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ComparableKey)) 
            return false;
        if (this == o)
            return true;
        ComparableKey other = (ComparableKey) o;
        return decade.equals(other.getDecade()) && w1.equals(other.getW1()) && w2.equals(other.getW2()) && npmi.equals(other.getNpmi());
    }

    @Override
    public int compareTo(ComparableKey other) {//compare by decade -> npmi -> w1 -> w2 
        if(decade.compareTo(other.getDecade()) > 0) {
            return -1;
        } else if(decade.compareTo(other.getDecade()) < 0) {
            return 1;
        } else {//it is the same decade
            if(getNpmiAsDouble()<other.getNpmiAsDouble()) {
                return 1;
            } else if(getNpmiAsDouble()>other.getNpmiAsDouble()) {
                return -1;
            } else {//it is the same npmi
                if(w1.compareTo(other.getW1()) < 0) {
                    return -1;
                } else if(w1.compareTo(other.getW1()) > 0) {
                    return 1;
                } else {//it is the same w1
                    if(w2.compareTo(other.getW2()) > 0) {
                        return -1;
                    } else if(w2.compareTo(other.getW2()) < 0) {
                        return 1;
                    } else {//both are the same
                        return 0;
                    }
                }
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        decade.readFields(dataInput);
        w1.readFields(dataInput);
        w2.readFields(dataInput);
        npmi.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        decade.write(dataOutput);
        w1.write(dataOutput);
        w2.write(dataOutput);
        npmi.write(dataOutput);
    }

    @Override
    public String toString() {
        return decade.toString() + " " + w1.toString() + " " + w2.toString() + " " + npmi.toString();
    }

}