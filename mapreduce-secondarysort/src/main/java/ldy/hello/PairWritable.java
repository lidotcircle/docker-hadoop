package ldy.hello;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class PairWritable implements WritableComparable<PairWritable> {
    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }
    
    private int first;
    public int getFirst() {
        return first;
    }
    public void setFirst(int first) {
        this.first = first;
    }

    private int second;
    public int getSecond() {
        return second;
    }
    public void setSecond(int second) {
        this.second = second;
    }
    
    public void readFields(DataInput in) throws IOException {
        this.setFirst(in.readInt());
        this.setSecond(in.readInt());
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.getFirst());
        out.writeInt(this.getSecond());
    }
    public int compareTo(PairWritable o) {
        int compare = Integer.valueOf(this.getFirst()).compareTo(o.getFirst());
        if (compare != 0) {
            return compare;
        }
        return Integer.valueOf(this.getSecond()).compareTo(o.getSecond());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + first;
        result = prime * result + second;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PairWritable other = (PairWritable) obj;
        if (first != other.first)
            return false;
        if (second != other.second)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}

