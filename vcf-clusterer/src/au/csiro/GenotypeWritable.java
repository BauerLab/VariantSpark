package au.csiro;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A custom Writable implementation for Request information.
 *
 * This is simple Custom Writable, and does not implement Comparable or RawComparator
 */
public class GenotypeWritable extends Object implements WritableComparable<GenotypeWritable> {

    private IntWritable location;
    private DoubleWritable variant;
   

    public GenotypeWritable(IntWritable location, DoubleWritable variant) {
    	set(location, variant);
    }

    public GenotypeWritable() {
    	set(new IntWritable(), new DoubleWritable());
    }
    
    public GenotypeWritable(int location, Double variant) {
    	set(new IntWritable(location), new DoubleWritable(variant));
    }
    
    public IntWritable getLocation() {
    	return location;
    }
    
    public DoubleWritable getVariant() {
    	return variant;
    }
        
    public void set(IntWritable location, DoubleWritable variant) {
        this.location = location;
        this.variant = variant;
    }
    

    @Override
    public void readFields(DataInput in) throws IOException {
        location.readFields(in);
        variant.readFields(in);
    }
 
    @Override
    public void write(DataOutput out) throws IOException {
        location.write(out);
        variant.write(out);
    }
 
    @Override
    public String toString() {
        return "[" + location + ", " + variant + "]";
    }
 
    @Override
    public int compareTo(GenotypeWritable tp) {
        int cmp = location.compareTo(tp.location);
 
        if (cmp != 0) {
            return cmp;
        }
 
        return variant.compareTo(tp.variant);
    }
 
    @Override
    public int hashCode(){
        return location.hashCode()*163 + variant.hashCode();
    }
 
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof GenotypeWritable)
        {
        	GenotypeWritable tp = (GenotypeWritable) o;
            return location.equals(tp.location) && variant.equals(tp.variant);
        }
        return false;
    }
 
}
