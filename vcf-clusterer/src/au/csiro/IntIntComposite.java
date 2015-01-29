package au.csiro;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
* This key is a composite key. The "actual"
* key is the UDID. The secondary sort will be performed against the datetime.
*/
public class IntIntComposite implements WritableComparable<IntIntComposite> {

	private IntWritable individualid;
	private IntWritable variantlocation;
	
	
	public IntIntComposite() {
    	set(new IntWritable(), new IntWritable());
	}

	public IntIntComposite(IntWritable individualid, IntWritable variantlocation) {
		this.individualid = individualid;
		this.variantlocation = variantlocation;
	}

    public void set(IntWritable individualid, IntWritable variantlocation) {
        this.individualid = individualid;
        this.variantlocation = variantlocation;
    }
    public void set(int individualid, int variantlocation) {
        this.individualid.set(individualid);
        this.variantlocation.set(variantlocation);
    }
    
    public void set(int individualid, IntWritable variantlocation) {
        this.individualid.set(individualid);
        this.variantlocation = variantlocation;
    }
	
	@Override
	public String toString() {
		return "[" + individualid.toString() + ',' + variantlocation + "]";
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.individualid.readFields(in);
		this.variantlocation.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		individualid.write(out);
		variantlocation.write(out);
	}
	
	@Override
	public int compareTo(IntIntComposite o) {
	
		int result = individualid.compareTo(o.individualid);
		if (0 == result) {
			result = variantlocation.compareTo(o.variantlocation);
		}
		return result;
	}
	
	/**
	* Gets the udid.
	*
	* @return UDID.
	*/
	public IntWritable getIndividualId() {
		return individualid;
	}
	
	public void setIndividualId(IntWritable individualid) {
		this.individualid = individualid;
	}
	

	public IntWritable getVariantLocation() {
		return variantlocation;
	}
	
	public void setDatetime(IntWritable variantlocation) {
		this.variantlocation = variantlocation;
	}
	

}