package au.csiro;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IndividualKeyGroupingComparator extends WritableComparator {

	protected IndividualKeyGroupingComparator() {
	super(IntIntComposite.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		IntIntComposite key1 = (IntIntComposite) w1;
		IntIntComposite key2 = (IntIntComposite) w2;
		// (check on IndividualId)
		return key1.getIndividualId().compareTo(key2.getIndividualId());
	}
}