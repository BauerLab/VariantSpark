package net.aidan;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IndividualKeyGroupingComparator extends WritableComparator {

	protected IndividualKeyGroupingComparator() {
	super(CompositeKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;
		// (check on IndividualId)
		return key1.getIndividualId().compareTo(key2.getIndividualId());
	}
}