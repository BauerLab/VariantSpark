package au.csiro;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LocationKeyComparator extends WritableComparator {
protected LocationKeyComparator() {
super(IntIntComposite.class, true);
}
@SuppressWarnings("rawtypes")
@Override
public int compare(WritableComparable w1, WritableComparable w2) {
	
	IntIntComposite key1 = (IntIntComposite) w1;
	IntIntComposite key2 = (IntIntComposite) w2;
	
	// (first check on udid)
	int compare = key1.getIndividualId().compareTo(key2.getIndividualId());
	if (compare == 0) {
		// only if we are in the same input group should we try and sort by value (datetime)
		return key1.getVariantLocation().compareTo(key2.getVariantLocation());
	}
	
	return compare;
	}
}