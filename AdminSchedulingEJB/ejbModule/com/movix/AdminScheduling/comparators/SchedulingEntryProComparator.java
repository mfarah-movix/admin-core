package com.movix.AdminScheduling.comparators;

import java.util.Comparator;

import com.movixla.service.scheduling.common.SchedulingEntryPro;

public class SchedulingEntryProComparator implements Comparator<SchedulingEntryPro> {

	@Override
    public int compare(SchedulingEntryPro o1, SchedulingEntryPro o2) {
		String o1SP = o1.getServicePrice(); 
		String o2SP = o1.getServicePrice(); 
		if(o1SP == null){
			o1SP = "";
		}
		if(o2SP == null){
			o2SP = "";
		}
		int spDiff = o1SP.compareTo(o2SP);
    	int	typeDiff = o1.getType().compareTo(o2.getType());
    	int	keyDiff = o1.getKey().compareTo(o2.getKey());
    	if(spDiff != 0) {
    		return spDiff;
    	} else if(typeDiff != 0) {
    		return typeDiff;
    	} else {
    		return keyDiff;
    	}
    }
}

