package de.simon.originality.comparators;

import java.util.Comparator;

import de.simon.originality.magicquery.database_interact.DatabaseQuery.QueryResult;

public class ClusterIdComparator {
	private static Comparator<QueryResult> hierarchicalComparator = (r1, r2) -> {
        String id1 = (String) r1.payload().get("id");
        String id2 = (String) r2.payload().get("id");

        String[] parts1 = id1.split("-");
        String[] parts2 = id2.split("-");

        int minLength = Math.min(parts1.length, parts2.length);
        for (int i = 0; i < minLength; i++) {
            try {
                int num1 = Integer.parseInt(parts1[i].replaceAll("\\D", "")); 
                int num2 = Integer.parseInt(parts2[i].replaceAll("\\D", ""));
                int numCompare = Integer.compare(num1, num2);
                if (numCompare != 0) {
                    return numCompare; 
                }
            } catch (NumberFormatException e) {
                int stringCompare = parts1[i].compareTo(parts2[i]);
                if (stringCompare != 0) {
                    return stringCompare;
                }
            }
        }

        return Integer.compare(parts1.length, parts2.length);
    };
    
    public static Comparator<QueryResult> get(){
    	return hierarchicalComparator;
    }
}
