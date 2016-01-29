package org.costa;

import java.util.HashMap;
import java.util.Map;

public enum FileEntryStatus {
	PENDING, PROCESSING, DONE, MISSING;
	private static final Map<String, FileEntryStatus> map = new HashMap<String, FileEntryStatus>();

	static {
		map.put(PENDING.name(), PENDING);
		map.put(PROCESSING.name(), PROCESSING);
		map.put(DONE.name(), DONE);
		map.put(MISSING.name(), MISSING);
	}

	public static FileEntryStatus getByName(String name) {
		return map.get(name);
	}
}
