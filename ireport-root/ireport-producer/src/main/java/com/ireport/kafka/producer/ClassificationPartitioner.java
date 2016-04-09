package com.ireport.kafka.producer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class ClassificationPartitioner implements Partitioner {
	public ClassificationPartitioner(VerifiableProperties props) {

	}

	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		String stringKey = (String) key;
		int offset = stringKey.lastIndexOf('.');
		if (offset > 0) {
			partition = Integer.parseInt(stringKey.substring(offset + 1))
					% a_numPartitions;
		}
		return partition;
	}
}
