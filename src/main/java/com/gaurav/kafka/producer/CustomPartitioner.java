package com.gaurav.kafka.producer;
//
//public class CustomPartitioner {
//}
import java.util.Map;
        import org.apache.kafka.clients.producer.Partitioner;
        import org.apache.kafka.common.Cluster;
public class CustomPartitioner implements Partitioner{
    private static final int PARTITION_COUNT=50;
    @Override
    public void configure(Map<String, ?> configs) {
    }
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer keyInt=Integer.parseInt(key.toString());
        keyInt%= PARTITION_COUNT;
        System.out.println(keyInt.toString() );
        return keyInt % PARTITION_COUNT;
    }
    @Override
    public void close() {
    }
}