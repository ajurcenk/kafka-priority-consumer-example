# PriorityConsumerExample Example

## Send data

```bash
kafka-producer-perf-test.sh --producer-props bootstrap.servers=redpanda-0.testzone.local:31092 --topic critical-topic --throughput 1 \
--payload-file ./data/critical-topic_data.txt \
--num-records 1000

kafka-producer-perf-test.sh --producer-props bootstrap.servers=redpanda-0.testzone.local:31092 --topic high-topic --throughput 10 \
--payload-file ./data/high-topic_data.txt \
--num-records 1000

kafka-producer-perf-test.sh --producer-props bootstrap.servers=redpanda-0.testzone.local:31092 --topic low-topic --throughput 20 \
--payload-file ./data/low-topic-data.txt \
--num-records 1000
```