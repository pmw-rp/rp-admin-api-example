import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Clean {

    public static Map<TopicPartition, OffsetSpec> buildOffsetRequest(String topic, int partitions, OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> result = new HashMap<>();
        for (int i = 0; i < partitions; i++) {
            result.put(new TopicPartition(topic, i), spec);
        }
        return result;
    }

    public static Map<TopicPartition, RecordsToDelete> buildDeletions(Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets) {
        Map<TopicPartition, RecordsToDelete> deletions = new HashMap<>();
        for (TopicPartition tp: offsets.keySet()) {
            deletions.put(tp, RecordsToDelete.beforeOffset(offsets.get(tp).offset()));
        }
        return deletions;
    }

    public static boolean isClean(TopicDescription description) throws ExecutionException, InterruptedException {
        int partitions = description.partitions().size();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest = client.listOffsets(buildOffsetRequest(description.name(), partitions, OffsetSpec.earliest())).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = client.listOffsets(buildOffsetRequest(description.name(), partitions, OffsetSpec.latest())).all().get();
        for(TopicPartition tp: earliest.keySet()) {
            if(earliest.get(tp).offset() != latest.get(tp).offset()) {
                return false;
            }
        }
        return true;
    }

    public static boolean clean(TopicDescription description) throws ExecutionException, InterruptedException {
        int partitions = description.partitions().size();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = client.listOffsets(buildOffsetRequest(description.name(), partitions, OffsetSpec.latest())).all().get();
        Map<TopicPartition, RecordsToDelete> deletions = buildDeletions(latest);
        Map<TopicPartition, KafkaFuture<DeletedRecords>> result = client.deleteRecords(deletions).lowWatermarks();
        for (TopicPartition tp: result.keySet()) {
            DeletedRecords dr = result.get(tp).get();
            if(dr.lowWatermark() != latest.get(tp).offset()) {
                return false;
            }
        }
        return true;
    }

    public static Long lastProduced(TopicDescription description) throws ExecutionException, InterruptedException {
        int partitions = description.partitions().size();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = client.listOffsets(buildOffsetRequest(description.name(), partitions, OffsetSpec.maxTimestamp())).all().get();
        long age = 0;
        for(TopicPartition tp: latest.keySet()) {
            long timestamp = latest.get(tp).timestamp();
            if(timestamp > age) {
                age = timestamp;
            }
        }
        return age;
    }

    public static String convertTime(long time){
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date);
    }


    private static AdminClient client;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        System.setProperty("java.security.auth.login.config", "src/main/resources/jaas.conf");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "seed-fa015309.certnoj7m575jtvbg730.fmc.prd.cloud.redpanda.com:9092");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");

        String prefix = "foo-";
        boolean performClean = false;

        client = AdminClient.create(properties);

        List<String> topics = client.listTopics().names().get().stream().filter(t -> t.startsWith(prefix)).toList();
        Map<String, TopicDescription> descriptions = client.describeTopics(topics).allTopicNames().get();

        for (TopicDescription description: descriptions.values()) {

            long lastProduced = lastProduced(description);
            if (lastProduced > 0) {
                System.out.println("Topic " + description.name() + " was last produced to at " + convertTime(lastProduced));
            } else {
                System.out.println("Topic " + description.name() + " is empty.");
            }

            if(!isClean(description)) {
                if(performClean) {
                    boolean cleaned = clean(description);
                    System.out.println("Topic " + description.name() + " was" + (cleaned ? "" : " not") + " cleaned.");
                } else {
                    System.out.println("Refusing to clean " + description.name() + " since performClean is false.");
                }
            } else {
                System.out.println("Topic " + description.name() + " was already clean.");
            }
        }

    }
}
