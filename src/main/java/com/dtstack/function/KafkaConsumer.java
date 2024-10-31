package com.dtstack.function;

import com.dtstack.util.KafkaSourceConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaConsumer {
    /*
    joyce,
    2024/10/31
    */
    public static void main(String args[]) {
    }
    public  static KafkaSource<String> build(KafkaSourceConfig kafkaSourceConfig){

        KafkaSource<String> kafka = KafkaSource.<String>builder().setBootstrapServers(kafkaSourceConfig.getBootstrap())
                .setTopics(kafkaSourceConfig.getConsumeTopic()).setGroupId(kafkaSourceConfig.getGroupId())
                // .setClientIdPrefix(kafkaSourceConfig.getClientidPrefix())
                .setProperty("enable.auto.commit", kafkaSourceConfig.getAutoCommit())
                .setProperty("auto.commit.interval.ms", kafkaSourceConfig.getCommitInterval())
                .setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000")
                .setStartingOffsets(OffsetsInitializer.latest()).setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return kafka;
    }

}