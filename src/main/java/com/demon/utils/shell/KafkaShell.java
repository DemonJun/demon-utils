package com.demon.utils.shell;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * kafka utilities.
 *
 * @author fanjx@weipaitang.com
 * @date 2021/4/25 2:12 下午:00
 */
@ShellComponent
public class KafkaShell {

    @ShellMethod(key = "getOffsetByTime", group = "kafka",
            value = "根据给定的时间戳, 查询 topic - partition 对应的 offset")
    public List<String> getOffsetByTime(@NotBlank @ShellOption(value = {"-b", "--bootstrap-servers"}, help = "bootstrap-servers")
                                                String bootstrapServers,
                                        @NotBlank @ShellOption(value = {"-t", "--topic"}, help = "topic") String topic,
                                        @ShellOption(value = {"-p", "--partitions"},
                                                help = "partitions, 多个已','分割, 默认为空, 查询所有的 partitions",
                                                defaultValue = "") String partitions,
                                        @NotNull @ShellOption(value = "-time", help = "timestamp") Long timestamp) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demon-utils");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<TopicPartition, Long> timestampsToSearch = Maps.newHashMap();
            if (StringUtils.isBlank(partitions)) {
                consumer.partitionsFor(topic).forEach(partitionInfo -> timestampsToSearch.put(
                        new TopicPartition(topic, partitionInfo.partition()),
                        timestamp
                ));
            } else {
                Splitter.on(",").omitEmptyStrings().trimResults().splitToList(partitions).forEach(partition ->
                        timestampsToSearch.put(
                                new TopicPartition(topic, Integer.parseInt(partition)),
                                timestamp
                        )
                );
            }

            Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsets = consumer.offsetsForTimes(timestampsToSearch);

            return topicPartitionOffsets.entrySet().stream()
                    .map(entry -> "partition - " + entry.getKey().partition() +
                            ", offset - " + entry.getValue().offset())
                    .collect(Collectors.toList());
        }
    }
}
