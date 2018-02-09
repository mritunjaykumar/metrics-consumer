package com.rackspacecloud.blueflood.metricsconsumers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(String trackingId, String topic, String payload) {
        String currentTrackingId = String.format("%s|%s", trackingId, UUID.randomUUID());
        LOGGER.info("TrackingId:{}, START: Processing", currentTrackingId);
        LOGGER.debug("TrackingId:{}, sending payload='{}' to topic='{}'", currentTrackingId, payload, topic);
        kafkaTemplate.send(topic, payload);
        LOGGER.info("TrackingId:{}, FINISH: Processing", currentTrackingId);
    }
}
