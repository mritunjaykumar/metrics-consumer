package com.rackspacecloud.blueflood.metricsconsumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Component
public class Listener {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Listener.class);

    private CountDownLatch latch = new CountDownLatch(1);

    @Autowired
    Sender sender;

    @KafkaListener(topics = "telegraf")
    public void listenTelegraf(ConsumerRecord<?, ?> record){
        LOGGER.info("received payload='{}'", record);
        System.out.println("Listener id01 thread id:" + Thread.currentThread().getId());
        System.out.println("Record:" + record);

        String[] strArray = ((String) record.value()).split(" ");

        String tagsString = strArray[0];
        String fieldString = strArray[1];
        long eventTime = Long.parseLong(strArray[2].trim());

        String[] tagsArray = tagsString.split(",");
        String measurementName = tagsArray[0];

        if(measurementName.equalsIgnoreCase("cpu")){
            String msgToSend = processCpu(tagsArray, fieldString, eventTime);
            sender.send("cpu", msgToSend);
        }

        latch.countDown();
    }

    public String processCpu(String[] tagsArray, String fieldString, long eventTime){
        Map<String, String> metricsMap = new HashMap<>();

        for (int i = 1; i < tagsArray.length; i++) {
            String[] kvPair = tagsArray[i].split("=");
            metricsMap.put(kvPair[0], kvPair[1]);
        }

        String[] fieldsArray = fieldString.split(",");
        for (int i = 0; i < fieldsArray.length; i++) {
            String[] kvPair = fieldsArray[i].split("=");
            metricsMap.put(kvPair[0], kvPair[1]);
        }

        Map<String, Object> objToSend = new HashMap<>();

        String measureKey = "usage_user";
        objToSend.put("metricName", String.format("%s.%s.%s.%s",
                metricsMap.get("tenantId"),
                metricsMap.get("dc"),
                metricsMap.get("rack"),
                measureKey));
        objToSend.put("metricValue", Double.parseDouble(metricsMap.get(measureKey)));
        objToSend.put("collectionTime", eventTime);

        String result = "";
        ObjectMapper mapper = new ObjectMapper();
        try {
            result = mapper.writeValueAsString(objToSend);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return result;
    }
}
