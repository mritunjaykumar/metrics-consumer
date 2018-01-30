package com.rackspacecloud.blueflood.metricsconsumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Component
public class Listener {
    @Value("${kafka.topic.blueflood.metrics}")
    private String kafkaTopicBluefloodMetrics;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(Listener.class);

    private CountDownLatch latch = new CountDownLatch(1);

    @Autowired
    Sender sender;

    @KafkaListener(topics = "telegraf")
    public void listenTelegraf(ConsumerRecord<?, ?> record){
        LOGGER.info("received payload='{}'", record);

        String[] strArray = ((String) record.value()).split(" ");

        // Based on telegraf's line protocol this should always be 3.
        // Following is line protocol schema:
        // measurementName,tagstring fieldstring timestamp
        if(strArray.length != 3) {
            LOGGER.error("Message '{}' doesn't follow line telegraf's protocol.", record.value());
        }
        else {
            processMessage(strArray);
            latch.countDown();
        }
    }

    private void processMessage(String[] strArray) {
        String tagsString = strArray[0];
        String fieldString = strArray[1];
        long eventTime = Long.parseLong(strArray[2].trim());

        String[] tagsArray = tagsString.split(",");
        String measurementName = tagsArray[0];

        Map<String, String> metricsMap = getMetricsMap(tagsArray, fieldString);

        String msgToSend = "";

        switch (measurementName.toLowerCase()) {
            case "cpu":
                if (metricsMap.get("cpu").equalsIgnoreCase("cpu-total")) {
                    msgToSend = processCpu(metricsMap, eventTime, measurementName);
                    sender.send("cpu", msgToSend);
                }
                break;

            case "mem":
                msgToSend = processMem(metricsMap, eventTime, measurementName);
                sender.send("mem", msgToSend);
                break;

            case "disk":
                if (metricsMap.get("path").equalsIgnoreCase("/")) {
                    msgToSend = processDisk(metricsMap, eventTime, measurementName);
                    sender.send("disk", msgToSend);
                }
                break;

            case "diskio":
                msgToSend = processDiskIO(metricsMap, eventTime, measurementName);
                sender.send("diskio", msgToSend);
                break;

            default:
                LOGGER.error("{} measurement type is not supported at this time.", measurementName);
        }

        if(!StringUtils.isEmpty(msgToSend))
            sender.send(kafkaTopicBluefloodMetrics, msgToSend);
    }

    public String processCpu(Map<String, String> metricsMap, long eventTime, String measurementName){
        Map<String, Object> objToSend = getCpuMap(eventTime, measurementName, metricsMap);
        String result = getMessageToSend(objToSend);

        return result;
    }

    public String processMem(Map<String, String> metricsMap, long eventTime, String measurementName){
        Map<String, Object> objToSend = getMemMap(eventTime, measurementName, metricsMap);
        String result = getMessageToSend(objToSend);

        return result;
    }

    private String getMessageToSend(Map<String, Object> objToSend) {
        String result = "";
        ObjectMapper mapper = new ObjectMapper();
        try {
            result = mapper.writeValueAsString(objToSend);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    private Map<String, Object> getCpuMap(long eventTime, String measurementName, Map<String, String> metricsMap) {
        Map<String, Object> objToSend = new HashMap<>();

        String measureKey = "usage_user";
        objToSend.put("metricName", String.format("%s.%s.%s.%s.%s.%s",
                metricsMap.get("tenantId"),
                metricsMap.get("dc"),
                metricsMap.get("rack"),
                metricsMap.get("host"),
                measurementName,
                measureKey));
        objToSend.put("metricValue", Double.parseDouble(metricsMap.get(measureKey)));
        objToSend.put("collectionTime", eventTime);
        return objToSend;
    }

    private Map<String, String> getMetricsMap(String[] tagsArray, String fieldString) {
        Map<String, String> metricsMap = new HashMap<>();

        for (int i = 1; i < tagsArray.length; i++) {
            String[] kvPair = tagsArray[i].split("=");
            String temp = kvPair[1].replace(".", "_");
            metricsMap.put(kvPair[0], temp);
        }

        String[] fieldsArray = fieldString.split(",");
        for (int i = 0; i < fieldsArray.length; i++) {
            String[] kvPair = fieldsArray[i].split("=");
            metricsMap.put(kvPair[0], kvPair[1]);
        }
        return metricsMap;
    }

    private Map<String, Object> getMemMap(long eventTime, String measurementName,
                                          Map<String, String> metricsMap) {
        Map<String, Object> objToSend = new HashMap<>();

        String measureKey = "used_percent";
        objToSend.put("metricName", String.format("%s.%s.%s.%s.%s.%s",
                metricsMap.get("tenantId"),
                metricsMap.get("dc"),
                metricsMap.get("rack"),
                metricsMap.get("host"),
                measurementName,
                measureKey));
        objToSend.put("metricValue", Double.parseDouble(metricsMap.get(measureKey)));
        objToSend.put("collectionTime", eventTime);
        return objToSend;
    }


    private Map<String, Object> getDiskMap(long eventTime, String measurementName,
                                          Map<String, String> metricsMap) {
        Map<String, Object> objToSend = new HashMap<>();

        String measureKey = "used_percent";
        objToSend.put("metricName", String.format("%s.%s.%s.%s.%s.%s",
                metricsMap.get("tenantId"),
                metricsMap.get("dc"),
                metricsMap.get("rack"),
                metricsMap.get("host"),
                measurementName,
                measureKey));
        objToSend.put("metricValue", Double.parseDouble(metricsMap.get(measureKey)));
        objToSend.put("collectionTime", eventTime);
        return objToSend;
    }

    public String processDisk(Map<String, String> metricsMap, long eventTime, String measurementName){
        Map<String, Object> objToSend = getDiskMap(eventTime, measurementName, metricsMap);
        String result = getMessageToSend(objToSend);

        return result;
    }

    private Map<String, Object> getDiskIOMap(long eventTime, String measurementName,
                                           Map<String, String> metricsMap) {
        Map<String, Object> objToSend = new HashMap<>();

        String measureKey = "iops_in_progress";
        objToSend.put("metricName", String.format("%s.%s.%s.%s.%s.%s",
                metricsMap.get("tenantId"),
                metricsMap.get("dc"),
                metricsMap.get("rack"),
                metricsMap.get("host"),
                measurementName,
                measureKey));
        String temp = metricsMap.get(measureKey);
        temp = temp.substring(0, temp.length()-1);
        objToSend.put("metricValue", Integer.parseInt(temp));
        objToSend.put("collectionTime", eventTime);
        return objToSend;
    }

    public String processDiskIO(Map<String, String> metricsMap, long eventTime, String measurementName){
        Map<String, Object> objToSend = getDiskIOMap(eventTime, measurementName, metricsMap);
        String result = getMessageToSend(objToSend);

        return result;
    }
}



