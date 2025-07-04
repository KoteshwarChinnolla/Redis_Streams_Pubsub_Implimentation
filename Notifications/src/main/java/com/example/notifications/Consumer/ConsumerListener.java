package com.example.notifications.Consumer;

import java.util.HashMap;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Repository;
import com.example.notifications.service.NotificationEmitterService;

@Repository
public class ConsumerListener implements StreamListener<String, MapRecord<String, String, String>> {
    @Autowired
    NotificationEmitterService notificationController;

    private static final Logger log = LoggerFactory.getLogger(SubscribeListen.class);
    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        // Assuming "receiver" is a key in the value map
        log.info("Received message: {}", message);
        log.info("Message ID: {}", message.getId());
        log.info("Message Values: {}", message.getValue());
        Map<String, String> valueMap = message.getValue();
        notificationController.sendMessage(valueMap);
    }
}