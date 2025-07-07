package com.example.notifications.Consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import com.example.notifications.entity.Notification;
import com.example.notifications.service.NotificationEmitterService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Repository;

@Repository
public class EventListener implements MessageListener {

    private final ObjectMapper objectMapper;
    private final NotificationEmitterService notificationController;

    private static final Logger log = LoggerFactory.getLogger(SubscribeListen.class);


    @Autowired
    public EventListener(ObjectMapper objectMapper, NotificationEmitterService notificationController) {
        this.objectMapper = objectMapper;
        this.notificationController = notificationController;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            log.info("New message received: {}", message);
            Notification event = objectMapper.readValue(message.getBody(), Notification.class);
            notificationController.sendMessage(event);
        } catch (IOException e) {
            log.error("Error while parsing message");
        }
    }
}