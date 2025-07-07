package com.example.notifications.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.example.notifications.Consumer.SubscribeListen;
import com.example.notifications.entity.Notification;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
public class ProduceMessage {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    private static final Logger log = LoggerFactory.getLogger(ProduceMessage.class);
    public String sendMessage(Notification notification) {
        // Convert Notification to Map<String, String>
        Map<String, String> notificationMap = new HashMap<>();
        notificationMap.put("id", String.valueOf(notification.getId()));
        notificationMap.put("message", notification.getMessage());
        notificationMap.put("timestamp", String.valueOf(LocalDateTime.now()));
        notificationMap.put("receiver", notification.getReceiver());
        notificationMap.put("sender", notification.getSender());
        notificationMap.put("type", notification.getType());
        notificationMap.put("link", notification.getLink());
        notificationMap.put("read", String.valueOf(notification.isRead()));
        // Add more fields as needed
        String StreamKey = notification.getReceiver();
        StringRecord record = StreamRecords.string(notificationMap).withStreamKey(StreamKey);
        log.info("Sending message: {} into stream: {}", notificationMap, notification.getReceiver());
        RecordId recordId = stringRedisTemplate.opsForStream().add(record);
        log.info("Group info for stream {} : {}",StreamKey,stringRedisTemplate.opsForStream().groups(StreamKey));
        return recordId.getValue();
    }
}
