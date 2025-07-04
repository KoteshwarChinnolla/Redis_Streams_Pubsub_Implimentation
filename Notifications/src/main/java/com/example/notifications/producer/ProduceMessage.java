package com.example.notifications.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.example.notifications.entity.Notification;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
public class ProduceMessage {

    @Value("${redis.stream.key}")
    private String streamKey;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

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

        StringRecord record = StreamRecords.string(notificationMap).withStreamKey(streamKey);
        RecordId recordId = stringRedisTemplate.opsForStream().add(record);
        return recordId.getValue();
    }
}
