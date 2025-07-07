package com.example.notifications.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.example.notifications.entity.Notification;

@Service
public class ProduceMessage {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    public Long sendMessage(Notification notification, String channel) {
        
        System.out.println("Sending notification to channel: " + channel);
        if ("all".equals(channel)) return redisTemplate.convertAndSend("notifications:all", notification);

        return redisTemplate.convertAndSend("notifications:user:"+channel, notification);
    }
}
