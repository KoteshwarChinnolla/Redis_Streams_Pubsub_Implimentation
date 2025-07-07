package com.example.notifications.Consumer;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class SubscribeListen {

    private static final Logger log = LoggerFactory.getLogger(SubscribeListen.class);


    @Autowired
    MessageListenerAdapter listenerAdapter;

    @Autowired
    RedisMessageListenerContainer redisContainer;

    @Autowired
    StringRedisTemplate stringRedisTemplate;

    public void subscribeUser(String userId) {
        ChannelTopic topic_per_user = new ChannelTopic("notifications:user:" + userId);
        ChannelTopic topic = new ChannelTopic("notifications:all");
        log.info("Subscribing to topic: {} and {}", topic_per_user.getTopic(), topic.getTopic());
        redisContainer.addMessageListener(listenerAdapter, topic_per_user);
        redisContainer.addMessageListener(listenerAdapter, topic);
    }

    public void unsubscribeUser(String userId) {
        ChannelTopic topic_per_user = new ChannelTopic("notifications:user:" + userId);
        ChannelTopic topic = new ChannelTopic("notifications:all");
        redisContainer.removeMessageListener(listenerAdapter, topic_per_user);
        redisContainer.removeMessageListener(listenerAdapter, topic);
    }
}