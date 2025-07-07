package com.example.notifications.Consumer;

import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class SubscribeListen {

    private static final Logger log = LoggerFactory.getLogger(SubscribeListen.class);

    @Autowired
    LettuceConnectionFactory redisConnectionFactory;

    @Autowired
    ConsumerListener consumerListener;

    @Autowired
    StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer;

    @Autowired
    StringRedisTemplate stringRedisTemplate;

    public Subscription subscribe(String consumerName, String groupName, String streamKey) {
        return singleConsumerSubscription(
            redisConnectionFactory,
            streamMessageListenerContainer,
            consumerName,
            groupName,
            streamKey,
            stringRedisTemplate
        );
    }

public Subscription singleConsumerSubscription(
        RedisConnectionFactory factory,
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer,
        String consumerName,
        String groupName,
        String streamKey,
        StringRedisTemplate redisTemplate) {

    // 1. Ensure stream exists
    
    if (!redisTemplate.hasKey(streamKey)) {
        Map<String, String> dummy = new HashMap<>();
        dummy.put("init", "thanks for subscribing");
        redisTemplate.opsForStream().add(streamKey, dummy);
        log.info("Stream '{}' created and dummy message added", streamKey);
    }


    // 2. Ensure group exists

    try {
        redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("$"), groupName);
        log.info("Consumer group '{}' created for stream '{}'", groupName, streamKey);
    } catch (RedisSystemException e) {
        if (e.getRootCause() != null && e.getRootCause().getMessage() != null && e.getRootCause().getMessage().contains("BUSYGROUP")) {
            log.warn("Consumer group '{}' already exists for stream '{}'", groupName, streamKey);
        } else {
            log.error("Error creating consumer group '{}' for stream '{}': {}", groupName, streamKey, e.getMessage());
        }
    } catch (Exception e) {
        log.error("Unexpected error creating consumer group '{}' for stream '{}': {}", groupName, streamKey, e.getMessage(), e);
    }
    

    // 3. Subscribe
    Consumer consumer = Consumer.from(groupName, consumerName);
    
    Subscription subscription = listenerContainer.receive(
            consumer,
            StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
            consumerListener
    );

    if (listenerContainer.isRunning()) {
        log.info("Listener container is already running.");
    } else {
        listenerContainer.start();
    }

    log.info("Subscription created for consumer '{}' on group '{}', stream '{}'", consumerName, groupName, streamKey);
    log.info("StreamMessageListenerContainer started {}.", listenerContainer.isRunning() ? "successfully" : "unsuccessfully");

    return subscription;
}


}