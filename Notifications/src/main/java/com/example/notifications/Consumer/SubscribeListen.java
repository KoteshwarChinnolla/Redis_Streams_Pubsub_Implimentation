package com.example.notifications.Consumer;

import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class SubscribeListen {

    private static final Logger log = LoggerFactory.getLogger(SubscribeListen.class);

    @Value("${redis.stream.key}")
    private String streamKey;


    @Autowired
    LettuceConnectionFactory redisConnectionFactory;

    @Autowired
    ConsumerListener consumerListener;

    @Autowired
    StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer;

    @Autowired
    StringRedisTemplate stringRedisTemplate;

    public Subscription subscribe(String consumerName, String groupName) {
        return singleConsumerSubscription(
            redisConnectionFactory,
            streamMessageListenerContainer,
            consumerName,
            groupName,
            stringRedisTemplate
        );
    }

    public Subscription singleConsumerSubscription(RedisConnectionFactory factory,
                                   StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer,
                                   String consumerName,
                                   String groupName,
                                   StringRedisTemplate redisTemplate) {

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

        Subscription subscription = listenerContainer.receive(
                Consumer.from(groupName, consumerName),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                consumerListener
                );
        if(listenerContainer.isRunning()) log.info("Listener container is running.");
        else listenerContainer.start();

        log.info("Subscription created for consumer '{}' on group '{}', stream '{}'", consumerName, groupName, streamKey);
        
        log.info("StreamMessageListenerContainer started {}.", listenerContainer.isRunning() ? "successfully" : "unsuccessfully");

        return subscription;
    }
}