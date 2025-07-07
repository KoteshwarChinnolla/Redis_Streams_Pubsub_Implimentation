package com.example.notifications.configurations;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

// import io.lettuce.core.Consumer; // Remove this line

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;

import com.example.notifications.Consumer.EventListener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
class RedisAppConfig {
    private static final Logger log = LoggerFactory.getLogger(RedisAppConfig.class);

    private final RedisProperties redisProperties;

    @Autowired
    EventListener EventListener;

    public RedisAppConfig(RedisProperties redisProperties) {
        this.redisProperties = redisProperties;
    }

    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        RedisSentinelConfiguration sentinelConfig = new RedisSentinelConfiguration()
                .master(redisProperties.getSentinel().getMaster());

        for (String s : redisProperties.getSentinel().getNodes()) {
            String[] parts = s.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid sentinel node: " + s);
            }
            sentinelConfig.sentinel(parts[0], Integer.parseInt(parts[1]));
        }

        // This sets the password for Redis master
        sentinelConfig.setPassword(RedisPassword.of(redisProperties.getPassword()));

        return new LettuceConnectionFactory(sentinelConfig);
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        log.info("done setting redisTemplate");
        return template;

    }
    @Bean
    public MessageListenerAdapter messageListener() { 
        return new MessageListenerAdapter(EventListener);
    }

    @Bean
    public RedisMessageListenerContainer redisContainer() {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory());
        return container;
    }

}