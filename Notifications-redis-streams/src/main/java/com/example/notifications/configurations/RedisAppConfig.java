package com.example.notifications.configurations;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

@Configuration
class RedisAppConfig {
    // private static final Logger log = LoggerFactory.getLogger(RedisAppConfig.class);

    @Value("${redis.stream.key}")
    private String streamKey;

    @Value("${redis.stream.group}")
    private String groupName;

    private final RedisProperties redisProperties;

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
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }

    @Bean
    public TaskExecutor streamListenerExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(8);
        executor.setMaxPoolSize(16);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("stream-listener-");
        executor.initialize();
        return executor;
    }


    @Bean
    public StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> streamMessageListenerContainerOptions(
             TaskExecutor streamListenerExecutor
    ) {
        return StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofSeconds(1))
                .executor(streamListenerExecutor)
                .build();
    }

    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer(
            RedisConnectionFactory connectionFactory,
            StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options) {

        return StreamMessageListenerContainer.create(connectionFactory, options);
    }

}