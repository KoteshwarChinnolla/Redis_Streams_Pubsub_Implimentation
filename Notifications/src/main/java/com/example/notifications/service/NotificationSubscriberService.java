package com.example.notifications.service;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Service;

import com.example.notifications.Consumer.SubscribeListen;

@Service
public class NotificationSubscriberService {
    private static final Logger log = LoggerFactory.getLogger(NotificationSubscriberService.class);
    @Autowired
    SubscribeListen subscribeListen;
    ConcurrentHashMap<String, Subscription> activeSubscription = new ConcurrentHashMap<>();

    public Subscription getSubscription(String Employee_Id) {
        return activeSubscription.get(Employee_Id);
    }

    public void addSubscription(String Employee_Id) {
        activeSubscription.computeIfAbsent(Employee_Id, k -> {
            log.info("Creating Redis subscription for {}", k);
            return subscribeListen.subscribe(Employee_Id, Employee_Id);
        });
    }

    public void removeSubscription(String Employee_Id) {
        Subscription subscription = activeSubscription.remove(Employee_Id);
        if (subscription != null) {
            subscription.cancel();
            log.info("Cancelled Redis subscription for {}", Employee_Id);
        }
    }

    public void disconnectSubscription(String Employee_Id) {
        Subscription subscription = activeSubscription.get(Employee_Id);
        if (subscription != null) {
            subscription.cancel();
            log.info("Unsubscribed from Redis stream for {}", Employee_Id);
            activeSubscription.remove(Employee_Id);
        }
    }
}
