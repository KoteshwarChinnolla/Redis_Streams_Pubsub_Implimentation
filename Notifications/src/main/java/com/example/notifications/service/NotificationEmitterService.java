package com.example.notifications.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Service
public class NotificationEmitterService {
    private static final Logger log = LoggerFactory.getLogger(NotificationEmitterService.class);

    ConcurrentHashMap<String, List<SseEmitter>> userEmitters = new ConcurrentHashMap<>();

    public SseEmitter addEmitter(String Employee_Id) {
        userEmitters.computeIfAbsent(Employee_Id, k ->{
            log.info("Creating new emitter list for Employee_Id: {}", k);
            return new CopyOnWriteArrayList<>();
        }).add(new SseEmitter(0L));
        return userEmitters.get(Employee_Id).get(userEmitters.get(Employee_Id).size() - 1);
    }


    public List<SseEmitter> getEmitters(String user) {
        return userEmitters.get(user);
    }

    public List<SseEmitter> removeEmitter(String Employee_Id, SseEmitter emitter) {
        List<SseEmitter> emitters = userEmitters.get(Employee_Id);
        if (emitters != null) {
            emitters.remove(emitter);
            log.info("Removed emitter for Employee_Id: {}", Employee_Id);
            if (emitters.isEmpty()) {
                // Optionally remove subscription when no emitters remain
                // Subscription sub = activeSubscription.remove(Employee_Id);
                // if (sub != null) {
                //     sub.cancel();
                //     log.info("Cancelled Redis subscription for {}", Employee_Id);
                // }
                userEmitters.remove(Employee_Id);
                return null;
            }
        }
        return emitters;
    }

    public void disconnectEmitter(String Employee_Id) {
        List<SseEmitter> emitters = userEmitters.get(Employee_Id);
        if (emitters != null) {
            for (SseEmitter emitter : emitters) {
                emitter.complete();
            }
            userEmitters.remove(Employee_Id);
        }
    }

    public void sendMessage(Map<String, String> message) {

        String receiver = message.get("receiver");

        if (receiver != null) {

            List<SseEmitter> emitters = userEmitters.get(receiver);
            if (emitters != null) {
                for (SseEmitter emitter : emitters) {
                    try {
                        emitter.send(SseEmitter.event().data(message));
                        log.info("Sent message to emitter for receiver: {}", receiver);
                    } catch (IOException e) {
                        emitters.remove(emitter);
                    }
                }
            }

            if ("all".equals(receiver)) {
                for (List<SseEmitter> allEmitters : userEmitters.values()) {
                    for (SseEmitter emitter : allEmitters) {
                        try {
                            emitter.send(SseEmitter.event().data(message));
                        } catch (IOException e) {
                            allEmitters.remove(emitter);
                        }
                    }
                }
            }
        }
    }
}
