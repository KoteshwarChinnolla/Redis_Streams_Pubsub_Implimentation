package com.example.notifications.controller;

import com.example.notifications.Consumer.SubscribeListen;
import com.example.notifications.entity.Notification;
import com.example.notifications.producer.ProduceMessage;
import com.example.notifications.service.NotificationEmitterService;
import com.example.notifications.service.NotificationService;

import org.springframework.http.MediaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;


import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private static final Logger log = LoggerFactory.getLogger(NotificationController.class);

    @Autowired
    private NotificationService notificationService;

    @Autowired
    private NotificationEmitterService notificationEmitterService;

    @Autowired
    private ProduceMessage produceMessage;


    @PostMapping("/send")
    public String sendNotification(@RequestBody Notification notification) {
        notificationService.sendNotification(
                notification.getReceiver(),
                notification.getMessage(),
                notification.getSender(),
                notification.getType(),
                notification.getLink()
        );
        
        try {
            Long result = produceMessage.sendMessage(notification, notification.getReceiver());
            return ""+result;
        } catch (Exception e) {
            return "Failed to send notification: " + e.getMessage();
        }
    }

    @GetMapping("/unread/{user}")
    public ResponseEntity<List<Notification>> getUnread(@PathVariable String user) {
        return ResponseEntity.ok(notificationService.getUnreadNotifications(user));
    }

    @GetMapping("/all/{user}")
    public ResponseEntity<List<Notification>> getAll(@PathVariable String user) {
        return ResponseEntity.ok(notificationService.getAllNotifications(user));
    }

    @PutMapping("/read/{id}")
    public ResponseEntity<String> markRead(@PathVariable Long id) {
        notificationService.markAsRead(id);
        return ResponseEntity.ok("Notification marked as read");
    }
    @GetMapping("/unread-count/{receiver}")
    public ResponseEntity<Long> getUnreadCount(@PathVariable String receiver) {
        return ResponseEntity.ok(notificationService.getUnreadCount(receiver));
    }

    @Autowired
    SubscribeListen subscribeListen;
    
    @GetMapping(value = "/stream/{Employee_Id}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@PathVariable String Employee_Id) {
        log.info("New SSE connection for Employee_Id: {}", Employee_Id);
        SseEmitter emitter = notificationEmitterService.addEmitter(Employee_Id);
        subscribeListen.subscribeUser(Employee_Id);
        
        emitter.onCompletion(() -> removeEmitter(Employee_Id, emitter));
        emitter.onTimeout(() -> removeEmitter(Employee_Id, emitter));
        emitter.onError((e) -> removeEmitter(Employee_Id, emitter));

        return emitter;
    }

    @GetMapping(value = "/stream/{Employee_Id}/disconnect")
    public ResponseEntity<String> disconnect(@PathVariable String Employee_Id) {
        notificationEmitterService.disconnectEmitter(Employee_Id);
        subscribeListen.unsubscribeUser(Employee_Id);
        return ResponseEntity.ok("Unsubscribed from notifications for user: " + Employee_Id);
    }

    public void removeEmitter(String Employee_Id, SseEmitter emitter) {
        List<SseEmitter> emitters = notificationEmitterService.removeEmitter(Employee_Id, emitter);
        if(emitters == null ||emitters.isEmpty()) {
            // Optionally remove subscription when no emitters remain
            subscribeListen.unsubscribeUser(Employee_Id);
            log.info("Cancelled Redis subscription for {}", Employee_Id);
        }
    }

}
