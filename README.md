# RealTime_Notification_system

In general Streams and Pub Sub are the most interesting concepts of software development lifecycle, They serves for different purposes. In this repository I tried to demonstrate the difference between them using the Redis Streams and Redis PubSub.

I planned to take an example of Notification system where in there might be lot of instances of our application, but we cant predict where the user actually connected to, so that server can send notification. to resolve this we need to have a common temparery storage so that the notification can be destributiong amoung all the servers. Then notification can be sent to user no mattter, to wich server user is connected to.

<video controls autoplay src="https://github.com/KoteshwarChinnolla/Real_Time_Notification_System/blob/streams_and_pubsub/redis-streams-vs-pubsub.mp4"></video>



