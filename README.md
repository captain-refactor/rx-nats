# RX-NATS

Use nats as rxjs observable.

Feel free to dig around a try this lib.

## NatsSubject
This represents subject in nats. 
It implements Subscriber so you can do things like this
```
of(1,2,3).subscribe(natsSubject)
``` 

Class provides hot and cold observable. The difference is, that cold observable subscribes to nats on rxjs subscription and unsubscribes on error or complete. Hot observable need you to subscribe and unsubscribe manually on NatsSubject.
Hot observable is good for robust error handling.

Example:
```typescript
subject.hot.pipe(tap(handleRequest),retry()).subscribe()
```
this snippet is example, how to not get disconnected, when some error happen.




