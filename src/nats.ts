import {attempt, Schema} from "@hapi/joi";
import {Client, connect, Msg, Subscription, SubscriptionOptions} from "ts-nats";
import {NextObserver, Observable, Subject} from "rxjs";
import * as nuid from "nuid";
import {map} from "rxjs/operators";

export async function connectToNATS(): Promise<Client> {
    return await connect(process.env.NATS_URI || "");
}

export interface XMsg<T, R = unknown> {
    subject: NatsSubject<T, R>;
    data?: T;
    reply?: NatsSubject<R, any>;
    sid: number;
    size: number;
}

export type RawNatsOptions = Pick<NatsSubjectOptions, "name" | "subscribeOpts">;

export interface NatsSubjectOptions {
    name: string;
    json?: boolean;
    schema?: Schema;
    subscribeOpts?: SubscriptionOptions;
    reply?: Omit<NatsSubjectOptions, "name">;
}

export class HotNatsSubject implements NextObserver<any> {
    private _subject = new Subject<Msg>();
    private _subscription: Subscription;

    constructor(private client: Client, private opts: RawNatsOptions) {
    }

    /**
     * Subscribe to nats subject.
     * we us nats prefix to avoid collision with rxjs subscribe method.
     */
    async natsSubscribe(): Promise<void> {
        this._subscription = await this.client.subscribe(
            this.opts.name,
            (err, msg) => {
                if (err) {
                    this._subject.error(err);
                } else {
                    this._subject.next(msg);
                }
            },
            this.opts.subscribeOpts
        );
    }

    /**
     * unsubscribe from nats subject
     * we us nats prefix to avoid collision with rxjs unsubscribe method.
     */
    natsUnsubscribe(complete: boolean = true) {
        this._subscription && this._subscription.unsubscribe();
        if (complete) this._subject.complete();
    }

    observable() {
        return this._subject.asObservable();
    }

    /**
     * to implement subscriber
     */
    next(data?) {
        this.client.publish(this.opts.name, data);
    }
}

export class ColdNatsSubject extends Observable<Msg> implements NextObserver<any> {
    private _hot: HotNatsSubject;

    constructor(private client: Client, private opts: RawNatsOptions) {
        super(subscriber => {
            let subscription = this._hot.observable().subscribe(subscriber);
            this._hot.natsSubscribe().catch(reason => subscriber.error(reason));
            return () => {
                subscription.unsubscribe();
                this._hot.natsUnsubscribe();
            };
        });
        this._hot = new HotNatsSubject(client, opts);
    }

    next(data?) {
        this.client.publish(this.opts.name, data);
    }
}

export class RxNatsError extends Error {
}

export class InvalidJSON extends RxNatsError {
    constructor(message: string, public data) {
        super(message);
    }
}

export class NatsSubject<T, R> implements NextObserver<T> {
    private _hot: HotNatsSubject;

    get name() {
        return this.opts.name;
    }

    readonly hot: Observable<XMsg<T, R>>;

    readonly cold: Observable<XMsg<T, R>>;

    constructor(private client: Client, private opts: NatsSubjectOptions) {
        this._hot = new HotNatsSubject(this.client, this.opts);
        this.hot = this._hot.observable().pipe(this.parseMsg());
        this.cold = new ColdNatsSubject(this.client, this.opts).pipe(this.parseMsg());
    }

    /**
     * Subscribe to nats subject.
     * we us nats prefix to avoid collision with rxjs subscribe method.
     */
    natsSubscribe() {
        return this._hot.natsSubscribe();
    }

    /**
     * unsubscribe from nats subject
     * we us nats prefix to avoid collision with rxjs unsubscribe method.
     */
    natsUnsubscribe() {
        return this._hot.natsUnsubscribe();
    }

    request(data?: T): NatsSubject<R, any> {
        let name = `INBOX_${this.opts.name}_${nuid.next()}`;
        this.publish(data, name);
        let inbox = this.opts.reply || {};
        return new NatsSubject<R, any>(this.client, {name, ...inbox});
    }

    publish(data: T, reply?: string): void {
        let toSend: any = data;
        if (this.opts.schema) {
            toSend = attempt(data, this.opts.schema);
        }
        if (data && this.opts.json) {
            toSend = JSON.stringify(data);
        }
        this.client.publish(this.opts.name, toSend, reply);
    }

    next(data: T) {
        this.publish(data);
    }

    private parseMsg() {
        let opts = this.opts;
        return map(
            (msg: Msg): XMsg<T, R> => {
                let data = msg.data;
                if (opts.json && data !== undefined) {
                    try {
                        data = JSON.parse(data);
                    } catch (e) {
                        throw new InvalidJSON(e, msg.data);
                    }
                    if (opts.schema) {
                        data = attempt(data, opts.schema);
                    }
                }
                let reply: NatsSubject<R, any>;
                if (msg.reply) {
                    reply = new NatsSubject<R, any>(this.client, {
                        ...(opts.reply || {}),
                        name: msg.reply
                    });
                }
                return {subject: this, data, reply, sid: msg.sid, size: msg.size};
            }
        );
    }
}

export class PowerNats {
    constructor(public readonly client: Client, public queue?: string) {
    }

    subject<T = unknown, R = unknown>(
        opts: NatsSubjectOptions | string
    ): NatsSubject<T, R> {
        if (!opts) {
            throw new Error("missing subject options");
        }
        if (typeof opts === "string") {
            opts = {name: opts};
        }
        opts.subscribeOpts = opts.subscribeOpts || {};
        if (this.queue) opts.subscribeOpts.queue = this.queue;
        return new NatsSubject<T, R>(this.client, opts);
    }

    close() {
        this.client.close();
    }
}

export interface FrameOptions {
    log?: boolean | string[]; // default true
}
