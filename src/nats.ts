import {attempt, Schema, string} from "@hapi/joi";
import {Client, connect, Msg, Subscription, SubscriptionOptions} from "ts-nats";
import {Observable, Observer, Subject} from "rxjs";
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

export class HotNatsSubject {
    private _subject = new Subject<Msg>();
    private _subscription: Subscription;

    constructor(private client: Client, private opts: RawNatsOptions) {
    }

    async subscribe(): Promise<void> {
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

    unsubscribe(complete: boolean = true) {
        this._subscription && this._subscription.unsubscribe();
        if (complete) this._subject.complete();
    }

    observable() {
        return this._subject.asObservable();
    }
}

export class ColdNatsSubject extends Observable<Msg> {
    private _hot: HotNatsSubject;

    constructor(private client: Client, opts: RawNatsOptions) {
        super(subscriber => {
            let subscription = this._hot.observable().subscribe(subscriber);
            this._hot.subscribe().catch(reason => subscriber.error(reason));
            return () => {
                subscription.unsubscribe();
                this._hot.unsubscribe();
            };
        });
        this._hot = new HotNatsSubject(client, opts);
    }
}

export class RxNatsError extends Error {
}

export class InvalidJSON extends RxNatsError {
    constructor(message: string, public data) {
        super(message);
    }
}

export class NatsSubject<T, R> implements Observer<T> {
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

    subscribe() {
        return this._hot.subscribe();
    }

    unsubscribe() {
        return this._hot.unsubscribe();
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

    error() {
    }

    complete() {
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
        let subject = new NatsSubject<T, R>(this.client, opts);
        return subject;
    }

    close() {
        this.client.close();
    }
}

export interface FrameOptions {
    log?: boolean | string[]; // default true
}
