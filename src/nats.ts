import {attempt, Schema, string} from "@hapi/joi";
import {Client, connect, Msg, Subscription, SubscriptionOptions} from "ts-nats";
import {Observable, Observer} from "rxjs";
import * as nuid from 'nuid';
import {map, share} from "rxjs/operators";

export async function connectToNATS(): Promise<Client> {
    return await connect(process.env.NATS_URI||'')
}

export interface XMsg<T, R = unknown> {
    subject: NatsSubject<T, R>;
    data?: T;
    reply?: NatsSubject<R, any>;
    sid: number;
    size: number;
}

export type NatsParseOptions = Pick<NatsSubjectOptions, 'json' | 'schema' | 'reply'>;

export type RawNatsOptions = Pick<NatsSubjectOptions, 'name' | 'subscribeOpts'>;

export interface NatsSubjectOptions {
    name: string;
    json?: boolean;
    schema?: Schema;
    subscribeOpts?: SubscriptionOptions;
    reply?: Omit<NatsSubjectOptions, 'name'>;
}


export class RawNatsSubject extends Observable<Msg> {
    constructor(private client: Client, opts: RawNatsOptions) {
        super(subscriber => {
            let subscriptionPromise = this.client
                .subscribe(opts.name, (err, msg) => {
                    if (err) {
                        subscriber.error(err);
                    } else {

                        subscriber.next(msg);
                    }
                }, opts.subscribeOpts);
            let subscription: Subscription;
            subscriptionPromise
                .then(value => subscription = value)
                .catch(err => {
                    subscriber.error(err);
                });
            return () => {
                subscription.unsubscribe();
            };
        });
    }
}


export class NatsSubject<T, R> extends Observable<XMsg<T, R>> implements Observer<T> {

    get name() {
        return this.opts.name;
    }

    constructor(private client: Client, private opts: NatsSubjectOptions) {
        super();
        this.source = new RawNatsSubject(client, opts).pipe(share(), this.parseMsg(client));
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
        this.publish(data)
    }

    error() {
    }

    complete() {
    }

    private parseMsg(client: Client) {
        let opts = this.opts;
        return map((msg: Msg): XMsg<T, R> => {
            let data = msg.data;
            if (opts.json && data !== undefined) {
                data = JSON.parse(data);
                if (opts.schema) {
                    data = attempt(data, opts.schema);
                }
            }
            let reply: NatsSubject<R, any>;
            if (msg.reply) {
                reply = new NatsSubject<R, any>(client, {...(opts.reply || {}), name: msg.reply});
            }
            return {subject: this, data, reply, sid: msg.sid, size: msg.size}
        })
    }
}

export class PowerNats {
    constructor(public readonly client: Client, public queue?: string) {
    }

    subject<T = unknown, R = unknown>(opts: NatsSubjectOptions | string): NatsSubject<T, R> {
        if (!opts) {
            throw new Error("missing subject options");
        }
        if (typeof opts === 'string') {
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
