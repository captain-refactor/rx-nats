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

    constructor(private clientProvider: ClientProvider, private opts: RawNatsOptions) {
    }

    /**
     * Subscribe to nats subject.
     * we us nats prefix to avoid collision with rxjs subscribe method.
     */
    async natsSubscribe(): Promise<void> {
        this._subscription = await this.clientProvider.client.subscribe(
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
        this.clientProvider.client.publish(this.opts.name, data);
    }
}

export class ColdNatsSubject extends Observable<Msg> implements NextObserver<any> {
    private _hot: HotNatsSubject;

    constructor(private clientProvider: ClientProvider, private opts: RawNatsOptions) {
        super(subscriber => {
            let subscription = this._hot.observable().subscribe(subscriber);
            this._hot.natsSubscribe().catch(reason => subscriber.error(reason));
            return () => {
                subscription.unsubscribe();
                this._hot.natsUnsubscribe();
            };
        });
        this._hot = new HotNatsSubject(clientProvider, opts);
    }

    next(data?) {
        this.clientProvider.client.publish(this.opts.name, data);
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

    constructor(private clientProvider: ClientProvider, private opts: NatsSubjectOptions) {
        this._hot = new HotNatsSubject(this.clientProvider, this.opts);
        this.hot = this._hot.observable().pipe(this.parseMsg());
        this.cold = new ColdNatsSubject(this.clientProvider, this.opts).pipe(this.parseMsg());
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
        return new NatsSubject<R, any>(this.clientProvider, {name, ...inbox});
    }

    publish(data: T, reply?: string): void {
        let toSend: any = data;
        if (this.opts.schema) {
            toSend = attempt(data, this.opts.schema);
        }
        if (data && this.opts.json) {
            toSend = JSON.stringify(data);
        }
        this.clientProvider.client.publish(this.opts.name, toSend, reply);
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
                    reply = new NatsSubject<R, any>(this.clientProvider, {
                        ...(opts.reply || {}),
                        name: msg.reply
                    });
                }
                return {subject: this, data, reply, sid: msg.sid, size: msg.size};
            }
        );
    }
}

function isPromise<T>(obj: T | Promise<T>): obj is Promise<T> {
    return !!(obj as Promise<T>).then;

}

export class PowerNats {
    private readonly clientProvider: ClientProvider;

    get client() {
        return this.clientProvider.client;
    }

    constructor(client: Client | Promise<Client>, public queue?: string) {
        if (isPromise(client)) {
            this.clientProvider = new PromiseClientProvider(client);
        } else {
            this.clientProvider = new SimpleClientProvider(client);
        }
    }

    async init() {
        await this.clientProvider.init();
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
        return new NatsSubject<T, R>(this.clientProvider, opts);
    }

    close() {
        this.clientProvider.client.close();
    }
}

export interface FrameOptions {
    log?: boolean | string[]; // default true
}

export interface ClientProvider {
    client: Client;

    init(): Promise<void>;
}

export class PromiseClientProvider implements ClientProvider {
    private _client: Client;
    get client(): Client {
        if (!this._client) {
            throw new Error("nats client not initialized");
        }
        return this._client;
    }

    constructor(private promise: Promise<Client>) {
    }

    async init() {
        this._client = await this.promise;
    }
}

export class SimpleClientProvider implements ClientProvider {
    constructor(public client: Client) {
    }

    async init() {
    }
}
