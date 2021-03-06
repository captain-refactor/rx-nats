import {attempt, Schema} from "@hapi/joi";
import {Client, connect, Msg, Subscription, SubscriptionOptions} from "ts-nats";
import {NextObserver, Observable, Subject} from "rxjs";
import * as nuid from "nuid";
import {map} from "rxjs/operators";

export async function connectToNATS(): Promise<Client> {
    return await connect(process.env.NATS_URI || "");
}

export interface XMsg<T, R = unknown, P = void> {
    subject: NatsSubject<T, R, P>;
    name: string;
    data?: T;
    reply?: NatsSubject<R, any>;
    sid: number;
    size: number;
}

type RawNatsOptions<P> = Pick<NatsSubjectOptions<P>, "queue" | "max" | "name">;

type NameFunction<P> = (params: P) => string
type NameParam<P> = NameFunction<P> | string;

export interface NatsSubjectOptions<P> {
    name: NameParam<P>;
    json?: boolean;
    /** Schema validation for subject */
    schema?: Schema;
    /** Name of the queue group for the subscription. True means that we will use default queue name */
    queue?: string;
    /** Maximum number of messages expected. When this number of messages is received the subscription auto [[Subscription.unsubscribe]]. */
    max?: number;
    /** Reply subject options */
    reply?: Omit<NatsSubjectOptions<void>, "name">;
}

export type NewSubjectOptions<P> = Omit<NatsSubjectOptions<P>, 'queue'> & { queue?: true | string };

function getName<P>(name: NameParam<P>, params?: P): string {
    if (typeof name === 'function') {
        return name(params);
    } else {
        return name;
    }
}

class HotNatsSubject<P> implements NextObserver<any> {
    private _subject = new Subject<Msg>();
    private _subscription: Subscription;

    constructor(private clientProvider: ClientProvider, private opts: RawNatsOptions<P>) {
    }

    /**
     * Subscribe to nats subject.
     * we us nats prefix to avoid collision with rxjs subscribe method.
     */
    async natsSubscribe(props?: P): Promise<void> {
        const {queue, max} = this.opts;
        this._subscription = await this.clientProvider.client.subscribe(
            getName(this.opts.name, props),
            (err, msg) => {
                if (err) {
                    this._subject.error(err);
                } else {
                    this._subject.next(msg);
                }
            },
            {queue, max}
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
        this.clientProvider.client.publish(getName(this.opts.name), data);
    }
}

class ColdNatsSubject<P> extends Observable<Msg> implements NextObserver<any> {
    private _hot: HotNatsSubject<P>;

    constructor(private clientProvider: ClientProvider, private opts: RawNatsOptions<P>, private params: P) {
        super(subscriber => {
            let subscription = this._hot.observable().subscribe(subscriber);
            this._hot.natsSubscribe(params).catch(reason => subscriber.error(reason));
            return () => {
                subscription.unsubscribe();
                this._hot.natsUnsubscribe();
            };
        });
        this._hot = new HotNatsSubject(clientProvider, opts);
    }

    next(data?) {
        this.clientProvider.client.publish(getName(this.opts.name, this.params), data);
    }
}

export class RxNatsError extends Error {
}

export class InvalidJSON extends RxNatsError {
    constructor(message: string, public data) {
        super(message);
    }
}

interface IPublishOptions<P> {
    reply?: string;
    params?: P;
}

export class NatsSubject<T, R, P = void> implements NextObserver<T> {
    private _hot: HotNatsSubject<P>;

    get name() {
        return this.opts.name;
    }

    readonly hot: Observable<XMsg<T, R, P>>;

    cold(params?: P): Observable<XMsg<T, R, P>> {
        return new ColdNatsSubject(this.clientProvider, this.opts, params).pipe(this.parseMsg());
    }

    constructor(private clientProvider: ClientProvider, private opts: NatsSubjectOptions<P>) {
        this._hot = new HotNatsSubject<P>(this.clientProvider, this.opts);
        this.hot = this._hot.observable().pipe(this.parseMsg());
    }

    /**
     * Subscribe to nats subject.
     * we us nats prefix to avoid collision with rxjs subscribe method.
     */
    natsSubscribe(props?: P) {
        return this._hot.natsSubscribe(props);
    }

    /**
     * unsubscribe from nats subject
     * we us nats prefix to avoid collision with rxjs unsubscribe method.
     */
    natsUnsubscribe() {
        return this._hot.natsUnsubscribe();
    }

    request(data: T, params?: P): NatsSubject<R, any> {
        const aname = getName(this.opts.name, params);
        const inboxName = `INBOX_${aname}_${nuid.next()}`;
        this.publish(data, {reply: inboxName});
        const inbox = this.opts.reply || {};
        return new NatsSubject<R, any>(this.clientProvider, {name: inboxName, ...inbox});
    }

    publish(data: T, opts?: IPublishOptions<P>): void {
        let toSend: any = data;
        if (this.opts.schema) {
            toSend = attempt(data, this.opts.schema);
        }
        if (data && this.opts.json) {
            toSend = JSON.stringify(data);
        }
        let subj = getName(this.opts.name, opts?.params);
        this.clientProvider.client.publish(subj, toSend, opts?.reply);
    }

    next(data: T) {
        this.publish(data);
    }

    private parseMsg() {
        let opts = this.opts;
        return map(
            (msg: Msg): XMsg<T, R, P> => {
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
                return {subject: this, name: msg.subject, data, reply, sid: msg.sid, size: msg.size};
            }
        );
    }
}

function isPromise<T>(obj: T | Promise<T>): obj is Promise<T> {
    return !!(obj as Promise<T>).then;

}

export class RxNats {
    private readonly clientProvider: ClientProvider;
    private _isInitialized = false;

    get client() {
        return this.clientProvider.client;
    }

    get isInitialized(): boolean {
        return this._isInitialized;
    }

    constructor(client: Client | Promise<Client> | ClientFn, public queue?: string) {
        if (isPromise(client)) {
            this.clientProvider = new PromiseClientProvider(client);
        } else if (typeof client === 'function') {
            this.clientProvider = new FunctionClientProvider(client);
        } else {
            this.clientProvider = new SimpleClientProvider(client);
        }
    }

    async init() {
        if (!this._isInitialized) {
            await this.clientProvider.init();
            this._isInitialized = true;
        }
    }

    subject<T = unknown, R = unknown, P = void>(
        opts: NewSubjectOptions<P> | NameParam<P>
    ): NatsSubject<T, R, P> {
        if (!opts) {
            throw new Error("missing subject options");
        }
        if (typeof opts === "string" || typeof opts === 'function') {
            opts = {name: opts};
        }
        if (opts.queue === true) {
            if (!this.queue) {
                throw new Error("no default queue available")
            }
            opts.queue = this.queue;
        }
        return new NatsSubject<T, R, P>(this.clientProvider, opts as NatsSubjectOptions<P>);
    }

    close() {
        if (this._isInitialized && !this.clientProvider.client.isClosed()) {
            this.clientProvider.client.close();
        }
    }
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
    constructor(public readonly client: Client) {
    }

    async init() {
    }
}

type ClientFn = (() => Client) | (() => Promise<Client>);

export class FunctionClientProvider implements ClientProvider {
    private _client: Client;
    get client(): Client {
        if (!this._client) {
            throw new Error("nats client not initialized");
        }
        return this._client;
    }

    constructor(private clientFn: ClientFn) {
    }

    async init() {
        this._client = await this.clientFn();
    }
}
