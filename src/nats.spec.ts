import {connectToNATS, InvalidJSON, RxNats, XMsg} from "./nats";
import {
    map,
    materialize,
    retry,
    take,
    toArray,
    share,
    tap,
    takeUntil,
    debounceTime,
    filter,
    last, pluck
} from "rxjs/operators";
import {expect} from "chai";
import {object, string} from "@hapi/joi";
import {interval, Observable, of, range, Subject} from "rxjs";

describe('RxNats', function () {
    let rxNats: RxNats;
    before(async () => {
        rxNats = new RxNats(connectToNATS)
        await rxNats.init();
    });

    after(async () => rxNats.close());

    afterEach(() => {
        expect(rxNats.client.numSubscriptions()).eq(0);
    })

    it('should subscribe and unsubscribe', async function () {
        let subject = rxNats.subject<string>({name: 'test1'});
        let msgPromise = subject.cold().pipe(take(1)).toPromise();
        subject.next("Hello");
        let msg = await msgPromise;
        expect(msg.data).to.be.eq('Hello');
    });

    it('should pipe data from one subject to another', async function () {
        type A = { a: string };
        type B = { b: string };
        let a$ = rxNats.subject<A>({name: 'a', json: true});
        let b$ = rxNats.subject<B>({name: 'b', json: true});
        a$.cold().pipe(take(1), map<XMsg<A>, B>(value => ({b: value.data.a}))).subscribe(b$);
        a$.publish({a: "1"});
        let result = await b$.cold().pipe(take(1)).toPromise();
        expect(result.data).to.deep.eq({b: '1'});
    });
    it('should propagate parsing error', async function () {
        let a$ = rxNats.subject({
            name: 'x',
            json: true,
            schema: object({
                name: string().required()
            })
        });
        let promise = a$.cold().pipe(take(1), materialize()).toPromise();
        rxNats.subject('x').publish("hello");
        let result = await promise;
        expect(result.error).to.be.an.instanceof(InvalidJSON);
    });

    it('should be possible to handle errors without resubscribe', async function () {
        let a$ = rxNats.subject({
            name: 'x2',
            json: true,
            schema: object({
                name: string().required()
            })
        });
        await a$.natsSubscribe();
        let promise = a$.hot.pipe(retry(), take(1)).toPromise();
        rxNats.subject('x2').publish("bad value");
        a$.publish({name: "good value"});
        let result = await promise;
        a$.natsUnsubscribe();
        expect(result.data).to.be.deep.eq({name: "good value"});
    });
    it('should not unsubscribe shared observable', async function () {
        let source = new Observable(subscriber => {
            of(1, 2, 3, 4, 5).subscribe(subscriber);
        });
        let result = await source.pipe(share(), tap(x => {
            if (x == 3) throw new Error("boom")
        }), retry(), toArray()).toPromise();
        expect(result).to.be.deep.eq([1, 2, 4, 5]);
    });
    it('should observe service health and start it again, when health check fails', async function () {
        let startServiceCommand$ = new Subject();
        let healthNatsSubject = rxNats.subject({name: "service.health"});
        let serviceHealth = healthNatsSubject.cold().pipe(share());
        // service need to be alive at least 7 cycles
        let test = serviceHealth.pipe(take(7), pluck("data"), toArray());
        let running = true;
        startServiceCommand$.pipe(takeUntil(test)).subscribe(() => running = true);
        interval(50).pipe(takeUntil(test), filter(() => running), map(x => x.toString())).subscribe(healthNatsSubject);
        let serviceDown$ = serviceHealth.pipe(takeUntil(test), debounceTime(200));
        serviceDown$.subscribe(startServiceCommand$);
        serviceHealth.pipe(take(2), last()).subscribe(() => running = false);
        let result = await test.toPromise();
        expect(result.length).eq(7);
    });

    it('should subscribe to hot observable and receive messages', async function () {
        // let sub = new Subject();
        let sub = rxNats.subject("hot");
        await sub.natsSubscribe();
        let testPromise = sub.hot.pipe(take(3), pluck('data'), toArray()).toPromise();
        range(0, 3).pipe(map(x => x.toString())).subscribe(sub);
        let result = await testPromise;
        expect(result).to.be.deep.eq(['0', '1', '2']);
        sub.natsUnsubscribe();
    });

    it('should stay subscribed, when error happens', async function () {
        let sub = rxNats.subject("resource.get");
        await sub.natsSubscribe();
        let result = [];
        let route = sub.hot.pipe(tap(msg => {
            if (msg.data == 'throw') throw new Error("test error");
            result.push(msg.data);
        }), pluck('data'), retry(), take(2), toArray());
        of('a', 'throw', 'b').subscribe(sub);
        await route.toPromise();
        expect(result).deep.eq(['a', 'b']);
        sub.natsUnsubscribe();
    });

    it('should publish with parameters', async () => {
        const sub = rxNats.subject({name: (id: string) => `item.${id}`});
        expect(rxNats.client.numSubscriptions()).eq(0);
        const prom = sub.cold('*').pipe(take(1)).toPromise();
        expect(rxNats.client.numSubscriptions()).eq(1);

        sub.publish('hello', {
            params: '4'
        });
        const result = await prom;
        expect(result.name).to.eq('item.4');
        expect(rxNats.client.numSubscriptions()).eq(0);
    });

    it('should subscribe to data in hot observable', async function () {
        const sub = rxNats.subject('hot');
        await sub.natsSubscribe();
        const resultPromise = sub.hot.pipe(take(3), pluck('data'), toArray()).toPromise();
        range(0, 5).pipe(map(x => x.toString())).subscribe(sub);
        let result = await resultPromise;
        expect(result).deep.eq(['0', '1', '2']);
        sub.natsUnsubscribe();
    });
});
