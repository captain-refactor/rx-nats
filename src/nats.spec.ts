import {connectToNATS, InvalidJSON, PowerNats, XMsg} from "./nats";
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
import {attempt, object, string} from "@hapi/joi";
import {interval, Observable, of, Subject} from "rxjs";

describe('PowerNats', function () {
    let power: PowerNats;
    before(async () => power = new PowerNats(await connectToNATS()));
    after(async () => power.close());

    it('should subscribe and unsubscribe', async function () {
        let subject = power.subject<string>({name: 'test1'});
        let msgPromise = subject.cold.pipe(take(1)).toPromise();
        subject.next("Hello");
        let msg = await msgPromise;
        expect(msg.data).to.be.eq('Hello');
        expect(power.client.numSubscriptions()).eq(0);
    });

    it('should pipe data from one subject to another', async function () {
        type A = { a: string };
        type B = { b: string };
        let a$ = power.subject<A>({name: 'a', json: true});
        let b$ = power.subject<B>({name: 'b', json: true});
        a$.cold.pipe(take(1), map<XMsg<A>, B>(value => ({b: value.data.a}))).subscribe(b$);
        a$.publish({a: "1"});
        let result = await b$.cold.pipe(take(1)).toPromise();
        expect(result.data).to.deep.eq({b: '1'});
        expect(power.client.numSubscriptions()).eq(0);
    });
    it('should propagate parsing error', async function () {
        let a$ = power.subject({
            name: 'x',
            json: true,
            schema: object({
                name: string().required()
            })
        });
        let promise = a$.cold.pipe(take(1), materialize()).toPromise();
        power.subject('x').publish("hello");
        let result = await promise;
        expect(result.error).to.be.an.instanceof(InvalidJSON);
    });

    it('should be possible to handle errors without resubscribe', async function () {
        let a$ = power.subject({
            name: 'x2',
            json: true,
            schema: object({
                name: string().required()
            })
        });
        await a$.subscribe();
        let promise = a$.hot.pipe(retry(), take(1)).toPromise();
        power.subject('x2').publish("bad value");
        a$.publish({name: "good value"});
        let result = await promise;
        a$.unsubscribe();
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
        let healthNatsSubject = power.subject({name: "service.health"});
        let serviceHealth = healthNatsSubject.cold.pipe(share());
        // service need to be alive at least 7 cycles
        let test = serviceHealth.pipe(take(7), pluck("data"), toArray());
        let running = true;
        startServiceCommand$.pipe(takeUntil(test)).subscribe(() => running = true);
        interval(50).pipe(takeUntil(test), filter(() => running), map(x => x.toString())).subscribe(healthNatsSubject);
        let serviceDown$ = serviceHealth.pipe(takeUntil(test), debounceTime(200));
        serviceDown$.subscribe(startServiceCommand$);
        serviceHealth.pipe(take(2), last()).subscribe(() => running = false);
        await test.toPromise();
    });
});
