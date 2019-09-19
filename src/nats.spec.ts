import {connectToNATS, PowerNats, XMsg} from "./nats";
import {map, take} from "rxjs/operators";
import {expect} from "chai";

describe('PowerNats', function () {
    let power: PowerNats;
    before(async () => power = new PowerNats(await connectToNATS()));
    after(async () => power.close());

    it('should subscribe and unsubscribe', async function () {
        let test1$ = power.subject<string>({name: 'test1'});
        let msgPromise = test1$.pipe(take(1)).toPromise();
        test1$.next("Hello");
        let msg = await msgPromise;
        expect(msg.data).to.be.eq('Hello');
        expect(power.client.numSubscriptions()).eq(0);
    });

    it('should pipe data from one subject to another', async function () {
        type A = { a: string };
        type B = { b: string };
        let a$ = power.subject<A>({name: 'a', json: true});
        let b$ = power.subject<B>({name: 'b', json: true});
        a$.pipe(take(1), map<XMsg<A>, B>(value => ({b: value.data.a}))).subscribe(b$);
        a$.publish({a: "1"});
        let result = await b$.pipe(take(1)).toPromise();
        expect(result.data).to.deep.eq({b: '1'});
        expect(power.client.numSubscriptions()).eq(0);
    });

});
