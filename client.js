const ReadLines = require('n-readlines');
const { resolve } = require('path');
const liner = new ReadLines(resolve(__dirname, 'events.json'));



const MAX_CONCURRENCY = 10;
const LIVE_EVENT_URL = 'http://localhost:8000/liveEvent';



async function work() {

    const promises = []; // Perhaps use P-queue or bluebird here
    let line = liner.next();

    while (line) {

        if (promises.length >= MAX_CONCURRENCY) {
            console.log("Max concurrency reached, awaiting promises");
            await Promise.all(promises);
            promises.length = 0;
        }


        promises.push(sendRequest(line.toString('ascii')));

        line = liner.next();

    }


    await Promise.all(promises);

    console.log('Done');

}

async function sendRequest(line) {

    console.log('Sending request ', line);

    const res = await fetch(LIVE_EVENT_URL, {

        method: 'POST',
        body: line,
        headers: {
            "authorization": "secret",
            'Content-Type': 'application/json'
        }
    });

    console.log('Response: ', res.status);

}


work();