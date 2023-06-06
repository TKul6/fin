const { unlink, rmdir } = require('fs/promises');
const { resolve } = require('path');
const { readdir, readFile } = require('fs').promises;
const MAX_CONCURRENCY = 10;
const ADD = 'add_revenue';
const SUBTRACT = 'subtract_revenue';
const EVENTS_DIRECTORY = resolve(__dirname, 'events')
const DELETE_FILES_MAX_CONCURRENCY = 2;
const { Client } = require('pg');
const Promise = require('bluebird');

// I would have export the repository to a separate file, but I don't want to over complicate things.
class UserRepository {
    constructor(client) {
        this.client = client;
    }

    async connect() {
        return this.client.connect();
    }

    async updateRevenue(userId, newRevenue) {
        // Maybe create SP for this
        const res = await this.client.query(`
      INSERT INTO users.users_revenue (user_id, revenue)
      VALUES ($1, $2)
      ON CONFLICT (user_id)
      DO UPDATE SET revenue = users.users_revenue.revenue + EXCLUDED.revenue;
    `, [userId, newRevenue]);

        return res;
    }

    async disconnect() {
        return this.client.end();
    }

}

const repository = new UserRepository(new Client({ connectionString: 'postgres://postgres:Password1!@localhost:5432/fin' }));
repository.connect();

async function work() {

    const usersIds = await getUsersIds();

    await Promise.map(usersIds, async (userId) => await HandleUser(userId), { concurrency: MAX_CONCURRENCY });

    console.log('Done');
    repository.disconnect();
}

async function HandleUser(userId) {

    console.log("Handling user ", userId);

    const files = await readdir(resolve(EVENTS_DIRECTORY, userId));

    const revenueFromFiles = await calculateNewRevenue(userId, files);

    await updateRevenueInRepository(userId, revenueFromFiles);

    await deleteFiles(userId, files);

}

async function deleteFiles(userId, files, {deleteParentFolder = true} = {}) {

await Promise.map(files, async (file) => await unlink(resolve(EVENTS_DIRECTORY, userId, file)), { concurrency: DELETE_FILES_MAX_CONCURRENCY });

    if(deleteParentFolder) {
        await rmdir(resolve(EVENTS_DIRECTORY, userId));
    }

}

async function updateRevenueInRepository(userId, newRevenue) {

    return repository.updateRevenue(userId, newRevenue);

}


async function calculateNewRevenue(userId, files) {

    let amount = 0;

    for (const file of files) {

        const content = await readFile(resolve(EVENTS_DIRECTORY, userId, file));
        const data = JSON.parse(content);

        switch (data.name) {
            case ADD:
                amount += data.value;
                break;
            case SUBTRACT:
                amount -= data.value;
                break;
            default:
                throw new Error('Unknown event type ' + data.name);
        }

    }

    return amount;

}

async function getUsersIds() {

    return readdir(EVENTS_DIRECTORY);



}

work();