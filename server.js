const express = require('express');
const Joi = require('joi');
const { existsSync, mkdirSync } = require('fs');
const { writeFile } = require('fs').promises;
const { v4: uuidv4 } = require('uuid');
const { resolve } = require('path');
const app = express();
const port = 8000;
const AUTHORIZATION_HEADER_NAME = "authorization";
const { Client } = require('pg');


class UserRepository {
    constructor(client) {
        this.client = client;
    }

    async connect() {
        return this.client.connect();
    }

    async getUserData(userId) {
        const res = await this.client.query('SELECT * FROM users.users_revenue WHERE user_id = $1', [userId]);
        return res.rows.length > 0 ? res.rows[0] : null;
    }

    async disconnect() {
        return this.client.end();
    }

}

// Should be taken from .env file or provided to ENV when building the container
const repository = new UserRepository(new Client({ connectionString: 'postgres://postgres:Password1!@localhost:5432/fin' }));



const LIVE_EVENT_SCHEMA = Joi.object({
    userId: Joi.string().required(),
    name: Joi.string().valid('add_revenue', 'subtract_revenue').required(),
    value: Joi.number().positive().required()
});

app.use(express.json());


// Validating each request has an authorization header with a value of 'secret'
app.use((req, res, next) => {

    if (req.headers[AUTHORIZATION_HEADER_NAME] === 'secret') {
        next();
    }
    else {
        res.status(401).send({ error: 'Unauthorized' });
    }
});


app.post('/liveEvent', async (req, res) => {

    const { body } = req;

    if (LIVE_EVENT_SCHEMA.validate(body).error) {

        return res.status(400).send({ error: 'Bad request' });
    }

    await storeEvent(body);

    return res.status(204).send(); //Maybe status 201?

});

async function storeEvent(event) {

    const dir = resolve(__dirname, 'events', event.userId);
    if (!existsSync(dir)) {
        mkdirSync(dir, { recursive: true });
    }

    const filePath = resolve(dir, `${uuidv4()}.json`);

    return writeFile(filePath, JSON.stringify(event));
}

app.get('/userEvents', async (req, res) => {

    const { userId } = req.query;

    if (!userId) {
        return res.status(400).send({ error: 'Bad request' });
    }

    const data = await getUserData(userId);

    if (data) {

        return res.status(200).send(data);
    } else {
        return res.status(404).send({ error: 'User not found' });
    }
});

async function getUserData(userId) {

    const data = await repository.getUserData(userId);

    return data;
}

repository.connect().then(() => {
    console.log('Connected to DB');
}).catch((err) => {
    console.log('Failed to connect to DB', err);
});

app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});

app.on('close', () => {
    repository.disconnect();
});



