import http from 'node:http'
import express from 'express'
import path from 'node:path';
import { Server } from 'socket.io';
import { publisher, subscriber, redis } from './redis.connection.js';



const CHECKBOX_SIZE = 100;
const CHECKBOX_STATE_KEY = 'checkbox:state';
const REDIS_CHANNEL = 'internal-server:checkbox:change';
const RATE_LIMIT_COOLDOWN_MS = 2000; 

const userLastActionMap = new Map();

const state = {
    checkboxes: new Array(CHECKBOX_SIZE).fill(false)
};

async function main() {
    const app = express();
    const server = http.createServer(app);
    const PORT = process.env.PORT ?? 8000;

    const io = new Server();
    io.attach(server);

    await subscriber.subscribe(REDIS_CHANNEL);
    subscriber.on('message', (channel, message) => {
        if (channel === REDIS_CHANNEL) {
            try {
                const { index, checked } = JSON.parse(message);
                if (Number.isInteger(index) && index >= 0 && index < state.checkboxes.length) {
                    state.checkboxes[index] = checked;
                    io.emit('server:checkbox:change', { index, checked });
                }
            } catch (err) {
                console.error('Failed to parse Redis message:', err);
            }
        }
    });

    io.on('connection', async (socket) => {
        console.log(`Socket connected`, { id: socket.id });

        try {
            const existingState = await redis.get(CHECKBOX_STATE_KEY);
            const checkboxes = existingState ? JSON.parse(existingState) : state.checkboxes;
            socket.emit('server:init', { checkboxes });
        } catch {
            socket.emit('server:init', { checkboxes: state.checkboxes });
        }

        socket.on('disconnect', () => {
            userLastActionMap.delete(socket.id);
        });

        socket.on('client:checkbox:change', async (data) => {
            console.log(`[Socket:${socket.id}]:client:checkbox:change`, data);
            if (!data || typeof data !== 'object') return;
            const { index, checked } = data;

            const now = Date.now();
            const lastActionTime = userLastActionMap.get(socket.id);

            if (lastActionTime) {
                const timeGap = now - lastActionTime;
                if (timeGap < RATE_LIMIT_COOLDOWN_MS) {
                    const remainingMs = RATE_LIMIT_COOLDOWN_MS - timeGap;
                    const remainingSec = (remainingMs / 1000).toFixed(1);

                    socket.emit('client:rate-limit-error', {
                        message: `Please wait ${remainingSec}s before checking another box.`,
                        remainingMs,
                        index,
                        revertTo: !checked
                    });
                    return;
                }
            }

            userLastActionMap.set(socket.id, now);

            try {
                const existingState = await redis.get(CHECKBOX_STATE_KEY);

                if (existingState) {
                    const remoteData = JSON.parse(existingState);
                    remoteData[index] = data.checked;
                    await redis.set(CHECKBOX_STATE_KEY, JSON.stringify(remoteData));
                } else {
                    const initialState = new Array(CHECKBOX_SIZE).fill(false);
                    if (Number.isInteger(index)) {
                        initialState[index] = data.checked;
                    }
                    await redis.set(
                        CHECKBOX_STATE_KEY,
                        JSON.stringify(initialState),
                    );
                }

                await publisher.publish(
                    REDIS_CHANNEL,
                    JSON.stringify({ index, checked: Boolean(checked) })
                );
            } catch (err) {
                console.error('Redis error on checkbox change:', err);
            }
        });
    });

    app.use(express.static(path.join(import.meta.dirname, 'public')));
    app.get('/health', (req, res) => res.json({ healthy: true }));

    app.get('/checkboxes', async (req, res) => {
        const existingState = await redis.get(CHECKBOX_STATE_KEY);
        if (existingState) {
            const remoteData = JSON.parse(existingState);
            return res.json({ checkboxes: remoteData });
        }
        return res.json({ checkboxes: state.checkboxes });
    });

    server.listen(PORT, () => {
        console.log(`Server is running on http://localhost:${PORT}`);
    });

    const shutdown = async () => {
        console.log('\nShutting down server gracefully...');
        server.close();
        io.close();
        await subscriber.quit();
        await publisher.quit();
        await redis.quit();
        process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
}

await main();