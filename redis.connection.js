import Redis from "ioredis";

function createRedisConnection() {
    const redis = new Redis({
        host: process.env.REDIS_HOST ?? 'localhost',
        port: Number(process.env.REDIS_PORT ?? 6379),
        lazyConnect: false,
    });

    redis.on('error', (err) => {
        console.error('[Redis Error]', err.message);
    });

    return redis;
}
export const redis = createRedisConnection();
export const publisher = createRedisConnection();

export const subscriber = createRedisConnection();

