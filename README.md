# check-box

A real-time, multi-user collaborative checkbox app built with Node.js, Socket.IO, and Redis Pub/Sub. Every checkbox state is shared across all connected clients in real time. I built this to understand how WebSocket-based state sync actually works at a lower level - and what breaks when you skip things like race condition handling, input validation, and rate limiting.

---

## What it does

100 checkboxes. Everyone who opens the page sees the same state. Click one and every other connected browser updates instantly. State persists across page refreshes and server restarts via Redis. You can also run multiple server instances on different ports and they all stay in sync through a shared Redis Pub/Sub channel.

---

## Tech Stack

| Layer | Choice | Why |
|---|---|---|
| Runtime | Node.js v24 (ESM) | Native `import/export`, `import.meta.dirname`, top-level `await` - no Babel needed |
| Web Framework | Express 5 | Minimal - just serves static files and 2 REST endpoints |
| Real-time | Socket.IO 4 | Handles WebSocket with fallback, rooms, and namespaced events cleanly |
| State / Persistence | Redis (via ioredis) | Three separate connections: one for `GET`/`SET` (state), one for `PUBLISH`, one for `SUBSCRIBE` |
| Cache/Broker | Valkey | Redis-compatible, runs locally via Docker |
| Package Manager | pnpm | Faster installs, disk-efficient |

---

## Architecture

```
Browser Client
    │
    │  WebSocket (Socket.IO)
    ▼
Node.js Server (Express + Socket.IO)
    │                   ▲
    │ PUBLISH           │ SUBSCRIBE listener
    ▼                   │
         Redis / Valkey
         (checkbox:state key + Pub/Sub channel)
```

When a user checks a box:
1. Client emits `client:checkbox:change` to the server over WebSocket
2. Server validates the data and checks rate limit
3. Server writes the new full state array to Redis (`SET checkbox:state`)
4. Server publishes `{ index, checked }` to the `internal-server:checkbox:change` channel
5. The subscriber listener (on the same or a different server instance) receives the message and calls `io.emit('server:checkbox:change', ...)` to broadcast to all connected browsers

The reason for three Redis connections is a hard constraint of the Redis protocol: **a connection in `SUBSCRIBE` mode cannot issue regular commands** like `GET` or `SET`. So `subscriber` is dedicated to Pub/Sub, `publisher` handles `PUBLISH`, and `redis` handles all the `GET`/`SET` state operations.

---

## Why Redis Pub/Sub and not just `io.broadcast.emit`?

`socket.broadcast.emit` only works within a single Node process. If you run two server instances on port 8000 and 8001, a client connected to port 8001 won't hear about a change made by a client on port 8000.

With Redis as the message broker, every server instance subscribes to the same channel. A change published by one instance gets received by all instances, and each one broadcasts to its own connected sockets. This is how horizontal scaling works in real Socket.IO apps — it's also what the official `@socket.io/redis-adapter` does internally.

---

## Edge Cases I Actually Tested

**Race condition on page load**

The original version used `fetch('/checkboxes')` to get initial state and a separate socket listener for live updates. These two channels are unordered — if another user toggled a box while the fetch was in flight, the HTTP response would arrive after the socket event and overwrite the live update with stale data.

Fix: Removed HTTP fetch entirely. When a socket connects, the server immediately emits `server:init` with the full current state from Redis. All subsequent changes arrive over the same ordered socket channel. No two-channel synchronization problem.

**Malformed socket payloads crashing the server**

If a client sends `socket.emit('client:checkbox:change', null)`, destructuring `data` throws a `TypeError` and, without a `try/catch`, kills the entire Node process.

Fix: Added a guard before destructuring:
```js
if (!data || typeof data !== 'object') return;
```
Plus `Number.isInteger(index)` instead of `typeof index === 'number'` — the latter allows floats like `1.5` which would silently corrupt the state array.

**Broadcasting invalid data back to all clients**

The original code called `io.emit(...)` outside the bounds check, so even out-of-range or invalid changes were broadcast to every connected client.

Fix: Moved the emit inside the validated `if` block.

**Path resolution breaking when you run from a different directory**

`path.resolve('./public')` resolves relative to `process.cwd()`, which changes depending on where you run `node` from. `path.join(import.meta.dirname, 'public')` always resolves relative to the file itself.

**Redis subscriber errors crashing the app**

ioredis throws an `error` event when the connection drops. If you don't attach a listener, Node's default behavior is to throw an uncaught exception and crash.

Fix: Attached an `error` listener on every Redis connection in `redis.connection.js`.

**Spam from a single user**

Without rate limiting, a user can fire checkbox events as fast as the browser can send them - hundreds per second - which hammers both the WebSocket layer and Redis.

Fix: In-memory `Map<socket.id, timestamp>` on the server. On each event, calculate `Date.now() - lastActionTime`. If it's under the cooldown window (currently 2 seconds), emit `client:rate-limit-error` back to that socket only with the exact remaining wait time. The client reverts the checkbox state and shows a toast notification. The Map entry is deleted on disconnect so there's no memory leak.

---

## What I Learned About Scaling

The biggest realization was that **WebSockets don't solve distribution, they just move the problem**. Keeping state in memory (`state.checkboxes`) is fine for a single process but breaks the moment you add a second server. Any state that needs to be shared across instances has to live outside the process — in Redis, a database, or similar.

Redis Pub/Sub is a fire-and-forget model. There's no acknowledgment, no persistence, no replay for late subscribers. If a server restarts mid-message, that Pub/Sub event is gone. That's why the actual source of truth is `redis.set(CHECKBOX_STATE_KEY, ...)` — the Pub/Sub channel is just the signal to other instances that something changed, not the data itself.

**JSON Serialization vs. Atomic Bitmaps:**
Right now, the server reads the JSON string, parses it, updates the index, and writes it back (`GET` → `SET`). For 100 checkboxes this is straightforward and readable. However, it introduces a classic Read-Modify-Write race condition under high concurrency (two simultaneous clicks can overwrite each other). For higher scale or 1M checkboxes, the next step is using Redis Bitmaps (`SETBIT` and `GETBIT`), which perform atomic $O(1)$ in-place bit updates without serializing/deserializing entire JSON payloads.

The three-connection requirement for ioredis is a real gotcha that the Redis docs understate. A `SUBSCRIBE`d connection enters a special mode and can't run any other commands — you'll get a `WRONGTYPE` error or a silent hang. Separate connections per role is the correct pattern.

Rate limiting on the socket layer (not HTTP) requires different tooling. There's no middleware like `express-rate-limit` for Socket.IO. A `Map<socketId, timestamp>` is simple and accurate. The only concern is cleanup on disconnect — without it you'd accumulate stale entries for every disconnected user indefinitely.

---

## Project Structure

```
check-box/
├── index.js              # Server entry: Express + Socket.IO + Redis wiring
├── redis.connection.js   # Creates 3 separate ioredis connections
├── docker-compose.yml    # Valkey (Redis-compatible) on port 6379
├── public/
│   ├── index.html        # Frontend: socket events, toast notifications
│   └── styles.css        # UI: checkbox grid, dark mode, toast animations
└── package.json
```

---

## Running Locally

**Prerequisites:** Node.js ≥ 20, Docker, pnpm

```bash
# 1. Start Valkey (Redis)
docker compose up -d

# 2. Install dependencies
pnpm install

# 3. Start the server
pnpm start              # port 8000

# Or run 3 instances simultaneously
PORT=8000 node index.js &
PORT=8001 node index.js &
PORT=8002 node index.js &
```

Open `http://localhost:8000`, `http://localhost:8001`, and `http://localhost:8002` in separate browser tabs. Changes on any tab sync to all others in real time via the shared Redis channel.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8000` | HTTP port the server listens on |
| `REDIS_HOST` | `localhost` | Redis/Valkey host |
| `REDIS_PORT` | `6379` | Redis/Valkey port |

---

## API

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Serves `index.html` |
| `GET` | `/checkboxes` | Returns current checkbox state array from Redis |
| `GET` | `/health` | Returns `{ healthy: true }` |

### Socket Events

| Direction | Event | Payload | Description |
|---|---|---|---|
| Server → Client | `server:init` | `{ checkboxes: boolean[] }` | Full state on connect |
| Server → Client | `server:checkbox:change` | `{ index, checked }` | Live update from another user |
| Server → Client | `client:rate-limit-error` | `{ message, index, revertTo, remainingMs }` | Rate limit exceeded |
| Client → Server | `client:checkbox:change` | `{ index, checked }` | User toggled a checkbox |
