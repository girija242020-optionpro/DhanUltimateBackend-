# Dhan Ultimate Backend v1.1 — Universal Data Plane

This build fixes the Render memory crash seen when the large detailed Dhan instrument master was parsed at startup.

## What changed
- Uses Dhan's **compact** instrument master by default.
- Instrument master is **lazy-loaded only when `/api/v1/instruments/*` is requested**.
- CSV is parsed as a stream; the full CSV text and temporary row-object array are never held together.
- The Dhan WebSocket feed starts independently and does not require the instrument master in RAM.
- Existing REST/WebSocket/price-alert/push architecture is preserved.

## Render
- Root Directory: `.`
- Build Command: `npm install`
- Start Command: `npm start`
- Runtime: Node 20+

Required environment variables:
- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN` (or `DHAN_PIN` + `DHAN_TOTP_SECRET`)
- `VAPID_PUBLIC_KEY`
- `VAPID_PRIVATE_KEY`
- `VAPID_SUBJECT`
- `CLIENT_API_KEY` (optional; if set, `/api/*` requires `x-client-key`)

Optional:
- `INSTRUMENT_URL` — defaults to `https://images.dhan.co/api-data/api-scrip-master.csv`
- `MAX_TICKS_PER_INSTRUMENT` — default 3000
- `MAX_CANDLES_PER_INSTRUMENT` — default 2000
- `INSTRUMENT_REFRESH_MS` — default 6 hours
- `TICK_STALE_MS` — default 15 seconds

## First verification after deploy
Open:
`https://YOUR-SERVICE.onrender.com/health`

Expected basic state:
- `success: true`
- `dhanConfigured: true`
- `feedState` should become `CONNECTED`
- `subscribedInstruments` remains 0 until a PWA subscribes

Do not expect market ticks before a client subscribes to an instrument. This backend is a data provider and does not subscribe to the entire market by default.
