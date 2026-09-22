# DHAN ULTIMATE BACKEND v1

Universal **data-plane** for multiple PWAs / trading terminals.

## Core design

`Dhan WebSocket/REST -> one backend -> many PWAs`

The backend does **not** contain RSI/DEMA/HMA/Gann/Wall-Sniper/etc. strategy decisions. It provides normalized market data so every client can calculate its own indicators from the same live stream.

This is the same basic separation used by trading platforms: one market-data layer feeds many indicator/strategy consumers. Dhan itself documents that its platform uses its live market-feed WebSocket connections, and the feed is event/tick based. Dhan v2 supports ticker, quote and full packets; quote includes volume and quote fields, while full includes OI and market depth. citeturn2search1turn3search4

## What this backend provides

### Live stream
- Dhan v2 WebSocket ingestion.
- Binary little-endian packet decoding.
- Ticker / Quote / Full modes.
- LTP, LTT, last quantity, ATP, volume, buy/sell quantity, OHLC.
- OI, OI day high/low.
- 5-level depth from Full packets.
- In-memory tick cache.
- 1/3/5/15/25/60-minute live candle aggregation.
- Reconnect + stale-feed detection.
- Subscription multiplexing: repeated PWA subscriptions share one Dhan subscription.

Dhan currently documents up to 5 live-feed WebSocket connections per user and up to 5000 instruments per connection, with at most 100 instruments per subscription message. citeturn2search1

### REST pass-through / data APIs
- LTP
- Quote / market depth
- Intraday history
- Option chain
- Option expiry list
- Local snapshots, ticks and live candles

Dhan's Market Quote API can request up to 1000 instruments in one request, while the Option Chain API returns OI, Greeks, volume, LTP, IV and best bid/ask for all strikes of an underlying. citeturn1search5turn3search0

Dhan's intraday historical API supplies OHLC, volume and optional OI for minute intervals. citeturn1search0

### Instruments
The backend downloads Dhan's detailed instrument master and exposes symbol/security-ID search. Dhan publishes both compact and detailed scrip-master CSVs containing Security IDs and derivative metadata. citeturn3search8

### Universal push + price alerts
One VAPID pair is used for all PWAs. A PWA registers its browser subscription once, then the backend can send system push notifications even when the PWA is not foregrounded.

Price alerts are data-plane infrastructure:
- `POST /api/v1/alerts/price`
- alert is evaluated directly against incoming ticks
- push + WebSocket event on trigger
- one-shot or repeat behaviour

## WebSocket protocol

Connect:

`wss://YOUR-BACKEND/ws`

Subscribe:

```json
{
  "action":"subscribe",
  "instruments":[
    {"exchangeSegment":"IDX_I","securityId":"13","mode":"quote"},
    {"exchangeSegment":"NSE_FNO","securityId":"12345","mode":"full"}
  ]
}
```

Modes:
- `ticker` = LTP/LTT
- `quote` = trade/volume/OHLC/buy-sell quantities
- `full` = quote + OI + 5-level depth

Snapshot:

```json
{"action":"snapshot","exchangeSegment":"IDX_I","securityId":"13"}
```

Server events:
- `hello`
- `status`
- `tick`
- `snapshot`
- `alert`
- `market_status`
- `error`

## REST endpoints

- `GET /health`
- `GET /api/v1/status`
- `GET /api/v1/instruments/search?q=NIFTY`
- `GET /api/v1/instruments/:exchangeSegment/:securityId`
- `GET /api/v1/snapshot?exchangeSegment=IDX_I&securityId=13`
- `GET /api/v1/ticks?exchangeSegment=IDX_I&securityId=13&limit=500`
- `GET /api/v1/candles?exchangeSegment=IDX_I&securityId=13&timeframe=1&limit=500`
- `POST /api/v1/ltp`
- `POST /api/v1/quote`
- `POST /api/v1/depth`
- `POST /api/v1/history`
- `POST /api/v1/option-chain`
- `POST /api/v1/option-chain/expiry-list`
- `GET /api/v1/vapid-public-key`
- `POST /api/v1/push/subscribe`
- `POST /api/v1/push/test`
- `POST /api/v1/alerts/price`
- `GET /api/v1/alerts/price`
- `DELETE /api/v1/alerts/price/:id`

## Render deployment

1. Create a new GitHub repo named `dhan-ultimate-backend`.
2. Upload the files in this folder to the **repo root**.
3. Create a Render Web Service from that repo.
4. Build command: `npm install`.
5. Start command: `npm start`.
6. Add the Dhan credentials as Render environment variables.
7. Add the VAPID public/private pair and VAPID subject.
8. Deploy.
9. Open `/health`.

Expected health fields:
- `feedState: CONNECTED`
- `dhanConfigured: true`
- `instruments > 0`
- `feedLastMessageAt` updating during market hours.

## Security

- Dhan credentials remain server-side.
- VAPID **private** key remains server-side.
- PWA receives only the VAPID public key.
- Optional `CLIENT_API_KEY` can protect `/api/*`.
- This backend intentionally contains no order-placement endpoint.

## Important data limitation

Do not subscribe the backend to every instrument and every depth mode merely because the broker offers them. The backend should ingest the instruments actually needed by connected PWAs and multiplex duplicate subscriptions. Dhan's documented limits differ by feed/depth mode; 200-level depth is a separate, resource-heavier WebSocket service and is not silently mixed into the normal Full feed. citeturn0search2

## Indicator architecture

Examples:

- RSI reads the candle close series from this backend and calculates RSI locally in the PWA.
- Volume indicator reads candle/tick volume from this backend.
- OI Profile reads option-chain/OI/depth data from this backend.
- DEMA/HMA/SMA/EMA/MACD/ATR calculate locally from the same candle stream.
- Price alert is evaluated in the backend because it must continue while the PWA is backgrounded/closed.

This means hundreds of indicator implementations can share the same data stream without hundreds of broker connections.
