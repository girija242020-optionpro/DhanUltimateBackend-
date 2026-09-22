'use strict';

/*
 Dhan Ultimate Backend
 Universal data-plane for multiple PWAs / terminals.

 Strategy logic does NOT live here. The backend ingests Dhan market data once,
 normalizes it, caches it, aggregates candles, and fans out only requested data
 to connected clients. Price alerts + Web Push are infrastructure services.
*/

const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');
const webpush = require('web-push');
const crypto = require('crypto');

const app = express();
app.use(cors({ origin: true, credentials: false }));
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 3000);
const DHAN_CLIENT_ID = String(process.env.DHAN_CLIENT_ID || '').trim();
const DHAN_ACCESS_TOKEN = String(process.env.DHAN_ACCESS_TOKEN || '').trim();
const DHAN_PIN = String(process.env.DHAN_PIN || '').trim();
const DHAN_TOTP_SECRET = String(process.env.DHAN_TOTP_SECRET || '').replace(/\s+/g, '').trim();
const CLIENT_API_KEY = String(process.env.CLIENT_API_KEY || '').trim();
const INSTRUMENT_URL = String(process.env.INSTRUMENT_URL || 'https://images.dhan.co/api-data/api-scrip-master.csv').trim();
const VAPID_PUBLIC_KEY = String(process.env.VAPID_PUBLIC_KEY || 'BOAI-gvWU9K1f_R6LDlxkATZjK5Q0ZA-HD2Ru7WfL3_WIMHU9lJFFvSrE5TrkD_F9QpizB31aonIMQN-wSITAQE').trim();
const VAPID_PRIVATE_KEY = String(process.env.VAPID_PRIVATE_KEY || 'U2t9JOb4M0Im7jgw-XFP-KoqCkgAd4Us6eMXIg14Rks').trim();
const VAPID_SUBJECT = String(process.env.VAPID_SUBJECT || 'mailto:alerts@example.com').trim();
const MAX_TICKS_PER_INSTRUMENT = Number(process.env.MAX_TICKS_PER_INSTRUMENT || 3000);
const MAX_CANDLES_PER_INSTRUMENT = Number(process.env.MAX_CANDLES_PER_INSTRUMENT || 2000);
const INSTRUMENT_REFRESH_MS = Number(process.env.INSTRUMENT_REFRESH_MS || 6 * 3600 * 1000);
const TICK_STALE_MS = Number(process.env.TICK_STALE_MS || 15000);

const SEGMENT_CODE = { IDX_I: 0, NSE_EQ: 1, NSE_FNO: 2, NSE_CURRENCY: 3, BSE_EQ: 4, MCX_COMM: 5, BSE_CURRENCY: 7, BSE_FNO: 8 };
const CODE_SEGMENT = Object.fromEntries(Object.entries(SEGMENT_CODE).map(([k,v]) => [v,k]));
const FEED = { TICKER: 15, QUOTE: 17, FULL: 21 };
const RESP = { INDEX: 1, TICKER: 2, QUOTE: 4, OI: 5, PREV_CLOSE: 6, STATUS: 7, FULL: 8, DISCONNECT: 50 };

function now(){ return Date.now(); }
function keyOf(seg, sid){ return `${seg}:${String(sid)}`; }
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
function safeNum(v){ const n = Number(v); return Number.isFinite(n) ? n : null; }
function cleanSymbol(v){ return String(v || '').trim(); }

let dhanToken = DHAN_ACCESS_TOKEN;
let dhanTokenAt = DHAN_ACCESS_TOKEN ? now() : 0;
let dhanLoginPromise = null;

async function getDhanToken(){
  if (dhanToken && (!DHAN_PIN || now() - dhanTokenAt < 20 * 3600 * 1000)) return dhanToken;
  if (!DHAN_CLIENT_ID || !DHAN_PIN || !DHAN_TOTP_SECRET) return dhanToken || null;
  if (dhanLoginPromise) return dhanLoginPromise;
  dhanLoginPromise = (async()=>{
    try {
      const { authenticator } = require('otplib');
      const totp = authenticator.generate(DHAN_TOTP_SECRET);
      const u = 'https://auth.dhan.co/app/generateAccessToken?dhanClientId=' + encodeURIComponent(DHAN_CLIENT_ID) + '&pin=' + encodeURIComponent(DHAN_PIN) + '&totp=' + encodeURIComponent(totp);
      const r = await fetch(u, { method:'POST' });
      const j = await r.json().catch(()=>({}));
      const t = j.accessToken || j.access_token || j.token || j.accesstoken || null;
      if (t) { dhanToken = t; dhanTokenAt = now(); console.log('Dhan access token refreshed.'); }
      else console.log('Dhan token refresh returned no access token.');
      return dhanToken || null;
    } catch(e){ console.log('Dhan token refresh failed:', e.message); return dhanToken || null; }
    finally { dhanLoginPromise = null; }
  })();
  return dhanLoginPromise;
}

async function dhanPost(path, body){
  const token = await getDhanToken();
  if (!token) throw new Error('Dhan access token is not configured');
  const r = await fetch('https://api.dhan.co/v2' + path, {
    method:'POST', headers:{'Content-Type':'application/json','Accept':'application/json','access-token':token,'client-id':DHAN_CLIENT_ID}, body:JSON.stringify(body)
  });
  const j = await r.json().catch(()=>({}));
  if (!r.ok || j.status === 'failure') throw new Error(j.remarks || j.message || `Dhan HTTP ${r.status}`);
  return j;
}

// ---------------- Instrument master ----------------
// IMPORTANT: Do NOT download/parse the detailed master at boot. The detailed
// CSV is large enough to create a high peak-memory footprint on small Render
// instances. We use Dhan's compact master and load it lazily only when an
// instrument lookup/search is actually requested. The live feed itself does
// not require the master to be resident in RAM.
let instruments = [];
let instrumentByKey = new Map();
let instrumentLoadedAt = 0;
let instrumentLoadPromise = null;

function parseCSVLine(line){
  const out=[]; let cur=''; let quoted=false;
  for(let i=0;i<line.length;i++){
    const c=line[i];
    if(c==='"'){
      if(quoted && line[i+1]==='"'){ cur+='"'; i++; }
      else quoted=!quoted;
    } else if(c===',' && !quoted){ out.push(cur); cur=''; }
    else cur+=c;
  }
  out.push(cur); return out;
}

function addCompactInstrument(headers, vals){
  const x={};
  for(let i=0;i<headers.length;i++) x[headers[i]]=vals[i] ?? '';
  const exchange=x.SEM_EXM_EXCH_ID || x.EXCH_ID || '';
  const segment=x.SEM_SEGMENT || x.SEGMENT || '';
  const securityId=String(x.SEM_SMST_SECURITY_ID || x.SECURITY_ID || x.SECURITYID || '').trim();
  if(!securityId || !exchange || !segment) return;
  const exchangeSegment=`${exchange}_${segment}`;
  const item={
    exchange,
    segment,
    exchangeSegment,
    securityId,
    symbol:x.SM_SYMBOL_NAME || x.SYMBOL_NAME || '',
    tradingSymbol:x.SEM_TRADING_SYMBOL || '',
    displayName:x.SEM_CUSTOM_SYMBOL || x.DISPLAY_NAME || '',
    instrument:x.SEM_INSTRUMENT_NAME || x.INSTRUMENT || '',
    instrumentType:x.SEM_EXCH_INSTRUMENT_TYPE || x.INSTRUMENT_TYPE || '',
    underlyingSecurityId:x.UNDERLYING_SECURITY_ID || '',
    underlyingSymbol:x.UNDERLYING_SYMBOL || '',
    expiry:x.SEM_EXPIRY_DATE || x.SM_EXPIRY_DATE || '',
    strike:safeNum(x.SEM_STRIKE_PRICE || x.STRIKE_PRICE),
    optionType:x.SEM_OPTION_TYPE || x.OPTION_TYPE || '',
    lotSize:safeNum(x.SEM_LOT_UNITS || x.LOT_SIZE),
    tickSize:safeNum(x.SEM_TICK_SIZE || x.TICK_SIZE)
  };
  instruments.push(item);
  instrumentByKey.set(keyOf(exchangeSegment,securityId),item);
}

async function loadInstruments(force=false){
  if(!force && instruments.length && now()-instrumentLoadedAt<INSTRUMENT_REFRESH_MS) return instruments.length;
  if(instrumentLoadPromise) return instrumentLoadPromise;
  instrumentLoadPromise=(async()=>{
    try{
      const r=await fetch(INSTRUMENT_URL);
      if(!r.ok) throw new Error(`Instrument master HTTP ${r.status}`);
      if(!r.body) throw new Error('Instrument master response has no streaming body');

      // Release the previous index before rebuilding it. Never hold the full
      // CSV text and an array of row objects at the same time.
      instruments=[];
      instrumentByKey=new Map();
      const reader=r.body.getReader();
      const decoder=new TextDecoder();
      let carry='';
      let headers=null;
      for(;;){
        const {value,done}=await reader.read();
        if(done) break;
        carry += decoder.decode(value,{stream:true});
        const lines=carry.split(/\r?\n/);
        carry=lines.pop() || '';
        for(const line of lines){
          if(!line.trim()) continue;
          const vals=parseCSVLine(line);
          if(!headers){ headers=vals.map(x=>x.trim()); continue; }
          addCompactInstrument(headers,vals);
        }
      }
      carry += decoder.decode();
      if(carry.trim()){
        const vals=parseCSVLine(carry);
        if(!headers) headers=vals.map(x=>x.trim());
        else addCompactInstrument(headers,vals);
      }
      instrumentLoadedAt=now();
      console.log(`Instrument master loaded lazily: ${instruments.length}`);
      return instruments.length;
    } finally {
      instrumentLoadPromise=null;
    }
  })();
  return instrumentLoadPromise;
}

function lookupInstrument(segment, securityId){ return instrumentByKey.get(keyOf(segment,securityId)) || null; }

// ---------------- Market state ----------------
const state = new Map();
function getState(seg,sid){
  const k=keyOf(seg,sid);
  if(!state.has(k)) state.set(k,{ exchangeSegment:seg, securityId:String(sid), quote:{}, depth:[], ticks:[], candles:new Map(), lastUpdate:0, previousClose:null, oi:null });
  return state.get(k);
}

function appendTick(s,tick){
  s.ticks.push(tick);
  if(s.ticks.length>MAX_TICKS_PER_INSTRUMENT) s.ticks.splice(0,s.ticks.length-MAX_TICKS_PER_INSTRUMENT);
  s.lastUpdate=now();
}
function updateCandle(s, tick, minutes){
  const ms=minutes*60000; const bucket=Math.floor(tick.ts/ms)*ms;
  const map=s.candles;
  let c=map.get(bucket);
  const p=s.quote.ltp ?? tick.ltp;
  if(!c){ c={timestamp:bucket,open:p,high:p,low:p,close:p,volume:tick.volume ?? 0,oi:tick.oi ?? null,source:'live'}; map.set(bucket,c); }
  else { c.high=Math.max(c.high,p); c.low=Math.min(c.low,p); c.close=p; if(tick.volume!=null)c.volume=tick.volume; if(tick.oi!=null)c.oi=tick.oi; }
  while(map.size>MAX_CANDLES_PER_INSTRUMENT) map.delete(map.keys().next().value);
}
function updateDerivedCandles(s,tick){ [1,3,5,15,25,60].forEach(m=>updateCandle(s,tick,m)); }

// ---------------- Dhan WebSocket feed manager ----------------
let feedSocket=null;
let feedState='DISCONNECTED';
let feedLastMessageAt=0;
let feedReconnectTimer=null;
let feedReconnectMs=1000;
let desiredSubscriptions=new Map(); // key -> mode
const clients=new Set();
const pushSubscriptions=new Map();
const priceAlerts=new Map();

function modeRank(m){ return m==='full'?3:m==='quote'?2:1; }
function highestMode(a,b){ return modeRank(a)>=modeRank(b)?a:b; }
function modeRequestCode(mode){ return mode==='full'?FEED.FULL:mode==='quote'?FEED.QUOTE:FEED.TICKER; }
function sendDhanSubscription(mode, items){
  if(!feedSocket || feedSocket.readyState!==WebSocket.OPEN || !items.length) return;
  for(let i=0;i<items.length;i+=100){
    const batch=items.slice(i,i+100);
    feedSocket.send(JSON.stringify({RequestCode:modeRequestCode(mode),InstrumentCount:batch.length,InstrumentList:batch.map(x=>({ExchangeSegment:x.exchangeSegment,SecurityId:String(x.securityId)}))}));
  }
}
function rebuildDesired(){
  desiredSubscriptions.clear();
  for(const c of clients){ for(const [k,v] of c.subscriptions){ desiredSubscriptions.set(k, highestMode(desiredSubscriptions.get(k)||'ticker',v.mode)); } }
}
function connectFeed(){
  clearTimeout(feedReconnectTimer);
  if(!DHAN_CLIENT_ID) { feedState='NO_CLIENT_ID'; broadcast({type:'status',feedState}); return; }
  getDhanToken().then(token=>{
    if(!token){ feedState='NO_TOKEN'; broadcast({type:'status',feedState}); return; }
    feedState='CONNECTING'; broadcast({type:'status',feedState});
    const url='wss://api-feed.dhan.co?version=2&token='+encodeURIComponent(token)+'&clientId='+encodeURIComponent(DHAN_CLIENT_ID)+'&authType=2';
    feedSocket=new WebSocket(url);
    feedSocket.binaryType='arraybuffer';
    feedSocket.on('open',()=>{
      feedState='CONNECTED'; feedReconnectMs=1000; feedLastMessageAt=now(); broadcast({type:'status',feedState});
      rebuildDesired();
      const groups={ticker:[],quote:[],full:[]};
      for(const [k,mode] of desiredSubscriptions){ const [exchangeSegment,securityId]=k.split(':'); groups[mode].push({exchangeSegment,securityId}); }
      Object.entries(groups).forEach(([mode,list])=>sendDhanSubscription(mode,list));
      console.log('Dhan feed connected; subscriptions:', desiredSubscriptions.size);
    });
    feedSocket.on('message',buf=>{ feedLastMessageAt=now(); handleDhanPacket(Buffer.from(buf)); });
    feedSocket.on('close',()=>{ feedState='DISCONNECTED'; broadcast({type:'status',feedState}); scheduleReconnect(); });
    feedSocket.on('error',e=>console.log('Dhan feed error:',e.message));
  }).catch(e=>{ console.log('Feed connect error:',e.message); scheduleReconnect(); });
}
function scheduleReconnect(){ if(feedReconnectTimer) return; feedReconnectTimer=setTimeout(()=>{feedReconnectTimer=null; connectFeed();},feedReconnectMs); feedReconnectMs=Math.min(feedReconnectMs*2,30000); }

function readHeader(b){
  if(b.length<8) return null;
  return { responseCode:b.readUInt8(0), messageLength:b.readUInt16LE(1), exchangeCode:b.readUInt8(3), securityId:String(b.readUInt32LE(4)), exchangeSegment:CODE_SEGMENT[b.readUInt8(3)] || String(b.readUInt8(3)) };
}
function parseDepth5(b,offset){
  const d=[];
  for(let i=0;i<5;i++){
    const o=offset+i*20;
    if(o+20>b.length) break;
    d.push({bidQty:b.readInt32LE(o),askQty:b.readInt32LE(o+4),bidOrders:b.readInt16LE(o+8),askOrders:b.readInt16LE(o+10),bidPrice:b.readFloatLE(o+12),askPrice:b.readFloatLE(o+16)});
  }
  return d;
}
function normalizeTick(s,h,fields){
  const t={ts:fields.ltt?fields.ltt*1000:now(),receivedAt:now(),exchangeSegment:h.exchangeSegment,securityId:h.securityId,...fields};
  if(t.ts<100000000000) t.ts*=1000;
  s.quote={...s.quote,...fields,exchangeSegment:h.exchangeSegment,securityId:h.securityId};
  if(fields.oi!=null)s.oi=fields.oi;
  appendTick(s,t); updateDerivedCandles(s,t); evaluateAlerts(s,t); broadcastTick(h.exchangeSegment,h.securityId,t);
}
function handleDhanPacket(b){
  const h=readHeader(b); if(!h) return; const s=getState(h.exchangeSegment,h.securityId);
  try{
    switch(h.responseCode){
      case RESP.TICKER: normalizeTick(s,h,{ltp:b.readFloatLE(8),ltt:b.readUInt32LE(12)}); break;
      case RESP.QUOTE: normalizeTick(s,h,{ltp:b.readFloatLE(8),lastQty:b.readInt16LE(12),ltt:b.readUInt32LE(14),atp:b.readFloatLE(18),volume:b.readUInt32LE(22),sellQty:b.readUInt32LE(26),buyQty:b.readUInt32LE(30),open:b.readFloatLE(34),close:b.readFloatLE(38),high:b.readFloatLE(42),low:b.readFloatLE(46)}); break;
      case RESP.OI: { const oi=b.readUInt32LE(8); s.oi=oi; s.quote.oi=oi; broadcastTick(h.exchangeSegment,h.securityId,{ts:now(),oi,exchangeSegment:h.exchangeSegment,securityId:h.securityId}); break; }
      case RESP.PREV_CLOSE: s.previousClose=b.readFloatLE(8); s.quote.previousClose=s.previousClose; s.oiPrev=b.readUInt32LE(12); break;
      case RESP.FULL: {
        const q={ltp:b.readFloatLE(8),lastQty:b.readInt16LE(12),ltt:b.readUInt32LE(14),atp:b.readFloatLE(18),volume:b.readUInt32LE(22),sellQty:b.readUInt32LE(26),buyQty:b.readUInt32LE(30),oi:b.readUInt32LE(34),oiDayHigh:b.readUInt32LE(38),oiDayLow:b.readUInt32LE(42),open:b.readFloatLE(46),close:b.readFloatLE(50),high:b.readFloatLE(54),low:b.readFloatLE(58),depth:parseDepth5(b,62)};
        s.depth=q.depth; normalizeTick(s,h,q); break;
      }
      case RESP.INDEX: normalizeTick(s,h,{ltp:b.readFloatLE(8),ltt:b.readUInt32LE(12)}); break;
      case RESP.STATUS: broadcast({type:'market_status',exchangeSegment:h.exchangeSegment,securityId:h.securityId,code:h.responseCode}); break;
      case RESP.DISCONNECT: console.log('Dhan feed disconnect packet:', b.length>=10?b.readInt16LE(8):'unknown'); break;
    }
  }catch(e){ console.log('Packet decode error:',e.message); }
}

function broadcast(obj){ const raw=JSON.stringify(obj); for(const c of clients){ if(c.ws.readyState===WebSocket.OPEN) c.ws.send(raw); } }
function broadcastTick(seg,sid,tick){
  const msg={type:'tick',data:{exchangeSegment:seg,securityId:String(sid),instrument:lookupInstrument(seg,sid),tick}};
  const k=keyOf(seg,sid);
  const raw=JSON.stringify(msg);
  for(const c of clients){ const sub=c.subscriptions.get(k); if(sub && c.ws.readyState===WebSocket.OPEN) c.ws.send(raw); }
}

// ---------------- Price alerts / push ----------------
try { webpush.setVapidDetails(VAPID_SUBJECT,VAPID_PUBLIC_KEY,VAPID_PRIVATE_KEY); } catch(e){ console.log('VAPID configuration error:',e.message); }
async function broadcastPush(payload){
  const body=JSON.stringify(payload);
  for(const [id,sub] of [...pushSubscriptions]){
    try{ await webpush.sendNotification(sub,body); }
    catch(e){ if(e.statusCode===404||e.statusCode===410) pushSubscriptions.delete(id); else console.log('Push failed:',e.message); }
  }
}
function crossed(prev,cur,op,value){ if(prev==null||cur==null)return false; if(op==='>=' )return prev<value&&cur>=value; if(op==='<=')return prev>value&&cur<=value; if(op==='>')return prev<=value&&cur>value; if(op==='<')return prev>=value&&cur<value; if(op==='=')return prev!==value&&cur===value; return false; }
function evaluateAlerts(s,tick){
  const k=keyOf(s.exchangeSegment,s.securityId); const price=s.quote.ltp; if(price==null)return;
  for(const [id,a] of [...priceAlerts]){
    if(a.exchangeSegment!==s.exchangeSegment || String(a.securityId)!==String(s.securityId)) continue;
    if(a.triggered && a.once) continue;
    if(crossed(a.lastPrice,price,a.operator,a.value)){
      a.triggeredAt=now(); a.triggered=true;
      const payload={type:'PRICE_ALERT',alertId:id,title:a.title||'Price Alert',body:`${a.symbol||s.securityId}: ${price} ${a.operator} ${a.value}`,exchangeSegment:s.exchangeSegment,securityId:s.securityId,price,value:a.value,operator:a.operator};
      broadcast({type:'alert',data:payload}); broadcastPush(payload);
      if(a.once) priceAlerts.delete(id);
    }
    a.lastPrice=price;
  }
}

// ---------------- REST API ----------------
function auth(req,res,next){ if(!CLIENT_API_KEY)return next(); const k=req.get('x-client-key') || req.query.key || ''; if(k!==CLIENT_API_KEY)return res.status(401).json({success:false,error:'Unauthorized'}); next(); }
app.use('/api',auth);

app.get('/health',(req,res)=>res.json({success:true,service:'Dhan Ultimate Backend',version:'1.1.0',time:new Date().toISOString(),feedState,dhanConfigured:!!(DHAN_CLIENT_ID && (dhanToken||DHAN_PIN)),feedLastMessageAt:feedLastMessageAt||null,subscribedInstruments:desiredSubscriptions.size,connectedClients:clients.size,instruments:instruments.length}));
app.get('/api/v1/status',(req,res)=>res.json({success:true,feedState,feedLastMessageAt,stale:feedLastMessageAt?now()-feedLastMessageAt>TICK_STALE_MS:true,desiredSubscriptions:desiredSubscriptions.size,clients:clients.size,instruments:instruments.length}));
app.get('/api/v1/instruments/search',async(req,res)=>{ try{await loadInstruments(); const q=String(req.query.q||'').toUpperCase(); const seg=String(req.query.exchangeSegment||'').toUpperCase(); const limit=Math.min(Number(req.query.limit||50),200); const out=instruments.filter(x=>(!q || [x.symbol,x.tradingSymbol,x.displayName,x.underlyingSymbol,x.securityId].some(v=>String(v).toUpperCase().includes(q))) && (!seg||x.exchangeSegment===seg)).slice(0,limit); res.json({success:true,count:out.length,data:out}); }catch(e){res.status(500).json({success:false,error:e.message});} });
app.get('/api/v1/instruments/:exchangeSegment/:securityId',async(req,res)=>{try{await loadInstruments(); const x=lookupInstrument(req.params.exchangeSegment,req.params.securityId); res.json({success:true,data:x});}catch(e){res.status(500).json({success:false,error:e.message});}});
app.get('/api/v1/snapshot',async(req,res)=>{const seg=String(req.query.exchangeSegment||''); const sid=String(req.query.securityId||''); if(!seg||!sid)return res.status(400).json({success:false,error:'exchangeSegment and securityId required'}); const s=getState(seg,sid); res.json({success:true,data:{instrument:lookupInstrument(seg,sid),quote:s.quote,depth:s.depth,previousClose:s.previousClose,lastUpdate:s.lastUpdate,stale:!s.lastUpdate||now()-s.lastUpdate>TICK_STALE_MS}});});
app.get('/api/v1/ticks',async(req,res)=>{const s=getState(String(req.query.exchangeSegment||''),String(req.query.securityId||'')); const limit=Math.min(Number(req.query.limit||500),MAX_TICKS_PER_INSTRUMENT); res.json({success:true,data:s.ticks.slice(-limit)});});
app.get('/api/v1/candles',async(req,res)=>{const seg=String(req.query.exchangeSegment||''),sid=String(req.query.securityId||''),tf=Number(req.query.timeframe||1); const s=getState(seg,sid); let arr=Array.from(s.candles.values()).filter(c=>Math.abs((c.timestamp%(tf*60000)))<1); if(!arr.length && [1,3,5,15,25,60].includes(tf)) arr=Array.from(s.candles.values()).filter(c=>Math.floor(c.timestamp/(tf*60000))===Math.floor(c.timestamp/(tf*60000))); res.json({success:true,timeframe:tf,data:arr.slice(-Math.min(Number(req.query.limit||500),MAX_CANDLES_PER_INSTRUMENT))});});
app.post('/api/v1/history',async(req,res)=>{try{const j=await dhanPost('/charts/intraday',req.body);res.json(j);}catch(e){res.status(502).json({success:false,error:e.message});}});
app.post('/api/v1/quote',async(req,res)=>{try{const j=await dhanPost('/marketfeed/quote',req.body);res.json(j);}catch(e){res.status(502).json({success:false,error:e.message});}});
app.post('/api/v1/ltp',async(req,res)=>{try{const j=await dhanPost('/marketfeed/ltp',req.body);res.json(j);}catch(e){res.status(502).json({success:false,error:e.message});}});
app.post('/api/v1/option-chain',async(req,res)=>{try{const j=await dhanPost('/optionchain',req.body);res.json(j);}catch(e){res.status(502).json({success:false,error:e.message});}});
app.post('/api/v1/option-chain/expiry-list',async(req,res)=>{try{const j=await dhanPost('/optionchain/expirylist',req.body);res.json(j);}catch(e){res.status(502).json({success:false,error:e.message});}});
app.post('/api/v1/depth',async(req,res)=>{try{const j=await dhanPost('/marketfeed/quote',req.body);res.json(j);}catch(e){res.status(502).json({success:false,error:e.message});}});
app.get('/api/v1/vapid-public-key',(req,res)=>res.json({success:true,publicKey:VAPID_PUBLIC_KEY}));
app.post('/api/v1/push/subscribe',(req,res)=>{const {subscription,clientId='default'}=req.body||{}; if(!subscription||!subscription.endpoint)return res.status(400).json({success:false,error:'subscription required'}); pushSubscriptions.set(crypto.createHash('sha256').update(subscription.endpoint).digest('hex'),{...subscription,clientId}); res.json({success:true,subscribers:pushSubscriptions.size});});
app.post('/api/v1/push/test',async(req,res)=>{await broadcastPush({title:'Dhan Ultimate Backend',body:'Test alert — push pipeline is working.',type:'TEST'});res.json({success:true});});
app.post('/api/v1/alerts/price',(req,res)=>{const b=req.body||{}; if(!b.exchangeSegment||!b.securityId||b.value==null||!['>','>=','<','<=','='].includes(b.operator))return res.status(400).json({success:false,error:'exchangeSegment, securityId, operator and value are required'}); const id=b.id||crypto.randomUUID(); priceAlerts.set(id,{id,exchangeSegment:b.exchangeSegment,securityId:String(b.securityId),operator:b.operator,value:Number(b.value),title:b.title||'Price Alert',symbol:b.symbol||String(b.securityId),once:b.once!==false,lastPrice:getState(b.exchangeSegment,String(b.securityId)).quote.ltp??null,triggered:false,createdAt:now()}); ensureSubscription(b.exchangeSegment,String(b.securityId),'ticker'); res.json({success:true,id});});
app.delete('/api/v1/alerts/price/:id',(req,res)=>{res.json({success:true,deleted:priceAlerts.delete(req.params.id)});});
app.get('/api/v1/alerts/price',(req,res)=>res.json({success:true,data:[...priceAlerts.values()]}));

// Legacy-compatible health alias and minimal root metadata.
app.get('/',(req,res)=>res.json({success:true,name:'Dhan Ultimate Backend',version:'1.1.0',message:'Universal market-data plane. Strategies belong in PWAs.',docs:'/api/v1'}));

// ---------------- Client WebSocket ----------------
const server = app.listen(PORT,()=>console.log(`Dhan Ultimate Backend listening on :${PORT}`));
const wss = new WebSocket.Server({server,path:'/ws'});
function ensureSubscription(seg,sid,mode='ticker'){
  const k=keyOf(seg,sid); const before=desiredSubscriptions.get(k); const next=highestMode(before||'ticker',mode); desiredSubscriptions.set(k,next);
  if(feedSocket?.readyState===WebSocket.OPEN && (!before || modeRank(next)>modeRank(before))) sendDhanSubscription(next,[{exchangeSegment:seg,securityId:sid}]);
}
wss.on('connection',(ws,req)=>{
  const c={ws,subscriptions:new Map(),connectedAt:now()}; clients.add(c);
  ws.send(JSON.stringify({type:'hello',service:'Dhan Ultimate Backend',version:'1.1.0',feedState}));
  ws.on('message',raw=>{
    try{
      const m=JSON.parse(raw.toString());
      if(m.action==='subscribe'){
        const list=Array.isArray(m.instruments)?m.instruments:[];
        for(const x of list){if(!x.exchangeSegment||!x.securityId)continue; const mode=x.mode||'ticker'; c.subscriptions.set(keyOf(x.exchangeSegment,x.securityId),{mode}); ensureSubscription(x.exchangeSegment,String(x.securityId),mode);}
        ws.send(JSON.stringify({type:'subscribed',count:c.subscriptions.size}));
      } else if(m.action==='unsubscribe'){
        const list=Array.isArray(m.instruments)?m.instruments:[]; for(const x of list)c.subscriptions.delete(keyOf(x.exchangeSegment,String(x.securityId))); rebuildDesired();
      } else if(m.action==='snapshot'){
        const s=getState(String(m.exchangeSegment),String(m.securityId)); ws.send(JSON.stringify({type:'snapshot',data:{instrument:lookupInstrument(s.exchangeSegment,s.securityId),quote:s.quote,depth:s.depth,previousClose:s.previousClose,lastUpdate:s.lastUpdate}}));
      } else if(m.action==='ping') ws.send(JSON.stringify({type:'pong',ts:now()}));
    }catch(e){ws.send(JSON.stringify({type:'error',error:e.message}));}
  });
  ws.on('close',()=>{clients.delete(c);rebuildDesired();});
});

setInterval(()=>{
  if(feedState==='CONNECTED' && feedLastMessageAt && now()-feedLastMessageAt>TICK_STALE_MS*2){ console.log('Dhan feed appears stale; reconnecting.'); try{feedSocket.close();}catch{} }
  if(feedState==='DISCONNECTED' || feedState==='NO_TOKEN') connectFeed();
},10000);

// Intentionally do not load the instrument master at boot. This keeps the
// universal feed service within small Render memory limits. It is loaded lazily
// by instrument-search/lookup requests using Dhan's compact CSV stream.
connectFeed();

process.on('SIGTERM',()=>{try{feedSocket?.close();}catch{}; server.close(()=>process.exit(0));});
