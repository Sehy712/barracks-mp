// 곤뇽 멀티플레이 중계 서버 (Cloudflare Worker + Durable Object)
// 모든 접속자를 한 병영(방)에 모아, 각자의 위치·자세를 다른 사람에게 전달한다.
import { DurableObject } from 'cloudflare:workers';

const ORIGINS = ['https://online-barracks.sutong.workers.dev', 'https://jeojat.altolab.ai'];
const MAX_PLAYERS = 50;

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const origin = req.headers.get('Origin') || '';
    const cors = { 'Access-Control-Allow-Origin': ORIGINS.includes(origin) ? origin : ORIGINS[0], 'Vary': 'Origin', 'Cache-Control': 'no-store' };
    const room = env.ROOM.get(env.ROOM.idFromName('main'));
    if (url.pathname === '/ws') {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('websocket only', { status: 426 });
      if (origin && !ORIGINS.includes(origin)) return new Response('forbidden', { status: 403 });
      return room.fetch(req);
    }
    if (url.pathname === '/count') {
      const r = await room.fetch(new Request('https://room/count'));
      return new Response(await r.text(), { headers: { ...cors, 'Content-Type': 'application/json' } });
    }
    return new Response('barracks-mp ok', { headers: cors });
  },
};

const clean = (s, n) => String(s || '').replace(/[<>&"'`\\]/g, '').trim().slice(0, n);
const num = (v, lo, hi) => { v = Number(v); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : 0; };

export class Barracks extends DurableObject {
  constructor(ctx, env) { super(ctx, env); this.seq = 0; }

  peers() { return this.ctx.getWebSockets().map(ws => ws.deserializeAttachment()).filter(a => a && a.joined); }

  broadcast(msg, except) {
    const s = JSON.stringify(msg);
    for (const ws of this.ctx.getWebSockets()) { if (ws === except) continue; try { ws.send(s); } catch (e) {} }
  }

  async fetch(req) {
    if (new URL(req.url).pathname === '/count') return new Response(JSON.stringify({ online: this.peers().length }));
    if (this.ctx.getWebSockets().length >= MAX_PLAYERS) return new Response('full', { status: 503 });
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    pair[1].serializeAttachment({ id: Date.now().toString(36) + (++this.seq).toString(36) + Math.random().toString(36).slice(2, 5), joined: false });
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  async webSocketMessage(ws, data) {
    if (typeof data !== 'string' || data.length > 400) return;
    let m; try { m = JSON.parse(data); } catch (e) { return; }
    const me = ws.deserializeAttachment(); if (!me) return;
    if (m.t === 'hello') {
      me.name = clean(m.name, 8) || '홍길동'; me.rank = num(m.rank, 0, 3) | 0; me.joined = true;
      me.x = num(m.x, -60, 60); me.z = num(m.z, -60, 60); me.f = num(m.f, -7, 7); me.p = '';
      ws.serializeAttachment(me);
      ws.send(JSON.stringify({ t: 'welcome', id: me.id, peers: this.peers().filter(p => p.id !== me.id) }));
      this.broadcast({ t: 'join', id: me.id, name: me.name, rank: me.rank, x: me.x, z: me.z, f: me.f, p: '' }, ws);
    } else if (m.t === 'pos' && me.joined) {
      me.x = num(m.x, -60, 60); me.z = num(m.z, -60, 60); me.f = num(m.f, -7, 7);
      me.p = m.p === 'prone' || m.p === 'supine' ? m.p : '';
      ws.serializeAttachment(me);
      this.broadcast({ t: 'pos', id: me.id, x: me.x, z: me.z, f: me.f, p: me.p }, ws);
    } else if (m.t === 'rank' && me.joined) {
      me.rank = num(m.rank, 0, 3) | 0; ws.serializeAttachment(me);
      this.broadcast({ t: 'rank', id: me.id, rank: me.rank }, ws);
    } else if (m.t === 'ping') {
      ws.send('{"t":"pong"}');
    }
  }

  leave(ws) { const me = ws.deserializeAttachment(); if (me && me.joined) this.broadcast({ t: 'leave', id: me.id }, ws); }
  async webSocketClose(ws) { this.leave(ws); try { ws.close(); } catch (e) {} }
  async webSocketError(ws) { this.leave(ws); }
}
