// 곤뇽 멀티플레이 중계 서버 (Cloudflare Worker + Durable Object)
// 모든 접속자를 한 병영(방)에 모아, 각자의 위치·자세를 다른 사람에게 전달한다.
import { DurableObject } from 'cloudflare:workers';

const ORIGINS = ['https://online-barracks.sutong.workers.dev', 'https://jeojat.altolab.ai'];
const MAX_PLAYERS = 50;
const SALUTE_RANGE = 6;              // 경례는 이 거리(m) 안에서만 가능
const SALUTE_COOL = 5 * 60 * 1000;   // 같은 사람에게는 5분에 한 번만 경례 가능 (보내는 쪽 → 받는 쪽, 방향별)

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
      me.vid = clean(m.vid, 40);   // 브라우저 고유값 (재접속해도 같은 값 → 경례 쿨타임 우회 방지)
      me.x = num(m.x, -60, 60); me.z = num(m.z, -60, 60); me.f = num(m.f, -7, 7); me.p = '';
      ws.serializeAttachment(me);
      const shared = {}; const now = Date.now();
      for (const [k, v] of await this.ctx.storage.list({ prefix: 'sh:' })) {
        if (k !== 'sh:gate' && v < now) { await this.ctx.storage.delete(k); continue; }   // 정리 효과가 끝난 침상은 지움
        shared[k.slice(3)] = v;
      }
      ws.send(JSON.stringify({ t: 'welcome', id: me.id, peers: this.peers().filter(p => p.id !== me.id), shared }));
      this.broadcast({ t: 'join', id: me.id, name: me.name, rank: me.rank, x: me.x, z: me.z, f: me.f, p: '' }, ws);
    } else if (m.t === 'pos' && me.joined) {
      me.x = num(m.x, -60, 60); me.z = num(m.z, -60, 60); me.f = num(m.f, -7, 7);
      me.p = m.p === 'prone' || m.p === 'supine' ? m.p : '';
      ws.serializeAttachment(me);
      this.broadcast({ t: 'pos', id: me.id, x: me.x, z: me.z, f: me.f, p: me.p }, ws);
    } else if (m.t === 'rank' && me.joined) {
      me.rank = num(m.rank, 0, 3) | 0; ws.serializeAttachment(me);
      this.broadcast({ t: 'rank', id: me.id, rank: me.rank }, ws);
    } else if (m.t === 'set' && me.joined) {
      // 함께 쓰는 상태: 침상·관물대 정리(s번호.blanket|locker = 정리가 풀리는 시각), 정문(gate = 연 시각, 0이면 닫힘)
      const k = String(m.k || ''); const now = Date.now();
      if (!/^s\d{1,3}\.(blanket|locker)$/.test(k) && k !== 'gate') return;
      let v = num(m.v, 0, now + 86400e3);
      if (k !== 'gate') v = Math.min(v, now + 600e3);   // 정리는 최대 10분 유지
      await this.ctx.storage.put('sh:' + k, v);
      this.broadcast({ t: 'set', id: me.id, k, v }, ws);
    } else if (m.t === 'salute' && me.joined) {
      // 경례: 가까이 있는 전우에게만, 같은 상대에게는 5분에 한 번
      const to = String(m.to || '').slice(0, 40);
      const target = this.ctx.getWebSockets().find(w => { const a = w.deserializeAttachment(); return a && a.joined && a.id === to; });
      const ta = target && target.deserializeAttachment();
      if (!ta || ta.id === me.id) return;
      if (Math.hypot(ta.x - me.x, ta.z - me.z) > SALUTE_RANGE) { ws.send(JSON.stringify({ t: 'salute_no', to, why: 'far' })); return; }
      const key = 'sal:' + (me.vid || me.id) + '>' + (ta.vid || ta.id), now = Date.now();
      const last = await this.ctx.storage.get(key);
      if (last && now - last < SALUTE_COOL) { ws.send(JSON.stringify({ t: 'salute_no', to, why: 'cool', left: Math.ceil((SALUTE_COOL - (now - last)) / 1000) })); return; }
      await this.ctx.storage.put(key, now);
      ws.send(JSON.stringify({ t: 'salute_ok', to }));
      try { target.send(JSON.stringify({ t: 'salute', from: me.id })); } catch (e) {}
    } else if (m.t === 'ping') {
      ws.send('{"t":"pong"}');
    }
  }

  leave(ws) { const me = ws.deserializeAttachment(); if (me && me.joined) this.broadcast({ t: 'leave', id: me.id }, ws); }
  async webSocketClose(ws) { this.leave(ws); try { ws.close(); } catch (e) {} }
  async webSocketError(ws) { this.leave(ws); }
}
