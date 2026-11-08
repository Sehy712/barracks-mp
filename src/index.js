// 곤뇽 멀티플레이 중계 서버 (Cloudflare Worker + Durable Object)
// 모든 접속자를 한 병영(방)에 모아, 각자의 위치·자세를 다른 사람에게 전달한다.
import { DurableObject } from 'cloudflare:workers';

const ORIGINS = ['https://online-barracks.sutong.workers.dev', 'https://jeojat.altolab.ai'];
const MAX_PLAYERS = 50;
const SALUTE_RANGE = 6;              // 경례는 이 거리(m) 안에서만 가능
const SALUTE_COOL = 5 * 60 * 1000;   // 같은 사람에게는 5분에 한 번만 경례 가능 (보내는 쪽 → 받는 쪽, 방향별)
const CHAT_MAX = 60;                  // 채팅 한 줄 최대 글자 수
const CHAT_KEEP = 30;                 // 새로 들어온 사람에게 보여 줄 최근 채팅 수
const CHAT_TTL = 6 * 3600 * 1000;     // 6시간 지난 채팅은 보여 주지 않음
const DUTY_MAX = 8 * 60 * 1000;       // 외곽 근무 자리는 최대 8분 (이동 + 3분 근무 + 복귀), 지나면 자동 해제

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

// 욕설 필터: 글자 사이에 띄어쓰기·숫자·기호를 끼워도 잡아서 *로 가림 (일상어와 겹치는 '보지 마', '자지 마', '호로록' 등은 제외)
const BAD_WORDS=['씨발','시발','씨빨','씨팔','시팔','씨바','씌발','쓰발','썅','쌍년','쌍놈','병신','븅신','빙신','등신','좆','존나','존내','졸라','지랄','염병','개새끼','개새기','개색기','개색끼','개쉐','새끼','쌔끼','새키','미친놈','미친년','미친새','개년','개놈','개같','엠창','니미','느금','니애미','니애비','애미뒤','호로새','호로자식','후장','창녀','꺼져','닥쳐','닥치','엿먹','ㅅㅂ','ㅆㅂ','ㅂㅅ','ㅄ','ㅈㄹ','ㅈㄴ','ㅗㅗ','fuck','fuk','fxxk','shit','bitch','asshole','motherfucker'];
const BAD_RE=new RegExp(BAD_WORDS.map(function(w){ return w.split('').map(function(c){ return c.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'); }).join('[\\s0-9._~!@#$%^&*+=|/,;:\'"`-]*'); }).join('|'),'gi');
function cleanBad(t){ return String(t).replace(BAD_RE,function(m){ return new Array(m.length+1).join('*'); }); }

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
      me.x = num(m.x, -150, 150); me.z = num(m.z, -150, 150); /* 외곽 초소(약 z 86)까지 위치 전달 */ me.f = num(m.f, -7, 7); me.p = '';
      ws.serializeAttachment(me);
      const shared = {}; const now = Date.now();
      for (const [k, v] of await this.ctx.storage.list({ prefix: 'sh:' })) {
        if (k !== 'sh:gate' && v < now) { await this.ctx.storage.delete(k); continue; }   // 정리 효과가 끝난 침상은 지움
        shared[k.slice(3)] = v;
      }
      const chat = ((await this.ctx.storage.get('chatlog')) || []).filter(c => now - c.at < CHAT_TTL);
      const peers = this.peers().filter(p => p.id !== me.id).map(p => ({ id: p.id, name: p.name, rank: p.rank, x: p.x, z: p.z, f: p.f, p: p.p }));
      const dt = await this.dutyNow();
      const duty = dt ? { id: dt.id, name: dt.name, rank: dt.rank, left: Math.ceil((dt.until - now) / 1000) } : null;
      ws.send(JSON.stringify({ t: 'welcome', id: me.id, peers, shared, chat, duty }));
      this.broadcast({ t: 'join', id: me.id, name: me.name, rank: me.rank, x: me.x, z: me.z, f: me.f, p: '' }, ws);
    } else if (m.t === 'pos' && me.joined) {
      me.x = num(m.x, -150, 150); me.z = num(m.z, -150, 150); /* 외곽 초소(약 z 86)까지 위치 전달 */ me.f = num(m.f, -7, 7);
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
    } else if (m.t === 'chat' && me.joined) {
      // 채팅: 제어문자 제거·길이 제한 · 1초에 1번, 20초에 8번까지
      let text = String(m.text || '').replace(/[\u0000-\u001f\u007f\u2028\u2029]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, CHAT_MAX);
      if (!text) return;
      text = cleanBad(text);
      const now = Date.now();
      me.cw = (me.cw || []).filter(t => now - t < 20000);
      if ((me.cw.length && now - me.cw[me.cw.length - 1] < 1000) || me.cw.length >= 8) { ws.send(JSON.stringify({ t: 'chat_no', why: 'fast' })); return; }
      me.cw.push(now); ws.serializeAttachment(me);
      const msg = { t: 'chat', id: me.id, name: me.name, rank: me.rank, text, at: now };
      const log = (await this.ctx.storage.get('chatlog')) || []; log.push(msg); while (log.length > CHAT_KEEP) log.shift();
      await this.ctx.storage.put('chatlog', log);
      this.broadcast(msg, ws);
    } else if (m.t === 'duty_req' && me.joined) {
      // 외곽 초소 근무: 한 번에 한 명만. 이미 다른 사람이 근무 중이면 거절
      const now = Date.now(), cur = await this.dutyNow();
      const mine = cur && (cur.id === me.id || (me.vid && cur.vid === me.vid));
      if (cur && !mine) { ws.send(JSON.stringify({ t: 'duty_no', id: cur.id, name: cur.name, rank: cur.rank, left: Math.ceil((cur.until - now) / 1000) })); return; }
      const d = { id: me.id, vid: me.vid || '', name: me.name, rank: me.rank, until: now + DUTY_MAX };
      await this.ctx.storage.put('duty', d);
      ws.send(JSON.stringify({ t: 'duty_ok' }));
      this.broadcast({ t: 'duty', id: d.id, name: d.name, rank: d.rank, left: Math.ceil(DUTY_MAX / 1000) }, ws);
    } else if (m.t === 'duty_end' && me.joined) {
      await this.dutyRelease(me);
    } else if (m.t === 'ping') {
      ws.send('{"t":"pong"}');
    }
  }

  async dutyNow() { const d = await this.ctx.storage.get('duty'); if (d && d.until > Date.now()) return d; if (d) await this.ctx.storage.delete('duty'); return null; }
  async dutyRelease(me) {
    const d = await this.ctx.storage.get('duty');
    if (d && me && d.id === me.id) { await this.ctx.storage.delete('duty'); this.broadcast({ t: 'duty', id: null }); }
  }
  leave(ws) { const me = ws.deserializeAttachment(); if (me && me.joined) this.broadcast({ t: 'leave', id: me.id }, ws); return me; }
  async webSocketClose(ws) { const me = this.leave(ws); await this.dutyRelease(me); try { ws.close(); } catch (e) {} }   // 근무 중 나가면 자리 해제
  async webSocketError(ws) { const me = this.leave(ws); await this.dutyRelease(me); }
}