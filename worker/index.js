import { createHash } from 'node:crypto';
// Hippo Strike 4.9.1 — Cloudflare Worker + one SQLite Durable Object hub.
const MAPS=['aim_hippo','de_hippodust','hvh_arena','de_mirage','de_canals','de_foundry','de_borgo','de_depot','de_abbey'];
const BUILD=490,MAXC=200,MAX_PROFILES=5000;
const str=(v,n)=>String(v==null?'':v).replace(/[<>]/g,'').slice(0,n);
const json=(o,status=200)=>Response.json(o,{status,headers:{'Cache-Control':'no-store','Access-Control-Allow-Origin':'*'}});
export default {
 async fetch(request,env){const url=new URL(request.url);
  if(url.pathname==='/ws'||url.pathname==='/info'){
   if(!env.HIPPO_HUB)return json({error:'Durable Object binding HIPPO_HUB is missing. Deploy the entire repository with its configuration.'},503);
   try{return await env.HIPPO_HUB.get(env.HIPPO_HUB.idFromName('hippo-global-v1')).fetch(request);}catch(e){console.error('Hub request failed',e);return json({error:'Online service unavailable; retry shortly.'},503);}
  }
  if(url.pathname==='/health')return json({ok:true,game:'Hippo Strike',version:'4.9.1-cf',build:BUILD});
  return env.ASSETS.fetch(request);
 }
};
export class HippoHub {
 constructor(ctx,env){this.ctx=ctx;this.env=env;this.core=null;ctx.blockConcurrencyWhile(async()=>{this.core=createCore(ctx,env);});}
 fetch(request){return this.core.fetch(request);}
 alarm(){return this.core.alarm();}
 webSocketMessage(ws,message){this.core.message(ws,message);}
 webSocketClose(ws,code,reason,wasClean){this.core.close(ws);try{ws.close(code===1005?1000:code,reason);}catch{}}
 webSocketError(ws){this.core.close(ws);try{ws.close(1011,'Connection error');}catch{}}
}
function createCore(ctx,env){
 const sql=ctx.storage.sql;
 sql.exec('CREATE TABLE IF NOT EXISTS profiles (code TEXT PRIMARY KEY, payload TEXT NOT NULL)');
 sql.exec('CREATE TABLE IF NOT EXISTS world (key TEXT PRIMARY KEY, payload TEXT NOT NULL)');
 const db={users:{}},savedUsers=new Map();
 for(const row of sql.exec('SELECT code,payload FROM profiles')){try{db.users[row.code]=JSON.parse(row.payload);savedUsers.set(row.code,row.payload);}catch{}}
 function U(code){return db.users[code];}
 function save(){for(const[code,value]of Object.entries(db.users)){const payload=JSON.stringify(value);if(savedUsers.get(code)===payload)continue;sql.exec('INSERT INTO profiles(code,payload) VALUES (?,?) ON CONFLICT(code) DO UPDATE SET payload=excluded.payload',code,payload);savedUsers.set(code,payload);}}
 const clients=new Map(),online=new Map(),rooms=new Map(),bySocket=new Map();let nextId=1,scheduledAt=null,lastScore=0;
 const persisted=[...sql.exec("SELECT payload FROM world WHERE key='rooms'")][0];
 if(persisted)for(const r of JSON.parse(persisted.payload)){r.members=new Set();r.invited=new Set(r.invited||[]);rooms.set(r.id,r);}
 function socketAdapter(ws){return{get destroyed(){return ws.readyState!==1},destroy(){try{ws.close(1000,'Disconnected');}catch{}}};}
 for(const ws of ctx.getWebSockets()){const a=ws.deserializeAttachment();if(!a)continue;const c={...a,ws,sock:socketAdapter(ws),room:null};clients.set(c.id,c);bySocket.set(ws,c);nextId=Math.max(nextId,c.id+1);if(c.code)online.set(c.code,c);if(a.roomId&&rooms.has(a.roomId)){c.room=rooms.get(a.roomId);c.room.members.add(c);}}
 for(const[id,r]of rooms)if(!r.members.size)rooms.delete(id);
 function attach(c){try{c.ws.serializeAttachment({id:c.id,code:c.code,name:c.name,agent:c.agent,dfx:c.dfx,kills:c.kills,deaths:c.deaths,roomId:c.room?.id||null,authHash:c.authHash||null,connectedAt:c.connectedAt||Date.now()});}catch{}}
 function persistWorld(){const rows=[...rooms.values()].map(r=>({...r,members:undefined,invited:[...r.invited]}));sql.exec("INSERT INTO world(key,payload) VALUES('rooms',?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload",JSON.stringify(rows));for(const c of clients.values())attach(c);}
 function send(c,o){if(!c||c.sock.destroyed)return;try{c.ws.send(JSON.stringify(o));}catch{}}
 function rbcast(r,o,except){const data=JSON.stringify(o);for(const c of r.members)if(c!==except&&!c.sock.destroyed)try{c.ws.send(data);}catch{}}
function isFriend(a,b){const u=U(a);return !!u&&u.friends.includes(b);}
function roomPub(r){return{id:r.id,name:r.name,type:r.type,map:r.map,max:r.max,kills:r.kills,players:r.members.size,host:r.hostName,hostCode:r.host,hasPass:!!r.pass,timeLeft:Math.round(r.timeLeft),phase:r.phase,time:r.time,rotate:!!r.rotate};}
function friendsOf(c){const u=U(c.code);if(!u)return;const mk=code=>{const f=U(code)||{name:'?',agent:'Классика'};const o=online.get(code);const r=o&&o.room;
  return{code,name:f.name,agent:f.agent,online:!!o,room:r?{id:r.id,name:r.name,type:r.type,players:r.members.size,max:r.max,map:r.map,joinable:r.type!=='private'||r.invited.has(c.code)}:null};};
 send(c,{t:'friends',list:u.friends.map(mk).sort((a,b)=>(b.online-a.online)||a.name.localeCompare(b.name)),inc:u.inc.map(code=>({code,name:(U(code)||{}).name||'?'})),out:u.out.map(code=>({code,name:(U(code)||{}).name||'?'}))});}
function pushFriends(code){const c=online.get(code);if(c)friendsOf(c);const u=U(code);if(u)for(const f of u.friends){const o=online.get(f);if(o)friendsOf(o);}}
function roomsFor(c){const L=[];for(const r of rooms.values()){if(r.type==='public'||(r.type==='friends'&&[...r.members].some(m=>isFriend(c.code,m.code)))||r.invited.has(c.code)||r.members.has(c))L.push(roomPub(r));}send(c,{t:'rooms',list:L});}
function pushRooms(){for(const c of clients.values())if(c.code)roomsFor(c);for(const code of online.keys())pushFriends(code);}
// ---------- rooms ----------
function rid(){const A='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';let s;do{s='';for(let i=0;i<5;i++)s+=A[Math.floor(Math.random()*A.length)];}while(rooms.has(s));return s;}
function joinRoom(c,r){if(c.room)leaveRoom(c);c.room=r;c.kills=0;c.deaths=0;r.members.add(c);
 send(c,{t:'joined',id:c.id,map:r.map,timeLeft:Math.round(r.timeLeft),room:roomPub(r),players:[...r.members].filter(x=>x!==c).map(x=>({id:x.id,name:x.name,agent:x.agent,dfx:x.dfx,code:x.code,kills:x.kills,deaths:x.deaths}))});
 rbcast(r,{t:'join',id:c.id,name:c.name,agent:c.agent,dfx:c.dfx,code:c.code},c);rbcast(r,{t:'roominfo',room:roomPub(r)});console.log(`  ${c.name} -> room ${r.id} "${r.name}" (${r.members.size})`);pushRooms();}
function leaveRoom(c){const r=c.room;if(!r)return;c.room=null;r.members.delete(c);rbcast(r,{t:'leave',id:c.id});
 if(!r.members.size){rooms.delete(r.id);console.log(`  room ${r.id} closed`);}else{if(r.host===c.code){const n=[...r.members][0];r.host=n.code;r.hostName=n.name;rbcast(r,{t:'notify',msg:'Новый хост комнаты: '+n.name});}rbcast(r,{t:'roominfo',room:roomPub(r)});}pushRooms();}
function drop(c){if(!clients.has(c.id))return;clients.delete(c.id);try{c.sock.destroy();}catch(e){}leaveRoom(c);if(c.code&&online.get(c.code)===c){online.delete(c.code);if(U(c.code))U(c.code).seen=Date.now();save();pushFriends(c.code);}}
function scores(r){return[...r.members].map(c=>({id:c.id,name:c.name,k:c.kills,d:c.deaths}));}
function over(r){if(r.phase!=='live')return;r.phase='over';r.timeLeft=0;r.restartAt=Date.now()+8000;const list=scores(r).sort((a,b)=>b.k-a.k||a.d-b.d),w=list[0];rbcast(r,{t:'over',winner:w?{id:w.id,name:w.name}:null,list});persistWorld();}
// ---------- messages ----------
function onMsg(c,txt){let m;try{m=JSON.parse(txt);}catch(e){return;}if(!m||typeof m.t!=='string')return;
 if(m.t==='hello'){if(c.code)return;if(m.build!==490){send(c,{t:'incompatible',msg:'Нужен клиент сборки 4.9.0. Обнови игру.'});setTimeout(()=>c.sock.destroy(),100);return;}const tok=str(m.token,64);if(tok.length<16){c.sock.destroy();return;}
  c.code=createHash('sha1').update('hippo:'+tok).digest('hex').slice(0,6).toUpperCase();c.name=str(m.name,20)||'hippo';c.agent=str(m.agent,24);c.dfx=str(m.dfx,16);
  c.authHash=createHash('sha256').update(tok).digest('hex');const bound=U(c.code);if(bound?.authHash&&bound.authHash!==c.authHash){send(c,{t:'authfail',msg:'Конфликт идентификатора профиля'});c.sock.destroy();return;}
  const old=online.get(c.code);if(old&&old!==c){send(old,{t:'notify',msg:'Вход с другого устройства'});drop(old);}
  const u=U(c.code)||(db.users[c.code]={name:c.name,friends:[],inc:[],out:[],agent:c.agent});u.authHash=c.authHash;u.name=c.name;u.agent=c.agent;save();online.set(c.code,c);
  send(c,{t:'hi',code:c.code,name:c.name,build:490});friendsOf(c);roomsFor(c);pushFriends(c.code);console.log(`+ ${c.name} #${c.code} online (${online.size})`);return;}
 if(!c.code||!U(c.code))return;const me=U(c.code),r=c.room;
 switch(m.t){
  case'prof':c.name=str(m.name,20)||c.name;c.agent=str(m.agent,24);c.dfx=str(m.dfx,16);me.name=c.name;me.agent=c.agent;save();if(r)rbcast(r,{t:'prof',id:c.id,name:c.name,agent:c.agent,dfx:c.dfx},c);pushFriends(c.code);break;
  case'rooms':roomsFor(c);break;
  case'create':{const type=['public','friends','private'].includes(m.type)?m.type:'public';const R={id:rid(),name:str(m.name,32)||(c.name+' room'),type,pass:type==='private'?str(m.pass,24):'',map:MAPS.includes(m.map)?m.map:'aim_hippo',
    max:Math.max(2,Math.min(12,m.max|0||10)),kills:Math.max(5,Math.min(150,m.kills|0||30)),time:Math.max(120,Math.min(3600,(m.time|0||10)*60)),rotate:!!m.rotate,host:c.code,hostName:c.name,members:new Set(),invited:new Set(),phase:'live'};
   R.timeLeft=R.time;R.deadline=Date.now()+R.time*1000;rooms.set(R.id,R);console.log(`* room ${R.id} "${R.name}" ${type} by ${c.name}`);joinRoom(c,R);break;}
  case'join':{const R=rooms.get(str(m.id,8).toUpperCase());if(!R)return send(c,{t:'joinfail',err:'Комната не найдена'});if(R.members.has(c))return;
   if(R.members.size>=R.max)return send(c,{t:'joinfail',err:'Комната заполнена'});const inv=R.invited.has(c.code);
   if(R.type==='friends'&&!inv&&![...R.members].some(x=>isFriend(c.code,x.code)))return send(c,{t:'joinfail',err:'Комната только для друзей'});
   if(!inv&&R.pass&&str(m.pass,24)!==R.pass)return send(c,{t:'joinfail',err:'Неверный пароль'});
   joinRoom(c,R);break;}
  case'leave':leaveRoom(c);break;
  case'fadd':{const code=str(m.code,8).toUpperCase();const t=U(code);if(!t||code===c.code)return send(c,{t:'notify',msg:'Игрок с кодом #'+code+' не найден'});
   if(me.friends.includes(code))return send(c,{t:'notify',msg:'Вы уже друзья'});
   if(me.inc.includes(code)){link(c.code,code);break;}if(!me.out.includes(code)){me.out.push(code);t.inc.push(c.code);save();}
   send(c,{t:'notify',msg:'Заявка отправлена: '+t.name});const o=online.get(code);if(o)send(o,{t:'notify',msg:'Заявка в друзья от '+c.name+' (#'+c.code+')'});pushFriends(c.code);pushFriends(code);break;}
  case'facc':{const code=str(m.code,8);if(me.inc.includes(code))link(c.code,code);break;}
  case'fdec':{const code=str(m.code,8),t=U(code);me.inc=me.inc.filter(x=>x!==code);me.out=me.out.filter(x=>x!==code);if(t){t.inc=t.inc.filter(x=>x!==c.code);t.out=t.out.filter(x=>x!==c.code);}save();pushFriends(c.code);pushFriends(code);break;}
  case'frem':{const code=str(m.code,8),t=U(code);me.friends=me.friends.filter(x=>x!==code);if(t)t.friends=t.friends.filter(x=>x!==c.code);save();pushFriends(c.code);pushFriends(code);pushRooms();break;}
  case'invite':{const code=str(m.code,8),o=online.get(code);if(!r)return send(c,{t:'notify',msg:'Сначала зайди в комнату'});if(!o)return send(c,{t:'notify',msg:'Друг не в сети'});
   r.invited.add(code);send(o,{t:'invite',from:{code:c.code,name:c.name},room:{id:r.id,name:r.name,map:r.map}});send(c,{t:'notify',msg:'Приглашение отправлено: '+o.name});pushRooms();break;}
  // ----- in-room game relay -----
  case's':if(r&&Array.isArray(m.s)&&m.s.length<40)rbcast(r,{t:'s',id:c.id,s:m.s},c);break;
  case'buy':{if(!r||!['ssg','awp','scar','ak','m4','mp9','mp7','mp5','p90','ump','dgl','r8','usp','knife','he','smoke','flash','molotov','decoy','kevlar','helmet'].includes(m.item))break;const now=Date.now();if(now-(c.buyAt||0)>1000){c.buyAt=now;c.buyCount=0;}if((c.buyCount=(c.buyCount||0)+1)<=12)rbcast(r,{t:'buy',id:c.id,item:m.item},c);break;}
  case'shot':if(r)rbcast(r,{t:'shot',id:c.id,o:m.o,e:m.e,w:str(m.w,12)},c);break;
  case'nade':{if(!r||r.phase!=='live'||!['he','smoke','flash','molotov','decoy'].includes(m.k))break;const vec=(v,max)=>Array.isArray(v)&&v.length===3&&v.every(x=>typeof x==='number'&&Number.isFinite(x)&&Math.abs(x)<=max);if(!vec(m.p,10000)||!vec(m.v,2200))break;const now=Date.now();if(now-(c.nadeAt||0)<250)break;c.nadeAt=now;rbcast(r,{t:'nade',id:c.id,k:m.k,p:m.p,v:m.v},c);break;}
  case'hit':{if(!r||r.phase!=='live')break;const v=[...r.members].find(x=>x.id===m.to);if(v)send(v,{t:'hit',id:c.id,to:v.id,dmg:Math.max(0,Math.min(500,+m.dmg||0)),part:m.part==='head'?'head':'body',w:str(m.w,24),ap:+m.ap||.5});break;}
  case'die':{if(!r||r.phase!=='live')break;c.deaths++;const k=[...r.members].find(x=>x.id===m.by);if(k&&k!==c)k.kills++;rbcast(r,{t:'die',id:c.id,by:m.by,w:str(m.w,24),hs:!!m.hs});if(k&&k.kills>=r.kills)over(r);break;}
  case'chat':if(r)rbcast(r,{t:'chat',id:c.id,m:str(m.m,120)},c);break;
  case'ping':send(c,{t:'pong',ts:m.ts});break;}}
function link(a,b){const A=U(a),B=U(b);if(!A||!B)return;for(const[x,y]of[[A,b],[B,a]]){x.inc=x.inc.filter(z=>z!==y);x.out=x.out.filter(z=>z!==y);if(!x.friends.includes(y))x.friends.push(y);}save();
 const oa=online.get(a),ob=online.get(b);if(oa)send(oa,{t:'notify',msg:'Теперь вы друзья с '+B.name});if(ob)send(ob,{t:'notify',msg:'Теперь вы друзья с '+A.name});pushFriends(a);pushFriends(b);pushRooms();}

 function clock(force=false){const now=Date.now();if(!force&&now-lastScore<1000)return;lastScore=now;for(const r of rooms.values()){
  if(r.phase==='live'){r.timeLeft=Math.max(0,(r.deadline-now)/1000);if(r.timeLeft<=0)over(r);}
  else if(r.restartAt&&now>=r.restartAt){for(const c of r.members)c.kills=c.deaths=0;if(r.rotate)r.map=MAPS[(MAPS.indexOf(r.map)+1)%MAPS.length];r.phase='live';r.deadline=now+r.time*1000;r.timeLeft=r.time;r.restartAt=null;rbcast(r,{t:'restart',map:r.map,timeLeft:r.time});persistWorld();pushRooms();}
  rbcast(r,{t:'score',list:scores(r),timeLeft:Math.max(0,Math.round(r.timeLeft))});
 }schedule();}
 // No permanent interval: client traffic drives scores; one durable alarm tracks
 // only the next round deadline/restart, so idle sockets can hibernate.
 function schedule(){let next=Infinity;for(const r of rooms.values())next=Math.min(next,r.phase==='live'?r.deadline:r.restartAt||Infinity);const due=Number.isFinite(next)?Math.max(Date.now()+1,Math.ceil(next)):null;if(due===scheduledAt)return;scheduledAt=due;ctx.waitUntil(due===null?ctx.storage.deleteAlarm():ctx.storage.setAlarm(due));}
 const vec=(v,max)=>Array.isArray(v)&&v.length===3&&v.every(n=>typeof n==='number'&&Number.isFinite(n)&&Math.abs(n)<=max);
 const control=new Set(['hello','prof','create','join','leave','fadd','facc','fdec','frem','invite','die']);
 function allow(c,m){const now=Date.now();if(now-(c.rateAt||0)>=1000){c.rateAt=now;c.rate=0;}if((c.rate=(c.rate||0)+1)>120)return false;
  if(m.t==='s'){const s=m.s;return Array.isArray(s)&&s.length>=18&&s.length<40&&s.slice(0,13).every(n=>typeof n==='number'&&Number.isFinite(n)&&Math.abs(n)<100000)&&typeof s[13]==='string'&&s[13].length<20&&s.slice(14,17).every(n=>typeof n==='number'&&Number.isFinite(n)&&Math.abs(n)<100000)&&Array.isArray(s[17])&&s[17].length<=12&&s[17].every(n=>typeof n==='number'&&Number.isFinite(n)&&Math.abs(n)<10000);}
  if(m.t==='shot'&&m.w!=='knife')return vec(m.o,10000)&&vec(m.e,50000);
  if(m.t==='hit')return Number.isInteger(m.to)&&typeof m.dmg==='number'&&Number.isFinite(m.dmg)&&typeof m.ap==='number'&&Number.isFinite(m.ap);
  if(m.t==='chat'){if(now-(c.chatAt||0)<300)return false;c.chatAt=now;}
  if(m.t==='create'){if(now-(c.createAt||0)<1500){send(c,{t:'notify',msg:'Подожди секунду перед созданием новой комнаты'});return false;}c.createAt=now;}
  if(['fadd','facc','fdec','frem','invite','prof'].includes(m.t)){if(now-(c.socialAt||0)<150)return false;c.socialAt=now;}
  if(m.t==='hello'&&!c.code&&Object.keys(db.users).length>=MAX_PROFILES){const token=str(m.token,64),code=createHash('sha1').update('hippo:'+token).digest('hex').slice(0,6).toUpperCase();if(!U(code)){send(c,{t:'notify',msg:'Лимит профилей сервера исчерпан'});return false;}}
  return true;
 }
 schedule();
 return {
  alarm(){scheduledAt=null;clock(true);},
  fetch(request){const u=new URL(request.url);if(u.pathname==='/info'){return json({name:env.SERVER_NAME||'Hippo Strike · Cloudflare',online:online.size,rooms:[...rooms.values()].filter(r=>r.type==='public').length,v:4,build:BUILD,cloudflare:true});}
   if(u.pathname!=='/ws')return new Response('Not found',{status:404});
   if(request.headers.get('Upgrade')?.toLowerCase()!=='websocket')return new Response('WebSocket upgrade required',{status:426});
   if(clients.size>=MAXC)return new Response('Server full',{status:503});
   const pair=new WebSocketPair(),client=pair[0],ws=pair[1];ctx.acceptWebSocket(ws);
   const c={id:nextId++,ws,sock:socketAdapter(ws),code:null,name:'hippo',agent:'Классика',dfx:'fall',room:null,kills:0,deaths:0,connectedAt:Date.now()};clients.set(c.id,c);bySocket.set(ws,c);attach(c);
   return new Response(null,{status:101,webSocket:client});
  },
  message(ws,data){const c=bySocket.get(ws);if(!c||!clients.has(c.id))return;if(typeof data!=='string'||data.length>8192){try{ws.close(1009,'Message too large');}catch{}drop(c);bySocket.delete(ws);persistWorld();schedule();return;}
   let m;try{m=JSON.parse(data);}catch{return;}if(!m||typeof m.t!=='string'||!allow(c,m))return;
   try{clock();onMsg(c,data);if(control.has(m.t)){persistWorld();schedule();}}catch(e){console.error('Protocol error',e);send(c,{t:'notify',msg:'Ошибка обработки запроса'});}
  },
  close(ws){const c=bySocket.get(ws);if(!c)return;drop(c);bySocket.delete(ws);persistWorld();schedule();}
 };
}
