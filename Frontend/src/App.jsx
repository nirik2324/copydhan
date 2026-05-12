import { useState, useEffect, useRef, useCallback } from "react";

const IS_PROD  = window.location.hostname !== "localhost";
const API_BASE = IS_PROD ? "" : "http://localhost:8000";
const WS_PROTO = window.location.protocol === "https:" ? "wss:" : "ws:";
const WS_URL   = IS_PROD ? `${WS_PROTO}//${window.location.host}/ws` : "ws://localhost:8000/ws";

const BROKERS = {
  dhan:    { name:"Dhan",      logo:"DH", color:"#FF6B35" },
  zerodha: { name:"Zerodha",   logo:"ZR", color:"#387ED1" },
  angel:   { name:"Angel One", logo:"AO", color:"#E8282D" },
  upstox:  { name:"Upstox",    logo:"UP", color:"#6741D9" },
};

// Credential fields required per broker
const BROKER_CREDS = {
  dhan: [
    { key:"clientId",       label:"Client ID",      hint:"Numeric Dhan user ID" },
    { key:"accessToken",    label:"Access Token",    hint:"From web.dhan.co → DhanHQ APIs", secret:true },
    { key:"dhanSecurityId", label:"Security ID",     hint:"Scrip ID from Dhan instrument master (per instrument)" },
  ],
  zerodha: [
    { key:"clientId",    label:"Client ID",     hint:"e.g. ZT8821A" },
    { key:"apiKey",      label:"API Key",       hint:"From Kite Connect developer console" },
    { key:"accessToken", label:"Access Token",  hint:"Generated daily via Kite login flow", secret:true },
  ],
  angel: [
    { key:"clientId",        label:"Client Code",    hint:"Angel One client ID" },
    { key:"apiKey",          label:"API Key",         hint:"From Angel SmartAPI developer console" },
    { key:"angelJwtToken",   label:"JWT Token",       hint:"Generated via SmartAPI login", secret:true },
    { key:"angelClientCode", label:"Client Code (2)", hint:"Same as Client ID usually" },
  ],
  upstox: [
    { key:"clientId",    label:"Client ID",    hint:"Upstox user ID" },
    { key:"accessToken", label:"Access Token", hint:"From Upstox developer console (OAuth)", secret:true },
  ],
};

const LOT_SIZES = { NIFTY:50, BANKNIFTY:15, FINNIFTY:40, SENSEX:10, BANKEX:15 };

const EMPTY_FOLLOWER = {
  id:"", name:"", broker:"dhan", clientId:"", lots:1, active:true,
  tags:["NIFTY","BANKNIFTY"], billing:"free", subFee:0, commPct:0,
  accessToken:"", apiKey:"", angelJwtToken:"", angelClientCode:"",
  dhanSecurityId:"", angelSymbolToken:"", upstoxInstrumentKey:"",
  trades:0, pnl:0,
};

// ── Helpers ──────────────────────────────────────────────────────────────────
function istTime() { return new Date().toLocaleTimeString("en-IN",{timeZone:"Asia/Kolkata",hour12:false}); }
function isMarketOpen() {
  const d=new Date(new Date().toLocaleString("en-US",{timeZone:"Asia/Kolkata"}));
  const m=d.getHours()*60+d.getMinutes(),wd=d.getDay();
  return wd>=1&&wd<=5&&m>=555&&m<930;
}
function fmtINR(v) {
  const a=Math.abs(v);
  const s=a>=10000000?"₹"+(a/10000000).toFixed(2)+"Cr":a>=100000?"₹"+(a/100000).toFixed(2)+"L":"₹"+a.toLocaleString("en-IN");
  return (v<0?"-":"+")+s;
}
function fmtINRPlain(v){ return "₹"+Math.abs(v).toLocaleString("en-IN"); }
function uid(){ return "fl"+Date.now()+Math.floor(Math.random()*1000); }

// Demo generator
let demoId=1000;
function genDemo(){
  const indices=["NIFTY","BANKNIFTY","FINNIFTY","SENSEX","BANKEX"];
  const index=indices[Math.floor(Math.random()*indices.length)];
  const exchange=["SENSEX","BANKEX"].includes(index)?"BSE":"NSE";
  const optType=Math.random()>.5?"CE":"PE";
  const side=Math.random()>.5?"BUY":"SELL";
  const base={NIFTY:24200,BANKNIFTY:52000,FINNIFTY:23000,SENSEX:79000,BANKEX:58000}[index];
  const step=["SENSEX","BANKEX"].includes(index)?500:50;
  const strike=Math.round(base+(Math.floor(Math.random()*9)-4)*step);
  const lots=[1,1,2,3,5][Math.floor(Math.random()*5)];
  const lot=LOT_SIZES[index]||50;
  const status=["TRANSIT","PENDING","TRADED","TRADED","TRADED","TRADED"][Math.floor(Math.random()*6)];
  const expiries=["17-APR","24-APR","01-MAY","30-APR"];
  const price=parseFloat((Math.random()*300+25).toFixed(1));
  return {
    id:`DEMO-${++demoId}`,exchOrderNo:`140${Math.floor(Math.random()*9000000+1000000)}`,
    index,symbol:`${index}${strike}${optType}`,exchange,optType,side,
    strike,expiry:expiries[Math.floor(Math.random()*4)],lots,qty:lots*lot,
    price,tradedPrice:status==="TRADED"?price:0,status,lotSize:lot,ts:istTime(),
    fillPct:status==="TRADED"?100:status==="PENDING"?0:45,
  };
}

// ── Backend WS Hook ───────────────────────────────────────────────────────────
function useBackendWS({ enabled, onMessage }) {
  const wsRef=useRef(null);
  const retryRef=useRef(0);
  const timerRef=useRef(null);
  const mountedRef=useRef(true);
  const cbRef=useRef(onMessage);
  useEffect(()=>{cbRef.current=onMessage;});

  const connect=useCallback(()=>{
    if (!enabled||!mountedRef.current) return;
    if (wsRef.current&&wsRef.current.readyState<=1) return;
    const ws=new WebSocket(WS_URL);
    wsRef.current=ws;
    ws.onopen=()=>{ retryRef.current=0; };
    ws.onmessage=(evt)=>{ try{ cbRef.current(JSON.parse(evt.data)); }catch{} };
    ws.onerror=()=>{};
    ws.onclose=()=>{
      if (!mountedRef.current||!enabled) return;
      const d=Math.min(2000*Math.pow(2,retryRef.current),15000);
      retryRef.current++;
      timerRef.current=setTimeout(connect,d);
    };
  },[enabled]);

  useEffect(()=>{
    mountedRef.current=true;
    if (enabled) connect();
    return ()=>{
      mountedRef.current=false;
      clearTimeout(timerRef.current);
      if (wsRef.current){wsRef.current.onclose=null;wsRef.current.close();}
    };
  },[connect,enabled]);
}

// ── Styles ────────────────────────────────────────────────────────────────────
const CSS=`
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Outfit:wght@400;500;600;700;800;900&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#04070f;--s1:#080d1a;--s2:#0d1424;--s3:#111b2e;
  --b1:#1a2640;--b2:#22334f;--b3:#2a3f60;
  --text:#dde5f0;--muted:#4d6280;
  --dhan:#FF6B35;--green:#00e676;--red:#ff3d6b;--gold:#f59e0b;--blue:#38bdf8;
  --ff:'Outfit',sans-serif;--fm:'DM Mono',monospace;
}
body{background:var(--bg);color:var(--text);font-family:var(--ff);-webkit-font-smoothing:antialiased}
.app{min-height:100vh;background:var(--bg);background-image:radial-gradient(ellipse 60% 40% at 8% 5%,rgba(255,107,53,.07) 0,transparent 55%),radial-gradient(ellipse 50% 30% at 92% 95%,rgba(56,189,248,.04) 0,transparent 55%)}
.topbar{display:flex;align-items:center;justify-content:space-between;padding:0 26px;height:60px;background:rgba(8,13,26,.96);border-bottom:1px solid var(--b1);backdrop-filter:blur(16px);position:sticky;top:0;z-index:200}
.brand{display:flex;align-items:center;gap:10px}
.brand-mark{width:34px;height:34px;background:var(--dhan);border-radius:8px;display:grid;place-items:center;font-size:16px;font-weight:900;color:#000;box-shadow:0 0 22px rgba(255,107,53,.4)}
.brand-name{font-size:17px;font-weight:800}.brand-name span{color:var(--dhan)}
.brand-sub{font-size:9px;color:var(--muted);font-family:var(--fm);letter-spacing:1px;margin-top:1px}
.topnav{display:flex;gap:2px}
.tn{padding:6px 14px;border-radius:7px;border:none;background:transparent;color:var(--muted);font-family:var(--ff);font-size:13px;font-weight:600;cursor:pointer;transition:all .15s}
.tn:hover{color:var(--text);background:var(--s3)}.tn.act{color:var(--dhan);background:rgba(255,107,53,.1)}
.topright{display:flex;align-items:center;gap:10px}
.mkt{display:flex;align-items:center;gap:6px;padding:4px 11px;border-radius:20px;font-size:10px;font-weight:700;font-family:var(--fm)}
.mkt.open{background:rgba(0,230,118,.08);border:1px solid rgba(0,230,118,.2);color:var(--green)}
.mkt.closed{background:rgba(255,61,107,.08);border:1px solid rgba(255,61,107,.2);color:var(--red)}
.dot{width:6px;height:6px;border-radius:50%}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}.blink{animation:blink 1.4s infinite}
.ws-pill{display:flex;align-items:center;gap:6px;padding:4px 12px;border-radius:20px;font-size:10px;font-weight:700;font-family:var(--fm);border:1px solid}
.ws-connected{background:rgba(0,230,118,.1);border-color:rgba(0,230,118,.25);color:var(--green)}
.ws-connecting,.ws-authenticating,.ws-reconnecting{background:rgba(245,158,11,.1);border-color:rgba(245,158,11,.25);color:var(--gold)}
.ws-disconnected,.ws-error,.ws-auth_failed{background:rgba(255,61,107,.08);border-color:rgba(255,61,107,.2);color:var(--red)}
.ws-demo{background:rgba(56,189,248,.08);border-color:rgba(56,189,248,.2);color:var(--blue)}
.wrap{padding:22px 26px;max-width:1700px;margin:0 auto}
/* Connect */
.connect-outer{display:flex;gap:24px;justify-content:center;min-height:72vh;padding-top:40px;align-items:flex-start;flex-wrap:wrap}
.connect-panel{background:var(--s1);border:1px solid var(--b1);border-radius:14px;padding:30px;width:480px;max-width:95vw}
.how-panel{background:var(--s1);border:1px solid var(--b1);border-radius:14px;padding:28px;width:340px;max-width:95vw}
.connect-title{font-size:20px;font-weight:800;margin-bottom:4px}
.connect-sub{font-size:11px;color:var(--muted);margin-bottom:22px;font-family:var(--fm)}
.form-row{margin-bottom:14px}
.form-lbl{font-size:10px;font-weight:700;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px}
.form-inp{width:100%;background:var(--s2);border:1px solid var(--b2);color:var(--text);border-radius:8px;padding:10px 13px;font-family:var(--fm);font-size:13px;transition:border-color .2s}
.form-inp:focus{outline:none;border-color:var(--dhan)}.form-inp::placeholder{color:var(--muted)}
.hint{font-size:11px;color:var(--muted);margin-top:5px;line-height:1.5}
.hint a{color:var(--dhan);text-decoration:none}
.demo-div{display:flex;align-items:center;gap:12px;margin:18px 0;color:var(--muted);font-size:11px}
.demo-div::before,.demo-div::after{content:'';flex:1;height:1px;background:var(--b2)}
.step{display:flex;gap:12px;margin-bottom:14px;align-items:flex-start}
.step-num{width:24px;height:24px;border-radius:6px;background:var(--dhan);color:#000;font-size:11px;font-weight:800;display:grid;place-items:center;flex-shrink:0;margin-top:1px}
.step-title{font-size:13px;font-weight:700;margin-bottom:2px}.step-desc{font-size:11px;color:var(--muted);line-height:1.5}
.alert{padding:10px 14px;border-radius:8px;font-size:12px;margin-bottom:12px;line-height:1.55}
.alert-warn{background:rgba(245,158,11,.1);border:1px solid rgba(245,158,11,.2);color:var(--gold)}
.alert-err{background:rgba(255,61,107,.1);border:1px solid rgba(255,61,107,.2);color:var(--red)}
.alert-ok{background:rgba(0,230,118,.1);border:1px solid rgba(0,230,118,.2);color:var(--green)}
.alert-info{background:rgba(56,189,248,.08);border:1px solid rgba(56,189,248,.2);color:var(--blue)}
/* Stats */
.stat-row{display:grid;grid-template-columns:repeat(5,1fr);gap:13px;margin-bottom:19px}
.sc{background:var(--s1);border:1px solid var(--b1);border-radius:12px;padding:17px 19px;position:relative;overflow:hidden}
.sc-glow{position:absolute;top:0;left:0;right:0;height:2px;background:var(--c,var(--dhan))}
.sc-lbl{font-size:10px;font-weight:700;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:7px}
.sc-val{font-size:23px;font-weight:800;line-height:1;color:var(--c,var(--text));font-family:var(--fm)}
.sc-sub{font-size:11px;color:var(--muted);margin-top:4px}
.sc-sub.g{color:var(--green)}.sc-sub.r{color:var(--red)}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:17px;margin-bottom:17px}
.g3{display:grid;grid-template-columns:2.2fr 1fr;gap:17px;margin-bottom:17px}
.full{margin-bottom:17px}
.card{background:var(--s1);border:1px solid var(--b1);border-radius:12px;overflow:hidden}
.ch{display:flex;align-items:center;justify-content:space-between;padding:13px 19px;border-bottom:1px solid var(--b1)}
.ct{font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--muted);display:flex;align-items:center;gap:7px}
/* Feed */
.feed{max-height:420px;overflow-y:auto}.feed::-webkit-scrollbar{width:3px}.feed::-webkit-scrollbar-thumb{background:var(--b2);border-radius:2px}
.feed-hdr,.feed-row{display:grid;grid-template-columns:100px 110px 50px 130px 85px 55px 55px 75px 1fr;align-items:center;gap:5px;padding:9px 19px;font-size:11px;font-family:var(--fm)}
.feed-hdr{color:var(--muted);font-size:9px;letter-spacing:1px;background:rgba(0,0,0,.3);border-bottom:1px solid var(--b2);position:sticky;top:0;z-index:1}
.feed-row{border-bottom:1px solid rgba(26,38,64,.5);transition:background .15s}.feed-row:hover{background:var(--s2)}
@keyframes fadeUp{from{opacity:0;transform:translateY(-3px)}to{opacity:1;transform:translateY(0)}}
.feed-row.new{animation:fadeUp .3s ease}
.ce{color:var(--green);font-weight:700}.pe{color:var(--red);font-weight:700}
.buy{color:var(--green);font-weight:700}.sell{color:var(--red);font-weight:700}
.ense{color:var(--blue);font-size:9px;font-weight:700;background:rgba(56,189,248,.1);padding:1px 5px;border-radius:3px}
.ebse{color:var(--gold);font-size:9px;font-weight:700;background:rgba(245,158,11,.1);padding:1px 5px;border-radius:3px}
.fill-t{height:3px;background:var(--b2);border-radius:2px;overflow:hidden;width:46px}
.fill-b{height:100%;background:var(--green);border-radius:2px;transition:width .5s}
.sb{padding:2px 7px;border-radius:4px;font-size:9px;font-weight:800;letter-spacing:.5px;white-space:nowrap;display:inline-block}
.sb-traded{background:rgba(0,230,118,.12);color:var(--green);border:1px solid rgba(0,230,118,.25)}
.sb-pending{background:rgba(56,189,248,.1);color:var(--blue);border:1px solid rgba(56,189,248,.2)}
.sb-transit{background:rgba(245,158,11,.1);color:var(--gold);border:1px solid rgba(245,158,11,.2)}
.sb-cancelled,.sb-rejected,.sb-expired{background:rgba(77,98,128,.15);color:var(--muted);border:1px solid var(--b2)}
/* Copy log */
.log-wrap{max-height:320px;overflow-y:auto;padding:9px}.log-wrap::-webkit-scrollbar{width:3px}.log-wrap::-webkit-scrollbar-thumb{background:var(--b2);border-radius:2px}
.log-item{display:flex;align-items:flex-start;gap:8px;padding:7px 10px;border-radius:7px;background:var(--s2);margin-bottom:5px;border-left:3px solid var(--green);font-size:10px;font-family:var(--fm);animation:fadeUp .2s ease}
.log-item.err{border-color:var(--red)}.log-item.info{border-color:var(--blue)}
.log-t{color:var(--muted);flex-shrink:0;white-space:nowrap}.log-m{flex:1;color:var(--text);line-height:1.4}
.log-ok{color:var(--green);font-weight:700}.log-err{color:var(--red);font-weight:700}
/* Table */
.ftbl{width:100%;border-collapse:collapse}
.ftbl th{text-align:left;padding:9px 15px;font-size:9px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--muted);border-bottom:1px solid var(--b2);background:rgba(0,0,0,.25);white-space:nowrap}
.ftbl td{padding:11px 15px;font-size:12px;border-bottom:1px solid rgba(26,38,64,.5);vertical-align:middle}
.ftbl tr:hover td{background:var(--s2)}.ftbl tr:last-child td{border-bottom:none}
.broker-pill{display:inline-flex;align-items:center;gap:5px;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;font-family:var(--fm)}
.fn{font-weight:700;font-size:13px;margin-bottom:2px}.fid{font-size:10px;color:var(--muted);font-family:var(--fm)}
/* Controls */
.bt{display:flex;gap:2px;background:var(--s3);border-radius:7px;padding:3px}
.bb{padding:3px 9px;border-radius:5px;border:none;font-family:var(--ff);font-size:10px;font-weight:700;cursor:pointer;transition:all .15s;background:transparent;color:var(--muted);white-space:nowrap}
.fee-inp{background:var(--s3);border:1px solid var(--b2);color:var(--text);border-radius:5px;padding:3px 7px;font-family:var(--fm);font-size:11px;width:68px}
.lc{display:flex;align-items:center;gap:4px}
.lb{width:21px;height:21px;border-radius:4px;border:1px solid var(--b2);background:var(--s3);color:var(--text);font-size:14px;cursor:pointer;display:grid;place-items:center;font-family:var(--fm)}
.lb:hover{background:var(--b2)}.lv{font-size:13px;font-weight:700;font-family:var(--fm);min-width:18px;text-align:center}
.sw{width:39px;height:21px;border-radius:11px;background:var(--b2);border:none;cursor:pointer;position:relative;transition:background .2s;flex-shrink:0}
.sw.on{background:var(--green)}.sw::after{content:'';position:absolute;top:2px;left:2px;width:17px;height:17px;border-radius:9px;background:#fff;transition:transform .2s}.sw.on::after{transform:translateX(18px)}
.idx-tags{display:flex;gap:3px;flex-wrap:wrap}
.idx-tag{padding:2px 5px;border-radius:3px;font-size:9px;font-weight:700;font-family:var(--fm)}
.nse-t{background:rgba(56,189,248,.08);color:var(--blue);border:1px solid rgba(56,189,248,.2)}
.bse-t{background:rgba(245,158,11,.08);color:var(--gold);border:1px solid rgba(245,158,11,.2)}
.pp{color:var(--green);font-weight:700;font-family:var(--fm)}.pn{color:var(--red);font-weight:700;font-family:var(--fm)}.pm{color:var(--muted);font-family:var(--fm)}
/* Buttons */
.btn{padding:7px 15px;border-radius:7px;border:none;font-family:var(--ff);font-size:12px;font-weight:700;cursor:pointer;transition:all .15s}
.btn:disabled{opacity:.4;cursor:not-allowed}
.btn-pri{background:var(--dhan);color:#000}.btn-pri:hover:not(:disabled){background:#ff8450}
.btn-ghost{background:transparent;color:var(--muted);border:1px solid var(--b2)}.btn-ghost:hover{color:var(--text);border-color:var(--b3)}
.btn-danger{background:rgba(255,61,107,.1);color:var(--red);border:1px solid rgba(255,61,107,.2)}.btn-danger:hover{background:rgba(255,61,107,.2)}
.btn-sm{padding:4px 11px;font-size:11px}.btn-full{width:100%}
/* Settings */
.set-row{display:flex;align-items:center;justify-content:space-between;padding:12px 14px;background:var(--s2);border-radius:8px;border:1px solid var(--b1);gap:10px;margin-bottom:9px}
.sl{font-size:13px;font-weight:600}.sd{font-size:10px;color:var(--muted);margin-top:2px}
.sv{font-size:13px;font-weight:700;font-family:var(--fm);color:var(--dhan);min-width:48px;text-align:right}
input[type=range]{accent-color:var(--dhan);cursor:pointer;width:86px}
select.sel{background:var(--s3);border:1px solid var(--b2);color:var(--text);border-radius:6px;padding:5px 9px;font-family:var(--fm);font-size:11px}
/* Modal */
.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.8);z-index:300;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(5px);overflow-y:auto;padding:20px}
.modal{background:var(--s1);border:1px solid var(--b2);border-radius:14px;padding:26px;width:520px;max-width:95vw;margin:auto}
.modal-title{font-size:17px;font-weight:800;margin-bottom:4px}
.modal-sub{font-size:11px;color:var(--muted);margin-bottom:18px;font-family:var(--fm)}
.modal-actions{display:flex;gap:10px;justify-content:flex-end;margin-top:18px}
.cred-section{background:var(--s2);border:1px solid var(--b1);border-radius:9px;padding:14px;margin-top:14px}
.cred-title{font-size:10px;font-weight:700;letter-spacing:1.5px;color:var(--muted);text-transform:uppercase;margin-bottom:12px}
.tag-toggle{display:flex;gap:4px;flex-wrap:wrap;margin-top:6px}
.tag-btn{padding:3px 9px;border-radius:5px;border:1px solid var(--b2);background:transparent;color:var(--muted);font-size:10px;font-weight:700;cursor:pointer;font-family:var(--fm)}
.tag-btn.on.nse{background:rgba(56,189,248,.15);color:var(--blue);border-color:rgba(56,189,248,.3)}
.tag-btn.on.bse{background:rgba(245,158,11,.15);color:var(--gold);border-color:rgba(245,158,11,.3)}
.empty{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:38px;color:var(--muted);gap:8px;font-size:12px}
.empty-ico{font-size:26px;opacity:.25}
.ist{font-size:11px;color:var(--muted);font-family:var(--fm)}
/* Copy result badge */
.cr-ok{display:inline-flex;align-items:center;gap:3px;padding:1px 6px;border-radius:3px;font-size:9px;font-weight:700;background:rgba(0,230,118,.1);color:var(--green);border:1px solid rgba(0,230,118,.2)}
.cr-err{display:inline-flex;align-items:center;gap:3px;padding:1px 6px;border-radius:3px;font-size:9px;font-weight:700;background:rgba(255,61,107,.1);color:var(--red);border:1px solid rgba(255,61,107,.2)}
@media(max-width:1100px){.stat-row{grid-template-columns:repeat(3,1fr)}.g3{grid-template-columns:1fr}}
@media(max-width:700px){.stat-row{grid-template-columns:repeat(2,1fr)}.g2{grid-template-columns:1fr}.connect-outer{flex-direction:column}}
`;

const ALL_TAGS = ["NIFTY","BANKNIFTY","FINNIFTY","SENSEX","BANKEX"];
const NSE_TAGS = ["NIFTY","BANKNIFTY","FINNIFTY"];
const BSE_TAGS = ["SENSEX","BANKEX"];

// ═══════════════════════════════════════════════════════════
export default function App() {
  const [tab,setTab]=useState("connect");
  const [creds,setCreds]=useState({clientId:"",accessToken:""});
  const [wsEnabled,setWsEnabled]=useState(false);
  const [wsStatus,setWsStatus]=useState("DISCONNECTED");
  const [isDemo,setIsDemo]=useState(false);
  const [trades,setTrades]=useState([]);
  const [followers,setFollowers]=useState([]);
  const [copyLog,setCopyLog]=useState([]);
  const [signalCount,setSignalCount]=useState(0);
  const [copyCount,setCopyCount]=useState(0);
  const [settings,setSettings]=useState({max_lots:5,exchange_filter:"BOTH",index_filter:"ALL",order_type:"MARKET",stop_mirror:true,pause_on_dd:false,dd_pct:15});
  const [showModal,setShowModal]=useState(false);
  const [editFollower,setEditFollower]=useState(null); // null = new
  const [form,setForm]=useState({...EMPTY_FOLLOWER});
  const [saving,setSaving]=useState(false);
  const [connecting,setConnecting]=useState(false);
  const [istClock,setIstClock]=useState(istTime());
  const logRef=useRef([]);

  useEffect(()=>{
    const el=document.createElement("style");el.innerHTML=CSS;document.head.appendChild(el);
    return ()=>document.head.removeChild(el);
  },[]);
  useEffect(()=>{const iv=setInterval(()=>setIstClock(istTime()),1000);return ()=>clearInterval(iv);},[]);

  // Push to copy log
  const pushLog=useCallback((entry)=>{
    logRef.current=[entry,...logRef.current].slice(0,60);
    setCopyLog([...logRef.current]);
  },[]);

  // Handle all WS messages from backend
  const handleWsMsg=useCallback((msg)=>{
    if (msg.type==="status"||msg.type==="init") {
      setWsStatus(msg.status);
      if (msg.copy_count!==undefined) setCopyCount(msg.copy_count);
      return;
    }
    if (msg.type==="reconnecting") { setWsStatus("RECONNECTING"); return; }

    if (msg.type==="order_alert") {
      const t=msg.trade||{};
      setTrades(prev=>{
        const idx=prev.findIndex(x=>x.id===t.id);
        if (idx>=0){const u=[...prev];u[idx]={...u[idx],...t};return u;}
        return [{...t,_new:true},...prev].slice(0,80);
      });
      setSignalCount(c=>c+1);
      return;
    }

    if (msg.type==="copy_result") {
      const ok=msg.ok;
      const b=BROKERS[msg.broker]||{name:msg.broker,color:"#888"};
      pushLog({
        id:Date.now()+Math.random(), ts:msg.ts, ok,
        info: false,
        msg: ok
          ? `✓ ${msg.side} ${msg.trade_symbol} → ${b.name} [${msg.follower_id}]  #${msg.order_id}`
          : `✗ ${msg.trade_symbol} → ${b.name} [${msg.follower_id}]  ${msg.error}`,
      });
      if (ok) setCopyCount(c=>c+1);
      // Update follower P&L (estimated)
      if (ok) {
        setFollowers(prev=>prev.map(f=>f.id===msg.follower_id?{...f,trades:(f.trades||0)+1}:f));
      }
      return;
    }

    if (msg.type==="follower_added"||msg.type==="follower_updated"||msg.type==="follower_removed"||msg.type==="follower_toggled") {
      // Re-fetch followers from server
      fetchFollowers();
    }
  },[pushLog]);

  // Demo mode handler
  const handleDemoOrder=useCallback((trade)=>{
    setTrades(prev=>{
      const idx=prev.findIndex(x=>x.id===trade.id);
      if (idx>=0){const u=[...prev];u[idx]={...u[idx],...trade};return u;}
      return [{...trade,_new:true},...prev].slice(0,80);
    });
    setSignalCount(c=>c+1);
    if (trade.status!=="TRADED") return;
    const eligible=followers.filter(f=>f.active&&(!f.tags?.length||f.tags.includes(trade.index)));
    eligible.forEach((f,i)=>{
      setTimeout(()=>{
        const ok=Math.random()>0.08;
        const b=BROKERS[f.broker]||{name:f.broker};
        pushLog({id:Date.now()+i,ts:istTime(),ok,info:false,
          msg:ok?`✓ DEMO: ${trade.side} ${trade.symbol} → ${b.name} [${f.name}]`
            :`✗ DEMO: ${trade.symbol} → ${b.name} — Simulated margin error`});
        if (ok) { setCopyCount(c=>c+1); setFollowers(prev=>prev.map(ff=>ff.id===f.id?{...ff,trades:(ff.trades||0)+1}:ff)); }
      },i*200);
    });
  },[followers,pushLog]);

  useBackendWS({enabled:wsEnabled&&!isDemo, onMessage:handleWsMsg});

  useEffect(()=>{
    if (!isDemo) return;
    setWsStatus("DEMO");
    const iv=setInterval(()=>handleDemoOrder(genDemo()),2800+Math.random()*1400);
    return ()=>clearInterval(iv);
  },[isDemo,handleDemoOrder]);

  // API helpers
  async function api(path, method="GET", body=null){
    const opts={method,headers:{"Content-Type":"application/json"}};
    if (body) opts.body=JSON.stringify(body);
    const r=await fetch(API_BASE+path,opts);
    return r.json();
  }

  async function fetchFollowers(){
    try {
      const d=await api("/api/followers");
      if (d.ok) setFollowers(d.followers.map(f=>({...f,trades:f.trades||0,pnl:f.pnl||0})));
    } catch {}
  }

  async function connectReal(){
    if (!creds.clientId||!creds.accessToken) return;
    setConnecting(true);
    try {
      const d=await api("/api/connect","POST",{client_id:creds.clientId,access_token:creds.accessToken});
      if (d.ok) { setIsDemo(false); setWsEnabled(true); await fetchFollowers(); setTab("dashboard"); }
      else setWsStatus("ERROR");
    } catch { setWsStatus("ERROR"); }
    finally { setConnecting(false); }
  }

  async function doDisconnect(){
    setWsEnabled(false); setIsDemo(false);
    try { await api("/api/disconnect","POST"); } catch {}
    setWsStatus("DISCONNECTED"); setTab("connect");
  }

  async function toggleFollower(f){
    if (isDemo) {
      setFollowers(prev=>prev.map(ff=>ff.id===f.id?{...ff,active:!ff.active}:ff));
      return;
    }
    try { await api(`/api/followers/${f.id}/toggle`,"PATCH"); } catch {}
  }

  async function deleteFollower(f){
    if (!window.confirm(`Remove ${f.name}?`)) return;
    if (isDemo) { setFollowers(prev=>prev.filter(ff=>ff.id!==f.id)); return; }
    try { await api(`/api/followers/${f.id}`,"DELETE"); } catch {}
  }

  async function saveFollower(){
    if (!form.name) return;
    setSaving(true);
    const payload={
      id: editFollower||form.id||uid(),
      name:form.name, broker:form.broker,
      client_id:form.clientId, lots:parseInt(form.lots)||1,
      active:true, tags:form.tags||[],
      billing:form.billing, sub_fee:parseFloat(form.subFee)||0,
      comm_pct:parseFloat(form.commPct)||0,
      access_token:form.accessToken||"",
      api_key:form.apiKey||"",
      angel_jwt_token:form.angelJwtToken||"",
      angel_client_code:form.angelClientCode||"",
      dhan_security_id:form.dhanSecurityId||"",
      angel_symbol_token:form.angelSymbolToken||"",
      upstox_instrument_key:form.upstoxInstrumentKey||"",
    };
    try {
      if (isDemo) {
        const newF={...payload,id:payload.id,clientId:payload.client_id,trades:0,pnl:0,active:true};
        if (editFollower) setFollowers(prev=>prev.map(f=>f.id===editFollower?{...f,...newF}:f));
        else setFollowers(prev=>[...prev,newF]);
      } else {
        if (editFollower) await api(`/api/followers/${editFollower}`,"PUT",payload);
        else await api("/api/followers","POST",payload);
        await fetchFollowers();
      }
    } catch (e) { console.error(e); }
    setSaving(false);
    setShowModal(false);
    setEditFollower(null);
    setForm({...EMPTY_FOLLOWER});
  }

  function openAdd(){ setEditFollower(null); setForm({...EMPTY_FOLLOWER,id:uid()}); setShowModal(true); }
  function openEdit(f){
    setEditFollower(f.id);
    setForm({
      ...EMPTY_FOLLOWER,...f,
      clientId:f.clientId||f.client_id||"",
      subFee:f.subFee||f.sub_fee||0,
      commPct:f.commPct||f.comm_pct||0,
      accessToken:f.accessToken||f.access_token||"",
      apiKey:f.apiKey||f.api_key||"",
      angelJwtToken:f.angelJwtToken||f.angel_jwt_token||"",
      angelClientCode:f.angelClientCode||f.angel_client_code||"",
      dhanSecurityId:f.dhanSecurityId||f.dhan_security_id||"",
    });
    setShowModal(true);
  }

  function toggleTag(t){
    setForm(f=>{
      const tags=f.tags||[];
      return {...f,tags:tags.includes(t)?tags.filter(x=>x!==t):[...tags,t]};
    });
  }

  const WS_LABELS={CONNECTING:"Connecting…",AUTHENTICATING:"Authenticating…",CONNECTED:"Live · Dhan WS",RECONNECTING:"Reconnecting…",DISCONNECTED:"Disconnected",ERROR:"Error",AUTH_FAILED:"Auth Failed",MAX_CONN:"Max Connections",DEMO:"Demo Mode"};
  const wsLabel=WS_LABELS[wsStatus]||wsStatus;
  const wsClass=`ws-pill ws-${wsStatus.toLowerCase().replace(/_/g,"-")}`;
  const wsColor=wsStatus==="CONNECTED"?"var(--green)":wsStatus==="DEMO"?"var(--blue)":["CONNECTING","AUTHENTICATING","RECONNECTING"].includes(wsStatus)?"var(--gold)":"var(--red)";

  const activeFol=followers.filter(f=>f.active).length;
  const totalTrades=followers.reduce((a,f)=>a+(f.trades||0),0);
  const monthlyRev=followers.reduce((a,f)=>{
    if (f.billing==="subscription") return a+(f.subFee||f.sub_fee||0);
    return a;
  },0);
  const mktOpen=isMarketOpen();
  const sbMap={TRADED:"sb-traded",PENDING:"sb-pending",TRANSIT:"sb-transit",CANCELLED:"sb-cancelled",REJECTED:"sb-rejected",EXPIRED:"sb-expired"};
  const TABS=[{id:"dashboard",label:"Dashboard"},{id:"followers",label:`Followers (${followers.length})`},{id:"master",label:"Master"},{id:"settings",label:"Settings"}];
  const bStyle=(f,m)=>{
    if (f.billing!==m) return {};
    return m==="free"?{background:"rgba(77,98,128,.3)",color:"var(--muted)"}:m==="subscription"?{background:"rgba(245,158,11,.15)",color:"var(--gold)"}:{background:"rgba(16,185,129,.15)",color:"#10b981"};
  };

  return (
    <div className="app">
      {/* Topbar */}
      <div className="topbar">
        <div className="brand">
          <div className="brand-mark">C</div>
          <div><div className="brand-name">Copy<span>Dhan</span></div><div className="brand-sub">TRADE REPLICATOR v3 · INDIA</div></div>
        </div>
        <nav className="topnav">{TABS.map(t=><button key={t.id} className={`tn ${tab===t.id?"act":""}`} onClick={()=>setTab(t.id)}>{t.label}</button>)}</nav>
        <div className="topright">
          <span className="ist">{istClock} IST</span>
          <div className={`mkt ${mktOpen?"open":"closed"}`}><div className={`dot ${mktOpen?"blink":""}`} style={{background:mktOpen?"var(--green)":"var(--red)"}}/>{mktOpen?"MKT OPEN":"MKT CLOSED"}</div>
          <div className={wsClass}><div className={`dot ${["CONNECTED","DEMO"].includes(wsStatus)?"blink":""}`} style={{background:wsColor}}/>{wsLabel}</div>
        </div>
      </div>

      <div className="wrap">

        {/* ══ CONNECT ══ */}
        {tab==="connect"&&(
          <div className="connect-outer">
            <div className="connect-panel">
              <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:20}}>
                <div className="brand-mark" style={{width:44,height:44,fontSize:20}}>🔶</div>
                <div><div className="connect-title">Connect Dhan Master</div><div className="connect-sub">Live F&O orders via FastAPI proxy</div></div>
              </div>
              {wsStatus==="AUTH_FAILED"&&<div className="alert alert-err">⚠ Auth failed — check your Client ID and Access Token.</div>}
              {wsStatus==="ERROR"&&<div className="alert alert-err">⚠ Connection error. Is the Railway server running?</div>}
              <div className="form-row">
                <div className="form-lbl">Dhan Client ID</div>
                <input className="form-inp" placeholder="e.g. 1000012345" value={creds.clientId} onChange={e=>setCreds(p=>({...p,clientId:e.target.value}))}/>
              </div>
              <div className="form-row">
                <div className="form-lbl">Access Token (JWT)</div>
                <input className="form-inp" type="password" placeholder="Paste your access token…" value={creds.accessToken} onChange={e=>setCreds(p=>({...p,accessToken:e.target.value}))}/>
                <div className="hint">From <a href="https://dhanhq.co/docs/v2/authentication/" target="_blank" rel="noreferrer">DhanHQ APIs</a> · Renew every 24h</div>
              </div>
              <button className="btn btn-pri btn-full" disabled={!creds.clientId||!creds.accessToken||connecting} onClick={connectReal}>
                {connecting?"Connecting…":"⚡ Connect Live"}
              </button>
              <div className="demo-div">or try without credentials</div>
              <button className="btn btn-ghost btn-full" onClick={()=>{setIsDemo(true);setWsEnabled(false);setTab("dashboard");}}>🧪 Demo Mode</button>
            </div>
            <div className="how-panel">
              <div style={{fontSize:15,fontWeight:800,marginBottom:16}}>📋 What happens when connected</div>
              {[
                {n:1,title:"Master order arrives",desc:"You place a NIFTY/BANKNIFTY/FINNIFTY/SENSEX/BANKEX option trade on Dhan."},
                {n:2,title:"Server receives it",desc:"Railway backend gets the order via Dhan WebSocket instantly."},
                {n:3,title:"Filters applied",desc:"Exchange, index, lot cap filters checked per your settings."},
                {n:4,title:"Orders fired",desc:"Real orders placed on all active follower accounts via their broker APIs simultaneously."},
                {n:5,title:"Results shown live",desc:"Every copy success or failure appears in the Copy Log in real-time."},
              ].map(s=>(
                <div className="step" key={s.n}><div className="step-num">{s.n}</div><div><div className="step-title">{s.title}</div><div className="step-desc">{s.desc}</div></div></div>
              ))}
            </div>
          </div>
        )}

        {/* ══ DASHBOARD ══ */}
        {tab==="dashboard"&&(<>
          {isDemo&&<div className="alert alert-warn" style={{marginBottom:14}}>🧪 Demo Mode — simulated signals and copy results. <button className="btn btn-ghost btn-sm" style={{marginLeft:8}} onClick={()=>setTab("connect")}>Connect real account</button></div>}
          {wsStatus==="CONNECTED"&&<div className="alert alert-ok" style={{marginBottom:14}}>✓ Live · <strong>wss://api-order-update.dhan.co</strong> · Real orders firing on <strong>{activeFol}</strong> follower{activeFol!==1?"s":""}</div>}

          <div className="stat-row">
            {[
              {label:"Signals Received",val:signalCount,sub:isDemo?"Simulated":"Live from Dhan",c:"var(--dhan)"},
              {label:"Orders Copied",val:copyCount,sub:"Real orders placed",c:"var(--green)"},
              {label:"Active Followers",val:`${activeFol}/${followers.length}`,sub:"Auto-copy ON",c:"var(--blue)"},
              {label:"Total Trades",val:totalTrades,sub:"Across all accounts",c:"var(--gold)"},
              {label:"Monthly Revenue",val:fmtINRPlain(monthlyRev),sub:"Subscription fees",c:"var(--green)"},
            ].map(s=>(
              <div className="sc" key={s.label} style={{"--c":s.c}}><div className="sc-glow"/><div className="sc-lbl">{s.label}</div><div className="sc-val">{s.val}</div><div className="sc-sub">{s.sub}</div></div>
            ))}
          </div>

          <div className="g3">
            <div className="card">
              <div className="ch">
                <div className="ct"><div className="dot blink" style={{background:wsStatus==="CONNECTED"?"var(--green)":isDemo?"var(--blue)":"var(--muted)"}}/>
                  {wsStatus==="CONNECTED"?"Live Dhan Order Feed":isDemo?"Demo Feed":"Order Feed"}
                </div>
                <span style={{fontSize:10,color:"var(--muted)",fontFamily:"var(--fm)"}}>{trades.length} orders</span>
              </div>
              <div className="feed">
                <div className="feed-hdr"><span>ORDER NO</span><span>SYMBOL</span><span>EXCH</span><span>CONTRACT</span><span>EXPIRY</span><span>SIDE</span><span>LOTS</span><span>FILL</span><span>STATUS</span></div>
                {trades.length===0&&<div className="empty"><div className="empty-ico">📡</div>{wsStatus==="CONNECTED"?"Waiting for orders…":"Connect to see live orders"}</div>}
                {trades.slice(0,25).map(t=>(
                  <div className={`feed-row ${t._new?"new":""}`} key={t.id}>
                    <span style={{color:"var(--muted)",fontSize:9}}>{(t.exchOrderNo||t.id||"").toString().slice(-10)}</span>
                    <span style={{fontWeight:700,fontSize:11}}>{t.symbol||`${t.index}${t.strike}${t.optType}`}</span>
                    <span><span className={t.exchange==="NSE"?"ense":"ebse"}>{t.exchange}</span></span>
                    <span><span className={t.optType==="CE"?"ce":"pe"}>{t.strike}{t.optType}</span></span>
                    <span style={{color:"var(--muted)",fontSize:10}}>{t.expiry}</span>
                    <span className={t.side==="BUY"?"buy":"sell"}>{t.side}</span>
                    <span style={{fontFamily:"var(--fm)",fontWeight:700}}>{t.lots}L</span>
                    <div><div className="fill-t"><div className="fill-b" style={{width:(t.fillPct||0)+"%"}}/></div></div>
                    <span><span className={`sb ${sbMap[t.status]||"sb-pending"}`}>{t.status}</span></span>
                  </div>
                ))}
              </div>
            </div>

            <div className="card">
              <div className="ch">
                <div className="ct">🔁 Copy Log</div>
                <span style={{fontSize:10,color:"var(--muted)",fontFamily:"var(--fm)"}}>{copyLog.length} events</span>
              </div>
              <div className="log-wrap">
                {copyLog.length===0&&<div className="empty"><div className="empty-ico">🔁</div>Real orders fire on TRADED signals</div>}
                {copyLog.map(l=>(
                  <div className={`log-item ${l.ok?"":"err"}`} key={l.id}>
                    <span className="log-t">{l.ts}</span>
                    <span className="log-m">{l.msg}</span>
                    <span className={l.ok?"log-ok":"log-err"}>{l.ok?"✓":"✗"}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="card full">
            <div className="ch"><div className="ct">👥 Followers</div><button className="btn btn-pri btn-sm" onClick={()=>setTab("followers")}>Manage</button></div>
            {followers.length===0
              ?<div className="empty"><div className="empty-ico">👤</div>No followers yet. <button className="btn btn-ghost btn-sm" style={{marginTop:6}} onClick={()=>{setTab("followers");openAdd();}}>Add first follower</button></div>
              :<table className="ftbl">
                <thead><tr><th>FOLLOWER</th><th>BROKER</th><th>LOTS</th><th>INDICES</th><th>TRADES</th><th>BILLING</th><th>COPY</th></tr></thead>
                <tbody>{followers.map(f=>{const b=BROKERS[f.broker]||{name:f.broker,color:"#888",logo:"??"};return(
                  <tr key={f.id}>
                    <td><div className="fn">{f.name}</div><div className="fid">{f.clientId||f.client_id}</div></td>
                    <td><span className="broker-pill" style={{background:b.color+"18",color:b.color,border:`1px solid ${b.color}35`}}>{b.logo} {b.name}</span></td>
                    <td><span style={{fontFamily:"var(--fm)",fontWeight:700}}>{f.lots}L</span></td>
                    <td><div className="idx-tags">{(f.tags||[]).map(t=><span key={t} className={`idx-tag ${NSE_TAGS.includes(t)?"nse-t":"bse-t"}`}>{t}</span>)}</div></td>
                    <td style={{fontFamily:"var(--fm)"}}>{f.trades||0}</td>
                    <td><span style={{padding:"2px 8px",borderRadius:4,fontSize:10,fontWeight:700,background:f.billing==="free"?"rgba(77,98,128,.2)":f.billing==="subscription"?"rgba(245,158,11,.15)":"rgba(16,185,129,.15)",color:f.billing==="free"?"var(--muted)":f.billing==="subscription"?"var(--gold)":"#10b981"}}>{f.billing==="free"?"FREE":f.billing==="subscription"?`₹${f.subFee||f.sub_fee||0}/mo`:`${f.commPct||f.comm_pct||0}%`}</span></td>
                    <td><button className={`sw ${f.active?"on":""}`} onClick={()=>toggleFollower(f)}/></td>
                  </tr>
                );})}
                </tbody>
              </table>
            }
          </div>
        </>)}

        {/* ══ FOLLOWERS ══ */}
        {tab==="followers"&&(<>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:17}}>
            <div><div style={{fontSize:21,fontWeight:800}}>Follower Accounts</div><div style={{fontSize:12,color:"var(--muted)",marginTop:3}}>Add API credentials — real orders will be placed on their accounts</div></div>
            <button className="btn btn-pri" onClick={openAdd}>+ Add Follower</button>
          </div>
          {followers.length===0
            ?<div className="card full"><div className="empty"><div className="empty-ico">👤</div>No followers yet. Click "+ Add Follower" to add one.<br/><span style={{fontSize:11,marginTop:4}}>You'll enter their broker API credentials here.</span></div></div>
            :<div className="card full">
              <table className="ftbl">
                <thead><tr><th>FOLLOWER</th><th>CLIENT ID</th><th>LOTS</th><th>INDICES</th><th>BILLING</th><th>TRADES</th><th>COPY</th><th>ACTIONS</th></tr></thead>
                <tbody>{followers.map(f=>{const b=BROKERS[f.broker]||{name:f.broker,color:"#888",logo:"??"};return(
                  <tr key={f.id}>
                    <td>
                      <div className="fn">{f.name}</div>
                      <span className="broker-pill" style={{background:b.color+"18",color:b.color,border:`1px solid ${b.color}35`,marginTop:4}}>{b.logo} {b.name}</span>
                    </td>
                    <td><span style={{fontFamily:"var(--fm)",fontSize:11,color:"var(--muted)"}}>{f.clientId||f.client_id}</span></td>
                    <td>
                      <div className="lc">
                        <button className="lb" onClick={()=>{const lots=Math.max(1,(f.lots||1)-1);if(isDemo)setFollowers(p=>p.map(ff=>ff.id===f.id?{...ff,lots}:ff));else api(`/api/followers/${f.id}`,"PUT",{...f,client_id:f.clientId||f.client_id,lots});}}>−</button>
                        <span className="lv">{f.lots||1}</span>
                        <button className="lb" onClick={()=>{const lots=Math.min(10,(f.lots||1)+1);if(isDemo)setFollowers(p=>p.map(ff=>ff.id===f.id?{...ff,lots}:ff));else api(`/api/followers/${f.id}`,"PUT",{...f,client_id:f.clientId||f.client_id,lots});}}>+</button>
                      </div>
                    </td>
                    <td><div className="idx-tags">{(f.tags||[]).map(t=><span key={t} className={`idx-tag ${NSE_TAGS.includes(t)?"nse-t":"bse-t"}`}>{t}</span>)}</div></td>
                    <td>
                      <div className="bt">
                        {["free","subscription","commission"].map(m=><button key={m} className="bb" style={bStyle(f,m)} onClick={()=>{if(isDemo)setFollowers(p=>p.map(ff=>ff.id===f.id?{...ff,billing:m}:ff));}}>{m==="free"?"Free":m==="subscription"?"Sub":"Comm"}</button>)}
                      </div>
                    </td>
                    <td style={{fontFamily:"var(--fm)"}}>{f.trades||0}</td>
                    <td><button className={`sw ${f.active?"on":""}`} onClick={()=>toggleFollower(f)}/></td>
                    <td>
                      <div style={{display:"flex",gap:5}}>
                        <button className="btn btn-ghost btn-sm" onClick={()=>openEdit(f)}>Edit</button>
                        <button className="btn btn-danger btn-sm" onClick={()=>deleteFollower(f)}>✕</button>
                      </div>
                    </td>
                  </tr>
                );})}
                </tbody>
              </table>
            </div>
          }

          {/* Broker credential guide */}
          <div className="card full" style={{marginTop:17}}>
            <div className="ch"><div className="ct">🔑 How to get API keys per broker</div></div>
            <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:1}}>
              {Object.entries(BROKERS).map(([key,b])=>(
                <div key={key} style={{padding:"16px 18px",borderRight:"1px solid var(--b1)"}}>
                  <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:10}}>
                    <span className="broker-pill" style={{background:b.color+"18",color:b.color,border:`1px solid ${b.color}35`}}>{b.logo}</span>
                    <span style={{fontWeight:700,fontSize:13}}>{b.name}</span>
                  </div>
                  {key==="dhan"&&<div style={{fontSize:11,color:"var(--muted)",lineHeight:1.7}}>1. Login <a href="https://web.dhan.co" target="_blank" rel="noreferrer" style={{color:"var(--dhan)"}}>web.dhan.co</a><br/>2. My Profile → DhanHQ APIs<br/>3. Generate Access Token (daily)<br/>4. Whitelist your Railway server IP</div>}
                  {key==="zerodha"&&<div style={{fontSize:11,color:"var(--muted)",lineHeight:1.7}}>1. <a href="https://developers.kite.trade" target="_blank" rel="noreferrer" style={{color:"#387ED1"}}>developers.kite.trade</a><br/>2. Create app → get API Key<br/>3. Login via Kite OAuth to get Access Token daily<br/>4. ₹2000/month for API access</div>}
                  {key==="angel"&&<div style={{fontSize:11,color:"var(--muted)",lineHeight:1.7}}>1. <a href="https://smartapi.angelbroking.com" target="_blank" rel="noreferrer" style={{color:"#E8282D"}}>smartapi.angelbroking.com</a><br/>2. Register → Create App → API Key<br/>3. Login via SmartAPI to get JWT Token<br/>4. Free API access</div>}
                  {key==="upstox"&&<div style={{fontSize:11,color:"var(--muted)",lineHeight:1.7}}>1. <a href="https://developer.upstox.com" target="_blank" rel="noreferrer" style={{color:"#6741D9"}}>developer.upstox.com</a><br/>2. Create app → get Client ID & Secret<br/>3. OAuth flow to get Access Token daily<br/>4. Free API access</div>}
                </div>
              ))}
            </div>
          </div>
        </>)}

        {/* ══ MASTER ══ */}
        {tab==="master"&&(
          <div className="g2">
            <div className="card">
              <div className="ch"><div className="ct">🔶 Dhan Master Connection</div></div>
              <div style={{padding:18}}>
                <div style={{padding:"14px 16px",background:"var(--s2)",borderRadius:10,border:"1px solid var(--b1)",marginBottom:16}}>
                  <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:8}}>
                    <div className={`dot ${["CONNECTED","DEMO"].includes(wsStatus)?"blink":""}`} style={{width:10,height:10,borderRadius:"50%",background:wsColor}}/>
                    <span style={{fontFamily:"var(--fm)",fontSize:12,fontWeight:700,color:wsColor}}>{wsLabel}</span>
                  </div>
                  <div style={{fontFamily:"var(--fm)",fontSize:11,color:"var(--muted)"}}>WS: {WS_URL}</div>
                  <div style={{fontFamily:"var(--fm)",fontSize:11,color:"var(--muted)",marginTop:3}}>Dhan: wss://api-order-update.dhan.co</div>
                  <div style={{fontFamily:"var(--fm)",fontSize:11,color:"var(--muted)",marginTop:3}}>Client: {creds.clientId||(isDemo?"Demo":"—")}</div>
                </div>
                {[
                  {l:"Version",v:"CopyDhan v3"},{l:"Architecture",v:"Browser → Railway → Dhan WS"},
                  {l:"Order trigger",v:"TRADED status only"},{l:"Segments",v:"NSE_FNO (D) · BSE_FNO (F)"},
                  {l:"Instruments",v:"CE + PE options only"},{l:"Reconnect",v:"Exp. backoff 2s→30s"},
                  {l:"Signals received",v:signalCount},{l:"Orders copied",v:copyCount},
                ].map(r=>(
                  <div className="set-row" key={r.l} style={{marginBottom:8}}>
                    <div className="sl" style={{fontSize:12}}>{r.l}</div>
                    <div style={{fontFamily:"var(--fm)",fontSize:11,color:"var(--muted)",textAlign:"right"}}>{r.v}</div>
                  </div>
                ))}
                <div style={{display:"flex",gap:9,marginTop:8}}>
                  {(wsStatus==="CONNECTED"||isDemo)&&<button className="btn btn-ghost" style={{flex:1}} onClick={doDisconnect}>Disconnect</button>}
                  <button className="btn btn-ghost" onClick={()=>setTab("connect")}>Edit Credentials</button>
                </div>
              </div>
            </div>
            <div className="card">
              <div className="ch"><div className="ct">🔌 Broker API Status</div></div>
              <div style={{padding:14,display:"flex",flexDirection:"column",gap:9}}>
                {Object.entries(BROKERS).map(([key,b])=>{
                  const fols=followers.filter(f=>f.broker===key);
                  const active=fols.filter(f=>f.active).length;
                  return(
                    <div className="set-row" key={key} style={{marginBottom:0}}>
                      <div style={{display:"flex",alignItems:"center",gap:12}}>
                        <span className="broker-pill" style={{background:b.color+"18",color:b.color,border:`1px solid ${b.color}35`}}>{b.logo}</span>
                        <div>
                          <div className="sl">{b.name}</div>
                          <div className="sd">{key==="dhan"?"Master (signal source)":fols.length?`${active}/${fols.length} active followers`:"No followers"}</div>
                        </div>
                      </div>
                      <div style={{display:"flex",flexDirection:"column",alignItems:"flex-end",gap:3}}>
                        {key==="dhan"&&<span style={{fontSize:9,color:"var(--dhan)",fontWeight:800,fontFamily:"var(--fm)"}}>MASTER</span>}
                        <span style={{fontSize:10,fontWeight:700,padding:"2px 8px",borderRadius:4,background:fols.length?"rgba(0,230,118,.1)":"rgba(77,98,128,.15)",color:fols.length?"var(--green)":"var(--muted)"}}>{fols.length?`${fols.length} account${fols.length>1?"s":""}` :"UNUSED"}</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        )}

        {/* ══ SETTINGS ══ */}
        {tab==="settings"&&(
          <div className="g2">
            <div className="card">
              <div className="ch"><div className="ct">⚙️ Copy Engine</div></div>
              <div style={{padding:16}}>
                {[
                  {k:"max_lots",l:"Max Lots Per Follower",d:"Cap per account per signal",min:1,max:10,step:1,u:"L"},
                  {k:"dd_pct",l:"Drawdown Pause Threshold",d:"Pause auto-copy if DD exceeds",min:5,max:50,step:5,u:"%"},
                ].map(s=>(
                  <div className="set-row" key={s.k}>
                    <div><div className="sl">{s.l}</div><div className="sd">{s.d}</div></div>
                    <div style={{display:"flex",alignItems:"center",gap:8}}>
                      <input type="range" min={s.min} max={s.max} step={s.step} value={settings[s.k]} onChange={e=>setSettings(p=>({...p,[s.k]:parseInt(e.target.value)}))}/>
                      <span className="sv">{settings[s.k]}{s.u}</span>
                    </div>
                  </div>
                ))}
                {[{k:"stop_mirror",l:"Mirror SL/Target",d:"Copy stop-loss and target orders"},{k:"pause_on_dd",l:"Pause on Drawdown",d:"Auto-pause if DD threshold hit"}].map(s=>(
                  <div className="set-row" key={s.k}>
                    <div><div className="sl">{s.l}</div><div className="sd">{s.d}</div></div>
                    <button className={`sw ${settings[s.k]?"on":""}`} onClick={()=>setSettings(p=>({...p,[s.k]:!p[s.k]}))}/>
                  </div>
                ))}
              </div>
            </div>
            <div className="card">
              <div className="ch"><div className="ct">🎯 Signal Filters</div></div>
              <div style={{padding:16}}>
                {[
                  {k:"exchange_filter",l:"Exchange",d:"Which exchange to copy from",opts:["BOTH","NSE","BSE"],labels:["NSE + BSE","NSE Only","BSE Only"]},
                  {k:"index_filter",l:"Index",d:"Filter to one underlying",opts:["ALL","NIFTY","BANKNIFTY","FINNIFTY","SENSEX","BANKEX"],labels:["All Indices","NIFTY","BANKNIFTY","FINNIFTY","SENSEX","BANKEX"]},
                  {k:"order_type",l:"Order Type",d:"How to place copied orders",opts:["MARKET","LIMIT"],labels:["Market (instant fill)","Limit (at traded price)"]},
                ].map(s=>(
                  <div className="set-row" key={s.k}>
                    <div><div className="sl">{s.l}</div><div className="sd">{s.d}</div></div>
                    <select className="sel" value={settings[s.k]} onChange={e=>setSettings(p=>({...p,[s.k]:e.target.value}))}>
                      {s.opts.map((o,i)=><option key={o} value={o}>{s.labels[i]}</option>)}
                    </select>
                  </div>
                ))}
                <button className="btn btn-pri btn-full" style={{marginTop:8}} onClick={async()=>{
                  try { await api("/api/settings","POST",settings); } catch {}
                }}>Save Settings</button>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* ══ ADD/EDIT FOLLOWER MODAL ══ */}
      {showModal&&(
        <div className="modal-bg" onClick={e=>e.target.className==="modal-bg"&&setShowModal(false)}>
          <div className="modal">
            <div className="modal-title">{editFollower?"Edit Follower":"Add Follower Account"}</div>
            <div className="modal-sub">Enter the follower's broker details and API credentials</div>

            <div className="form-row">
              <div className="form-lbl">Full Name</div>
              <input className="form-inp" placeholder="e.g. Rajesh Kumar" value={form.name} onChange={e=>setForm(p=>({...p,name:e.target.value}))}/>
            </div>

            <div className="form-row">
              <div className="form-lbl">Broker</div>
              <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
                {Object.entries(BROKERS).map(([key,b])=>(
                  <button key={key} onClick={()=>setForm(p=>({...p,broker:key}))}
                    style={{padding:"6px 14px",borderRadius:7,border:`1px solid ${form.broker===key?b.color:b.color+"40"}`,background:form.broker===key?b.color+"20":"transparent",color:form.broker===key?b.color:"var(--muted)",fontFamily:"var(--ff)",fontWeight:700,fontSize:12,cursor:"pointer"}}>
                    {b.logo} {b.name}
                  </button>
                ))}
              </div>
            </div>

            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12,marginBottom:14}}>
              <div><div className="form-lbl">Default Lots</div><input className="form-inp" type="number" min="1" max="10" value={form.lots} onChange={e=>setForm(p=>({...p,lots:e.target.value}))}/></div>
              <div>
                <div className="form-lbl">Billing</div>
                <div className="bt"><button className="bb" style={form.billing==="free"?{background:"rgba(77,98,128,.3)",color:"var(--muted)"}:{}} onClick={()=>setForm(p=>({...p,billing:"free"}))}>Free</button><button className="bb" style={form.billing==="subscription"?{background:"rgba(245,158,11,.15)",color:"var(--gold)"}:{}} onClick={()=>setForm(p=>({...p,billing:"subscription"}))}>Sub</button><button className="bb" style={form.billing==="commission"?{background:"rgba(16,185,129,.15)",color:"#10b981"}:{}} onClick={()=>setForm(p=>({...p,billing:"commission"}))}>Comm</button></div>
              </div>
            </div>

            <div className="form-row">
              <div className="form-lbl">Index Filter</div>
              <div className="tag-toggle">
                {ALL_TAGS.map(t=>(
                  <button key={t} className={`tag-btn ${(form.tags||[]).includes(t)?"on":""} ${NSE_TAGS.includes(t)?"nse":"bse"}`} onClick={()=>toggleTag(t)}>{t}</button>
                ))}
              </div>
              <div className="hint">Only checked indices will be copied to this follower</div>
            </div>

            {/* Dynamic credential fields per broker */}
            <div className="cred-section">
              <div className="cred-title">🔑 {BROKERS[form.broker]?.name} API Credentials</div>
              {(BROKER_CREDS[form.broker]||[]).map(field=>(
                <div className="form-row" key={field.key} style={{marginBottom:11}}>
                  <div className="form-lbl">{field.label}</div>
                  <input
                    className="form-inp"
                    type={field.secret?"password":"text"}
                    placeholder={field.hint}
                    value={form[field.key]||""}
                    onChange={e=>setForm(p=>({...p,[field.key]:e.target.value}))}
                  />
                  <div className="hint">{field.hint}</div>
                </div>
              ))}
            </div>

            <div className="modal-actions">
              <button className="btn btn-ghost" onClick={()=>{setShowModal(false);setEditFollower(null);setForm({...EMPTY_FOLLOWER});}}>Cancel</button>
              <button className="btn btn-pri" disabled={!form.name||saving} onClick={saveFollower}>{saving?"Saving…":editFollower?"Save Changes":"Add Follower"}</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
