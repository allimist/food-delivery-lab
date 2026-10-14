import React,{useCallback,useEffect,useRef,useState} from 'react';
import {createRoot} from 'react-dom/client';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import './style.css';

const API=import.meta.env.VITE_API_URL||'http://localhost:3030/api';
// Service addresses shown on the login page (host-side ports from docker-compose.yml).
const SERVICES=[
  {name:'API',value:API.replace(/\/api$/,''),href:true,usedFor:'Fastify REST API, JWT auth, saga orchestrator, dispatch, outbox publisher, load simulator'},
  {name:'PostgreSQL',value:import.meta.env.VITE_POSTGRES||'localhost:5436 (user/db/password: food)',href:false,usedFor:'Source of truth: users, restaurants, menus, orders, saga log, payments, outbox'},
  {name:'Redis',value:import.meta.env.VITE_REDIS||'redis://localhost:6381',href:false,usedFor:'Courier positions (GEO), the free-courier set, atomic Lua dispatch claims, prep-time averages'},
  {name:'Kafka UI',value:import.meta.env.VITE_KAFKA_UI_URL||'http://localhost:8082',href:true,usedFor:'Topics orders.events, payments.events, couriers.events (keyed by order / courier id)'},
];
const ACCOUNTS=[
  {label:'Admin',email:'admin@example.com',password:'admin123',role:'ADMIN',hint:'live map, simulator, impersonation'},
  {label:'Customer',email:'customer@example.com',password:'customer123',role:'CUSTOMER',hint:'order food, track it'},
  {label:'Customer 2',email:'customer2@example.com',password:'customer123',role:'CUSTOMER',hint:''},
  {label:'Restaurant (accepts by hand)',email:'restaurant@example.com',password:'restaurant123',role:'RESTAURANT',hint:'Somtam Corner'},
  {label:'Restaurant (auto-accept)',email:'restaurant2@example.com',password:'restaurant123',role:'RESTAURANT',hint:'Pad Thai Yaowarat'},
  {label:'Courier',email:'courier@example.com',password:'courier123',role:'COURIER',hint:'ride by hand next to 40 simulated couriers'},
];

async function api(path:string,opts:{method?:string,body?:any,key?:string}={}){
  const token=localStorage.getItem('token');
  const headers:any={...(opts.body?{'Content-Type':'application/json'}:{}),...(token?{Authorization:`Bearer ${token}`}:{}),...(opts.key?{'Idempotency-Key':opts.key}:{})};
  const r=await fetch(API+path,{method:opts.method||(opts.body?'POST':'GET'),headers,body:opts.body?JSON.stringify(opts.body):undefined});
  const data=await r.json().catch(()=>({}));
  if(!r.ok)throw new Error(data.error||data.message||'Request failed');
  return data;
}

/** Calls fn now and every ms; returns [data, reload]. */
function usePoll<T>(fn:()=>Promise<T>,ms:number,deps:any[]=[]):[T|null,()=>void,string]{
  const [data,setData]=useState<T|null>(null),[err,setErr]=useState('');
  const load=useCallback(()=>{fn().then(d=>{setData(d);setErr('')}).catch(e=>setErr(e.message))},deps);
  useEffect(()=>{load();const t=setInterval(load,ms);return()=>clearInterval(t)},[load,ms]);
  return [data,load,err];
}
function useNow(ms=500){const [n,setN]=useState(Date.now());useEffect(()=>{const t=setInterval(()=>setN(Date.now()),ms);return()=>clearInterval(t)},[ms]);return n}

const baht=(n:any)=>'฿'+Math.round(Number(n)).toLocaleString();
const time=(iso:any)=>iso?new Date(iso).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'}):'';
const secs=(n:number)=>n<60?`${Math.max(0,Math.round(n))}s`:`${Math.floor(n/60)}m ${String(Math.round(n%60)).padStart(2,'0')}s`;
const km=(a:{lat:number,lon:number},b:{lat:number,lon:number})=>{const R=6371,r=Math.PI/180,dLat=(b.lat-a.lat)*r,dLon=(b.lon-a.lon)*r;
  const h=Math.sin(dLat/2)**2+Math.cos(a.lat*r)*Math.cos(b.lat*r)*Math.sin(dLon/2)**2;return 2*R*Math.asin(Math.sqrt(h))};
const uuid=()=>crypto.randomUUID?crypto.randomUUID():Math.random().toString(36).slice(2)+Date.now().toString(36);

const LABEL:Record<string,string>={PLACED:'Placed',PAYMENT_AUTHORIZED:'Payment held',RESTAURANT_ACCEPTED:'Restaurant accepted',FOOD_READY:'Food ready',
  COURIER_ASSIGNED:'Courier assigned',PICKED_UP:'Picked up',DELIVERED:'Delivered',PAYMENT_FAILED:'Card declined',REJECTED:'Rejected by restaurant',
  CANCELLED:'Cancelled',CANCELLED_NO_COURIER:'No courier found'};
const FAILED=['PAYMENT_FAILED','REJECTED','CANCELLED','CANCELLED_NO_COURIER'];
const TERMINAL=['DELIVERED',...FAILED];
const Pill=({s}:{s:string})=><span className={`pill ${s==='DELIVERED'?'good':FAILED.includes(s)?'bad':'live'}`}>{LABEL[s]||s}</span>;

function Tile({label,value,hint}:{label:string,value:any,hint?:any}){
  return <div className="tile"><div className="tile-label">{label}</div><div className="tile-value">{value}</div>{hint&&<div className="tile-hint">{hint}</div>}</div>;
}

// ---- City map: Leaflet on OpenStreetMap tiles (no API key; dark mode darkens them with a CSS filter) -------------------------------------
const CITY:L.LatLngBoundsExpression=[[13.70,100.49],[13.80,100.61]];
const TILES='https://tile.openstreetmap.org/{z}/{x}/{y}.png';
const ATTRIBUTION='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors';
type LatLon=[number,number];
/** past: where the courier has been on this order (its trail); ahead: what is left of the streets planned for it. */
type Route={past:LatLon[],ahead:LatLon[]};
type MapProps={restaurants:any[],couriers?:any[],orders?:any[],drops?:{lat:number,lon:number,label:string}[],fit?:boolean,route?:Route|null};
/** What is left of a planned path for a courier at `here`: from here, on from the path segment it is nearest to. */
function rest(path:LatLon[],here:LatLon):LatLon[]{
  const cos=Math.cos(here[0]*Math.PI/180);
  let best=Infinity,k=0;
  for(let i=0;i+1<path.length;i++){
    const [a,b]=[path[i],path[i+1]],dx=(b[1]-a[1])*cos,dy=b[0]-a[0],len=dx*dx+dy*dy;
    const t=len?Math.max(0,Math.min(1,((here[1]-a[1])*cos*dx+(here[0]-a[0])*dy)/len)):0;
    const d=(a[0]+t*dy-here[0])**2+((a[1]-here[1])*cos+t*dx)**2;
    if(d<best){best=d;k=i}
  }
  return [here,...path.slice(k+1)];
}
/** The courier's route for one order: trail so far + its position, then the planned streets to the restaurant (if not
 *  picked up yet) and to the customer. Without a plan, straight lines. */
function courierRoute(o:any,pos:{lat:number,lon:number}|null|undefined):Route|null{
  const past:LatLon[]=[...(o.trail||[])],here:LatLon|null=pos&&Number.isFinite(pos.lat)?[pos.lat,pos.lon]:null;
  if(here)past.push(here);
  const p=o.plan,R:LatLon=[o.restLat,o.restLon],D:LatLon=[o.dropLat,o.dropLon];
  const ahead:LatLon[]=!here?[]:o.status==='COURIER_ASSIGNED'?(p?[...rest(p.toRestaurant,here),...p.toCustomer.slice(1)]:[here,R,D])
    :o.status==='PICKED_UP'?(p?rest(p.toCustomer,here):[here,D]):[];
  return past.length>1||ahead.length?{past,ahead}:null;
}
/** fit: frame the points shown (order tracker, courier screen) and follow them as the courier moves, until the user
 *  pans or zooms; the ⟲ button frames them again. Otherwise the whole city. Couriers are moved, not redrawn. */
function CityMap({restaurants,couriers=[],orders=[],drops=[],fit,route}:MapProps){
  const box=useRef<HTMLDivElement>(null);
  const st=useRef<{map:L.Map,shapes:L.LayerGroup,couriers:Map<string,L.CircleMarker>,follow:boolean,auto:boolean,key:string,past:L.Polyline,ahead:L.Polyline}|null>(null);
  const pts=useRef<[number,number][]>([]);
  pts.current=[...[...restaurants,...couriers,...drops].filter(p=>Number.isFinite(p.lat)).map(p=>[p.lat,p.lon] as LatLon),...(route?.past||[])];
  const frame=()=>{const s=st.current;if(!s)return;s.auto=true;
    if(fit&&pts.current.length)s.map.fitBounds(L.latLngBounds(pts.current),{paddingTopLeft:[40,40],paddingBottomRight:[170,40],maxZoom:16,animate:false});
    else s.map.fitBounds(CITY,{animate:false});
    s.auto=false};
  useEffect(()=>{
    const map=L.map(box.current!,{minZoom:8,maxZoom:18,zoomSnap:0.5,wheelPxPerZoomLevel:90});
    L.tileLayer(TILES,{attribution:ATTRIBUTION,maxZoom:19}).addTo(map);
    map.createPane('route').style.zIndex='420';     // the courier's route, above routes and drop-offs
    map.createPane('couriers').style.zIndex='450';  // above routes and drop-offs (overlay pane, 400), below restaurant icons
    const s:NonNullable<typeof st.current>={map,shapes:L.layerGroup().addTo(map),couriers:new Map(),follow:true,auto:false,key:'',
      past:L.polyline([],{pane:'route',className:'route-past',weight:5,lineCap:'round',lineJoin:'round',interactive:false}).addTo(map),
      ahead:L.polyline([],{pane:'route',className:'route-ahead',weight:5,dashArray:'8 8',lineCap:'round',lineJoin:'round',interactive:false}).addTo(map)};st.current=s;
    const Reset=L.Control.extend({onAdd(){const b=L.DomUtil.create('a','map-reset');b.href='#';b.innerHTML='⟲';b.title=fit?'Frame the order again':'Whole city';
      b.setAttribute('role','button');b.setAttribute('aria-label',b.title);
      L.DomEvent.on(b,'click',e=>{L.DomEvent.stop(e);s.follow=true;frame()});const bar=L.DomUtil.create('div','leaflet-bar');bar.appendChild(b);return bar}});
    new Reset({position:'topleft'}).addTo(map);
    map.on('dragstart',()=>{s.follow=false});map.on('zoomstart',()=>{if(!s.auto)s.follow=false});
    frame();
    return()=>{map.remove();st.current=null};
  },[]);
  useEffect(()=>{const s=st.current;if(!s)return;
    // restaurants, routes and drop-offs change rarely: rebuild them only when they do
    const key=JSON.stringify([restaurants.map(r=>r.id),orders.map(o=>[o.id,o.status]),drops]);
    if(key!==s.key){s.key=key;s.shapes.clearLayers();
      for(const o of orders){L.polyline([[o.restLat,o.restLon],[o.dropLat,o.dropLon]],{className:"route",weight:2,dashArray:"5 5",interactive:false}).addTo(s.shapes);
        L.circleMarker([o.dropLat,o.dropLon],{className:'drop',radius:5,weight:2,fillOpacity:1}).bindTooltip(`Drop-off · ${LABEL[o.status]}`).addTo(s.shapes)}
      for(const d of drops)L.circleMarker([d.lat,d.lon],{className:'drop',radius:7,weight:2,fillOpacity:1})
        .bindTooltip(d.label,{permanent:true,direction:'right',offset:[8,0],className:'map-tip'}).addTo(s.shapes);
      for(const r of restaurants)L.marker([r.lat,r.lon],{icon:L.divIcon({className:'rest-icon',iconSize:[12,12]}),keyboard:false})
        .bindTooltip(r.name,{permanent:true,direction:'right',offset:[8,0],className:'map-tip'}).addTo(s.shapes);
    }
    const seen=new Set<string>();
    for(const c of couriers){if(!Number.isFinite(c.lat))continue;seen.add(c.id);const busy=!!c.orderId;
      const tip=`${c.name||'Courier '+String(c.id).slice(0,6)} · ${busy?'carrying order '+String(c.orderId).slice(0,8):'free'}`;
      let mk=s.couriers.get(c.id);
      if(!mk){mk=L.circleMarker([c.lat,c.lon],{pane:'couriers',className:'courier',radius:6,weight:2,fillOpacity:1}).addTo(s.map);
        if(fit&&c.name)mk.bindTooltip(c.name,{permanent:true,direction:'top',offset:[0,-6],className:'map-tip'});else mk.bindTooltip(tip);
        s.couriers.set(c.id,mk)}
      else{mk.setLatLng([c.lat,c.lon]);if(!(fit&&c.name))mk.setTooltipContent(tip)}
      mk.setRadius(busy?7:6);mk.getElement()?.classList.toggle('busy',busy);
    }
    for(const [id,mk] of s.couriers)if(!seen.has(id)){mk.remove();s.couriers.delete(id)}
    s.past.setLatLngs(route?.past||[]);s.ahead.setLatLngs(route?.ahead||[]);
    if(fit&&s.follow)frame();
  });
  return <figure className="map">
    <div ref={box} className={fit?'map-canvas fit':'map-canvas'} role="img" aria-label="Map of central Bangkok with restaurants, couriers and deliveries"/>
    <figcaption className="legend">
      <span><i className="sw rest"/>Restaurant</span><span><i className="sw free"/>Free courier</span><span><i className="sw busy"/>Courier with an order</span><span><i className="sw drop"/>Drop-off</span>
      {route&&<><span><i className="ln past"/>Courier's route so far</span><span><i className="ln ahead"/>Still to ride</span></>}
      <span className="muted">Scroll or pinch to zoom, drag to move, ⟲ to {fit?'frame the order':'see the whole city'}</span>
    </figcaption>
  </figure>;
}

// ---- Order tracker -------------------------------------------------------------------------------------------------
const FLOW=['PLACED','PAYMENT_AUTHORIZED','RESTAURANT_ACCEPTED','COURIER_ASSIGNED','FOOD_READY','PICKED_UP','DELIVERED'];
function stepNote(s:any){const d=s.detail||{};
  if(s.status==='COURIER_ASSIGNED')return `${d.distanceKm} km away (searched ${d.radiusKm} km)`;
  if(s.status==='FOOD_READY')return d.by==='auto'?'kitchen timer':'marked by the restaurant';
  if(s.status==='RESTAURANT_ACCEPTED')return d.by==='auto'?'auto-accepted, payment captured':'accepted by staff, payment captured';
  if(s.status==='DELIVERED')return d.lateBySeconds>0?`${secs(d.lateBySeconds)} after the quoted ETA`:`${secs(-d.lateBySeconds)} before the quoted ETA`;
  return [d.reason,d.compensation].filter(Boolean).join(' · ');
}
function Tracker({id,onClose,note}:{id:string,onClose?:()=>void,note?:string}){
  // skew: server clock minus browser clock. ETAs are server times; a container VM's clock can drift (after sleep).
  const [o,reload]=usePoll(()=>api('/orders/'+id).then(o=>({...o,skew:o.serverTime?Date.parse(o.serverTime)-Date.now():0})),1000,[id]);
  const now=useNow();
  const [msg,setMsg]=useState('');
  if(!o)return <div className="card">Loading…</div>;
  const done=new Map(o.steps.map((s:any)=>[s.status,s]));
  const failed=o.steps.find((s:any)=>FAILED.includes(s.status));
  const eta=o.etaLive||o.etaAt,left=(new Date(eta).getTime()-now-o.skew)/1000;   // seconds, on the server's clock
  const cancel=async()=>{try{await api(`/orders/${id}/cancel`,{body:{}});reload()}catch(e:any){setMsg(e.message)}};
  return <div className="card tracker">
    <div className="row between"><h2>{o.restaurant} <small>#{o.id.slice(0,8)}</small></h2>{onClose&&<button onClick={onClose}>Close</button>}</div>
    <div className="row">
      <Pill s={o.status}/>
      {!TERMINAL.includes(o.status)&&eta&&<span className="eta">Arrives {left<1?<b>any moment</b>:<>in <b>{secs(left)}</b></>} <small>at {time(eta)} · quoted {time(o.etaQuotedAt)}</small></span>}
      {o.status==='DELIVERED'&&<span className="eta">Delivered at {time(o.deliveredAt)} <small>quoted {time(o.etaQuotedAt)}</small></span>}
      {o.payment&&<span className="muted">Payment: <b>{o.payment.status.toLowerCase()}</b> {baht(o.payment.amount)}</span>}
      {o.status==='PAYMENT_AUTHORIZED'&&<button className="danger" onClick={cancel}>Cancel order</button>}
    </div>
    {note&&<p className="flash">{note}</p>}
    {msg&&<p className="error">{msg}</p>}
    <div className="split">
      <ol className="steps">
        {FLOW.map(s=>{const d:any=done.get(s);return <li key={s} className={d?'done':''}><b>{LABEL[s]}</b> {d&&<small>{time(d.at)}</small>}{d&&stepNote(d)&&<div className="muted">{stepNote(d)}</div>}</li>})}
        {failed&&<li className="failed"><b>{LABEL[failed.status]}</b> <small>{time(failed.at)}</small><div>{stepNote(failed)}</div></li>}
      </ol>
      <CityMap fit route={courierRoute(o,o.courierPos)} restaurants={[{id:'r',name:o.restaurant,lat:o.restLat,lon:o.restLon}]} drops={[{lat:o.dropLat,lon:o.dropLon,label:o.address}]}
        couriers={o.courierPos?[{id:o.courierId,name:o.courier,orderId:o.id,...o.courierPos}]:[]}/>
    </div>
    <table><tbody>{o.items.map((i:any)=><tr key={i.name}><td>{i.qty} × {i.name}</td><td className="num">{baht(i.qty*i.unitPrice)}</td></tr>)}
      <tr><td>Delivery fee</td><td className="num">{baht(o.deliveryFee)}</td></tr><tr><td><b>Total</b></td><td className="num"><b>{baht(o.total)}</b></td></tr></tbody></table>
  </div>;
}

// ---- Customer --------------------------------------------------------------------------------------------------------
function Customer(){
  const [view,setView]=useState<{page:'list'}|{page:'menu',id:string}|{page:'order',id:string,note?:string}|{page:'orders'}>({page:'list'});
  const [rests]=usePoll(()=>api('/restaurants'),15000);
  return <>
    <nav className="tabs"><button className={view.page==='list'||view.page==='menu'?'active':''} onClick={()=>setView({page:'list'})}>Restaurants</button>
      <button className={view.page==='orders'||view.page==='order'?'active':''} onClick={()=>setView({page:'orders'})}>My orders</button></nav>
    {view.page==='list'&&<div className="grid">{(rests||[]).map((r:any)=><button key={r.id} className="card rcard" onClick={()=>setView({page:'menu',id:r.id})}>
      <b>{r.name}</b><span className="muted">{r.cuisine} · {r.area}</span>
      <span className="muted">Prep ≈ {r.prepAvgMin} min{r.prepAvgMin!==r.basePrepMin&&<> (learned, started at {r.basePrepMin})</>}</span>
      {!r.autoAccept&&<span className="tag">accepts by hand</span>}</button>)}</div>}
    {view.page==='menu'&&<Menu id={view.id} onPlaced={(id,note)=>setView({page:'order',id,note})}/>}
    {view.page==='orders'&&<MyOrders open={id=>setView({page:'order',id})}/>}
    {view.page==='order'&&<Tracker key={view.id} id={view.id} note={view.note} onClose={()=>setView({page:'orders'})}/>}
  </>;
}

function Menu({id,onPlaced}:{id:string,onPlaced:(id:string,note?:string)=>void}){
  const [r,setR]=useState<any>(null),[cfg,setCfg]=useState<any>(null);
  const [cart,setCart]=useState<Record<string,number>>({}),[addr,setAddr]=useState('lumpini'),[card,setCard]=useState('ok');
  const [msg,setMsg]=useState(''),[busy,setBusy]=useState(false);
  // One key per checkout: a retry or double click of the same cart reuses it, a changed cart gets a new one.
  const key=useRef(uuid());
  useEffect(()=>{api('/restaurants/'+id).then(setR);api('/config').then(setCfg)},[id]);
  useEffect(()=>{key.current=uuid()},[JSON.stringify(cart),addr,card]);
  if(!r||!cfg)return <div className="card">Loading…</div>;
  const items=Object.entries(cart).filter(([,q])=>q>0).map(([menuItemId,qty])=>({menuItemId,qty}));
  const subtotal=items.reduce((s,i)=>s+r.menu.find((m:any)=>m.id===i.menuItemId).price*i.qty,0);
  const a=cfg.addresses.find((x:any)=>x.id===addr),dist=km(r,a),fee=Math.round(15+7*dist);
  const add=(mid:string,d:number)=>setCart(c=>({...c,[mid]:Math.max(0,Math.min(20,(c[mid]||0)+d))}));
  const place=async(twice:boolean)=>{
    setBusy(true);setMsg('');
    try{
      const body={restaurantId:r.id,items,addressId:addr,card};
      const res=await Promise.all((twice?[1,2]:[1]).map(()=>api('/orders',{body,key:key.current})));
      onPlaced(res[0].orderId,twice?`Sent twice at once with one Idempotency-Key: ${new Set(res.map(x=>x.orderId)).size} order created, ${res.filter(x=>x.replayed).length} answer was a replay.`:undefined);
    }catch(e:any){setMsg(e.message)}finally{setBusy(false)}
  };
  return <div className="split menu">
    <div className="card"><h2>{r.name} <small>{r.cuisine} · {r.area}</small></h2>
      <p className="muted">Usual prep {r.prepAvgMin} min (real-world minutes; the lab runs {cfg.timeScale}× faster).{!r.autoAccept&&' Staff accept orders by hand: log in as restaurant@example.com.'}</p>
      <table><tbody>{r.menu.map((m:any)=><tr key={m.id}><td><b>{m.name}</b><div className="muted">{m.description}</div></td><td className="num">{baht(m.price)}</td>
        <td className="qty"><button onClick={()=>add(m.id,-1)} disabled={!cart[m.id]}>−</button><span>{cart[m.id]||0}</span><button onClick={()=>add(m.id,1)}>+</button></td></tr>)}</tbody></table>
    </div>
    <div className="card"><h3>Checkout</h3>
      <label>Deliver to<select value={addr} onChange={e=>setAddr(e.target.value)}>{cfg.addresses.map((x:any)=><option key={x.id} value={x.id}>{x.label} ({km(r,x).toFixed(1)} km)</option>)}</select></label>
      <label>Card<select value={card} onChange={e=>setCard(e.target.value)}><option value="ok">Visa •••• 4242 (approves)</option><option value="decline">Visa •••• 0002 (declines)</option></select></label>
      <table><tbody><tr><td>Food</td><td className="num">{baht(subtotal)}</td></tr><tr><td>Delivery ({dist.toFixed(1)} km)</td><td className="num">{baht(fee)}</td></tr>
        <tr><td><b>Total</b></td><td className="num"><b>{baht(subtotal+fee)}</b></td></tr></tbody></table>
      <div className="row"><button className="primary" disabled={!items.length||busy} onClick={()=>place(false)}>Place order</button>
        <button disabled={!items.length||busy} onClick={()=>place(true)} title="Sends the same request twice at the same instant, like a double click">Place twice at once</button></div>
      <p className="muted small">Idempotency-Key <code>{key.current.slice(0,8)}…</code>: the same cart sent again returns the first order.</p>
      {msg&&<p className="error">{msg}</p>}
    </div>
  </div>;
}

function MyOrders({open}:{open:(id:string)=>void}){
  const [orders]=usePoll(()=>api('/orders'),2000);
  return <div className="card"><OrdersTable orders={orders||[]} open={open}/></div>;
}

function OrdersTable({orders,open,showPeople}:{orders:any[],open:(id:string)=>void,showPeople?:boolean}){
  if(!orders.length)return <p className="muted">No orders yet.</p>;
  return <div className="tablewrap"><table className="clickable"><thead><tr><th>Placed</th><th>Restaurant</th>{showPeople&&<><th>Customer</th><th>Courier</th></>}<th>Status</th><th className="num">Total</th></tr></thead>
    <tbody>{orders.map(o=><tr key={o.id} onClick={()=>open(o.id)}><td>{time(o.createdAt)}</td><td>{o.restaurant}</td>
      {showPeople&&<><td>{o.customer}</td><td>{o.courier||'—'}</td></>}<td><Pill s={o.status}/>{o.cancelReason&&<div className="muted small">{o.cancelReason}</div>}</td><td className="num">{baht(o.total)}</td></tr>)}</tbody></table></div>;
}

// ---- Restaurant ------------------------------------------------------------------------------------------------------
function Restaurant(){
  const [d,reload,err]=usePoll(()=>api('/restaurant/orders'),1500);
  const now=useNow(1000);
  const [msg,setMsg]=useState('');
  if(err)return <p className="error">{err}</p>;
  if(!d)return <div className="card">Loading…</div>;
  const act=async(id:string,a:string)=>{try{setMsg('');await api(`/restaurant/orders/${id}/${a}`,{body:{}});reload()}catch(e:any){setMsg(e.message)}};
  const incoming=d.orders.filter((o:any)=>o.status==='PAYMENT_AUTHORIZED');
  const kitchen=d.orders.filter((o:any)=>['RESTAURANT_ACCEPTED','COURIER_ASSIGNED'].includes(o.status)&&!o.readyAt);
  const waiting=d.orders.filter((o:any)=>['RESTAURANT_ACCEPTED','COURIER_ASSIGNED'].includes(o.status)&&o.readyAt);
  const items=(o:any)=>o.items.map((i:any)=>`${i.qty}× ${i.name}`).join(', ');
  return <>
    <div className="tiles"><Tile label="Restaurant" value={d.restaurant.name} hint={d.restaurant.autoAccept?'auto-accepts orders':'accepts by hand'}/>
      <Tile label="Prep average" value={`${d.restaurant.prepAvgMin} min`} hint={`learned from real prep times, started at ${d.restaurant.basePrepMin}`}/>
      <Tile label="New" value={incoming.length}/><Tile label="Cooking" value={kitchen.length}/></div>
    {msg&&<p className="error">{msg}</p>}
    <div className="card"><h3>New orders</h3>{!incoming.length&&<p className="muted">Nothing waiting. Log in as a customer in another window and order from {d.restaurant.name}.</p>}
      {incoming.map((o:any)=>{const left=d.restaurantTimeoutS-(now-new Date(o.updatedAt).getTime())/1000;
        return <div key={o.id} className="row between line"><div><b>{items(o)}</b><div className="muted">{o.customer} · {o.address} · {baht(o.total)}</div></div>
          <div className="row"><span className={left<15?'countdown warn':'countdown'}>auto-reject in {secs(left)}</span>
            <button className="primary" onClick={()=>act(o.id,'accept')}>Accept</button><button className="danger" onClick={()=>act(o.id,'reject')}>Reject</button></div></div>})}
    </div>
    <div className="card"><h3>In the kitchen</h3>{!kitchen.length&&<p className="muted">Nothing cooking.</p>}
      {kitchen.map((o:any)=><div key={o.id} className="row between line"><div><b>{items(o)}</b><div className="muted">accepted {secs((now-new Date(o.acceptedAt).getTime())/1000)} ago · {o.courier?`courier ${o.courier} on the way`:'looking for a courier'}</div></div>
        <button className="primary" onClick={()=>act(o.id,'ready')}>Food ready</button></div>)}
      {waiting.map((o:any)=><div key={o.id} className="row between line"><div><b>{items(o)}</b><div className="muted">ready, waiting for {o.courier||'a courier'}</div></div></div>)}
    </div>
    <div className="card"><h3>All orders</h3><OrdersTable orders={d.orders} open={()=>{}} showPeople/></div>
  </>;
}

// ---- Courier ---------------------------------------------------------------------------------------------------------
function Courier(){
  const [d,reload]=usePoll(()=>api('/courier/current'),1500);
  const [msg,setMsg]=useState('');
  const run=async(f:()=>Promise<any>)=>{try{setMsg('');await f();reload()}catch(e:any){setMsg(e.message)}};
  if(!d)return <div className="card">Loading…</div>;
  const o=d.order,start={lat:13.7314,lon:100.5414};
  // Riding by hand jumps to the place, reporting the planned streets on the way so the trail follows them.
  const goTo=(lat:number,lon:number,path?:LatLon[])=>run(()=>api('/courier/location',{body:{lat,lon,via:path&&d.position?rest(path,[d.position.lat,d.position.lon]).slice(1,-1):[]}}));
  return <>
    <div className="tiles"><Tile label="Status" value={d.order?'On a delivery':d.online?'Online, free':'Offline'} hint={d.inFreeSet?'in the Redis free set: dispatch can pick you':'not in the free set'}/>
      <Tile label="Delivered" value={d.delivered}/></div>
    {msg&&<p className="error">{msg}</p>}
    <div className="card">
      {!d.online?<button className="primary" onClick={()=>run(()=>api('/courier/online',{body:d.position||start}))}>Go online at {d.position?'my last position':'Lumpini Park'}</button>
        :!o&&<button onClick={()=>run(()=>api('/courier/offline',{body:{}}))}>Go offline</button>}
      {d.online&&!o&&<p className="muted">Waiting for an order. Dispatch gives each order to the nearest free courier, so you compete with the simulated couriers: an admin can set them to 0 on the live map.</p>}
      {o&&<div className="job"><h3>{o.status==='COURIER_ASSIGNED'?'Pick up at':'Deliver to'} {o.status==='COURIER_ASSIGNED'?`${o.restaurant} (${o.restArea})`:o.address}</h3>
        <p className="muted">{o.items.map((i:any)=>`${i.qty}× ${i.name}`).join(', ')} · {o.readyAt?'food is ready':'kitchen is still cooking'}</p>
        <div className="row">{o.status==='COURIER_ASSIGNED'?<><button onClick={()=>goTo(o.restLat,o.restLon,o.plan?.toRestaurant)}>Ride to the restaurant</button><button className="primary" onClick={()=>run(()=>api(`/courier/orders/${o.id}/pickup`,{body:{}}))}>Picked up</button></>
          :<><button onClick={()=>goTo(o.dropLat,o.dropLon,o.plan?.toCustomer)}>Ride to the customer</button><button className="primary" onClick={()=>run(()=>api(`/courier/orders/${o.id}/deliver`,{body:{}}))}>Delivered</button></>}</div>
        <p className="muted small">Pick-up and delivery are refused unless you are within 150 m (your position is in Redis GEO).</p></div>}
    </div>
    {d.position&&<div className="card"><CityMap fit route={o?courierRoute(o,d.position):null} restaurants={o?[{id:'r',name:o.restaurant,lat:o.restLat,lon:o.restLon}]:[]} drops={o?[{lat:o.dropLat,lon:o.dropLon,label:o.address}]:[]}
      couriers={[{id:'me',name:'You',orderId:o?.id,...d.position}]}/></div>}
  </>;
}

// ---- Admin -----------------------------------------------------------------------------------------------------------
function Admin({onImpersonate}:{onImpersonate:(t:string,u:any)=>void}){
  const [tab,setTab]=useState(()=>localStorage.getItem('adminTab')||'map');
  useEffect(()=>{try{localStorage.setItem('adminTab',tab)}catch{}},[tab]);
  return <>
    <nav className="tabs">{[['map','Live map'],['orders','Orders'],['sim','Load simulator'],['users','Accounts']].map(([k,l])=><button key={k} className={tab===k?'active':''} onClick={()=>setTab(k)}>{l}</button>)}</nav>
    {tab==='map'&&<LiveMap/>}{tab==='orders'&&<AdminOrders/>}{tab==='sim'&&<Simulator/>}{tab==='users'&&<Users onImpersonate={onImpersonate}/>}
  </>;
}

function LiveMap(){
  const [m]=usePoll(()=>api('/admin/map'),1000);
  const [s,reloadStats]=usePoll(()=>api('/admin/stats'),2000);
  const [online,setOnline]=useState<number|null>(null),[check,setCheck]=useState<any>(null),[msg,setMsg]=useState('');
  useEffect(()=>{if(s&&online===null)setOnline(s.onlineTarget)},[s]);
  const run=async(f:()=>Promise<any>,done?:(r:any)=>void)=>{try{setMsg('');const r=await f();done?.(r);reloadStats()}catch(e:any){setMsg(e.message)}};
  const active=s?Object.entries(s.orders).filter(([k])=>!TERMINAL.includes(k)).reduce((a,[,n])=>a+Number(n),0):0;
  return <>
    {s&&<div className="tiles"><Tile label="Couriers online" value={s.couriers.online} hint={`${s.couriers.free} free · ${s.couriers.busy} busy`}/>
      <Tile label="Active orders" value={active}/><Tile label="Delivered" value={s.orders.DELIVERED||0}/>
      <Tile label="Cancelled / failed" value={FAILED.reduce((a,k)=>a+(s.orders[k]||0),0)}/>
      <Tile label="Outbox" value={`${s.outbox.pending} pending`} hint={`${s.outbox.published} published to Kafka`}/></div>}
    <div className="split wide">
      <div className="card">{m&&<CityMap restaurants={m.restaurants} couriers={m.couriers} orders={m.orders}/>}</div>
      <div className="card controls"><h3>Simulated couriers</h3>
        <label><span>Online: <b>{online??'…'}</b> of 40</span><input type="range" min={0} max={40} value={online??0} onChange={e=>setOnline(Number(e.target.value))}
          onMouseUp={()=>run(()=>api('/admin/sim-settings',{method:'PUT',body:{onlineCouriers:online}}))} onTouchEnd={()=>run(()=>api('/admin/sim-settings',{method:'PUT',body:{onlineCouriers:online}}))}/></label>
        <p className="muted small">Couriers above the limit finish their order, then go offline. Set 0 to see the "no courier" compensation.</p>
        <h3>Redis vs PostgreSQL</h3>
        <div className="row"><button onClick={()=>run(()=>api('/admin/consistency'),setCheck)}>Check</button><button onClick={()=>run(()=>api('/admin/reconcile',{body:{}}),r=>setCheck({ok:true,problems:[],fixed:r.fixed}))}>Rebuild Redis from PostgreSQL</button></div>
        {check&&<p className={check.ok?'ok':'error'}>{check.ok?`✓ consistent${check.checkedCouriers!==undefined?` (${check.checkedCouriers} couriers, ${check.activeOrders} active orders)`:''}${check.fixed!==undefined?` · ${check.fixed} fixed`:''}`:check.problems.join('; ')}</p>}
        <h3>Payments</h3>
        {s&&<table><tbody>{s.payments.map((p:any)=><tr key={p.status}><td>{p.status.toLowerCase()}</td><td className="num">{p.n}</td><td className="num">{baht(p.amount)}</td></tr>)}</tbody></table>}
        <h3>Kafka topics</h3>
        {s?.outbox.byTopic&&<table><tbody>{Object.entries(s.outbox.byTopic).map(([t,n]:any)=><tr key={t}><td><code>{t}</code></td><td className="num">{n}</td></tr>)}</tbody></table>}
        <h3>Start over</h3>
        <button className="danger" onClick={()=>run(()=>api('/admin/reset',{body:{}}),r=>setMsg(`Deleted ${r.deletedOrders} orders`))}>Delete all orders</button>
        {msg&&<p className="muted">{msg}</p>}
      </div>
    </div>
  </>;
}

function AdminOrders(){
  const [all,setAll]=useState(false),[open,setOpen]=useState<string|null>(null);
  const [orders]=usePoll(()=>api('/orders'+(all?'?all=1':'')),2000,[all]);
  return <>{open&&<div className="modal" onClick={e=>e.target===e.currentTarget&&setOpen(null)}><Tracker id={open} onClose={()=>setOpen(null)}/></div>}
    <div className="card"><label className="check"><input type="checkbox" checked={all} onChange={e=>setAll(e.target.checked)}/> include load-test orders</label>
      <OrdersTable orders={orders||[]} open={setOpen} showPeople/></div></>;
}

/** One bar per step: the p95, with p50 as a tick. Single measure, so one hue and no legend box. */
function LatencyBars({title,rows,unit}:{title:string,rows:[string,any][],unit:string}){
  const max=Math.max(1,...rows.map(([,s])=>s.p95||0));
  return <figure className="bars"><figcaption>{title}</figcaption>
    <table><thead><tr><th>Step</th><th className="num">n</th><th className="num">p50</th><th className="num">p95</th><th className="barcol">p95 (tick = p50)</th></tr></thead>
      <tbody>{rows.map(([k,s])=><tr key={k} title={`${k}: p50 ${s.p50} ${unit}, p95 ${s.p95} ${unit}, max ${s.max} ${unit}`}><td>{k}</td><td className="num">{s.n}</td><td className="num">{s.p50??'—'}</td><td className="num">{s.p95??'—'}</td>
        <td className="barcol"><div className="hbar"><i style={{width:`${(s.p95||0)/max*100}%`}}/>{s.p50!=null&&<b style={{left:`${s.p50/max*100}%`}}/>}</div></td></tr>)}</tbody></table></figure>;
}

function Simulator(){
  const [orders,setOrders]=useState(60),[dup,setDup]=useState(20),[msg,setMsg]=useState('');
  const [runs,reload]=usePoll(()=>api('/admin/simulations'),2000);
  const run=runs?.[0],r=run?.report||{};
  const start=async()=>{try{setMsg('');await api('/admin/simulate',{body:{orders,duplicatePct:dup}});reload()}catch(e:any){setMsg(e.message)}};
  return <div className="card">
    <h3>Burst of orders</h3>
    <p className="muted">N customers order at the same instant from the auto-accept restaurants; some send the same request twice (same Idempotency-Key). The simulated couriers deliver. Couriers online is set on the live map.</p>
    <div className="row"><label>Orders<input type="number" min={1} max={500} value={orders} onChange={e=>setOrders(Number(e.target.value))}/></label>
      <label>Sent twice (%)<input type="number" min={0} max={100} value={dup} onChange={e=>setDup(Number(e.target.value))}/></label>
      <button className="primary" disabled={run?.status==='RUNNING'} onClick={start}>Run</button></div>
    {msg&&<p className="error">{msg}</p>}
    {run&&<div className="report">
      <div className="row between"><b>Run {time(run.createdAt)} · {run.params.orders} orders · {run.params.onlineCouriers} couriers online · ×{run.params.timeScale} time</b><span className={`pill ${run.status==='DONE'?'good':run.status==='FAILED'?'bad':'live'}`}>{run.status.toLowerCase()}</span></div>
      {run.status==='RUNNING'&&<div className="meter"><div style={{width:`${r.progressPct||0}%`}}/></div>}
      {r.byStatus&&<div className="row">{Object.entries(r.byStatus).map(([k,n]:any)=><span key={k}><Pill s={k}/> {n}</span>)}</div>}
      {r.summary&&<div className="tiles">
        <Tile label="Requests → orders" value={`${r.summary.requests} → ${r.summary.ordersCreated}`} hint={`${r.summary.replayed} replayed, ${r.summary.duplicateOrders} duplicate orders`}/>
        <Tile label="Courier overlaps" value={r.summary.courierOverlaps} hint="a courier holding two orders at once"/>
        <Tile label="Couriers used" value={r.summary.couriersUsed} hint={`max ${r.summary.deliveriesPerCourier.max ?? 0} deliveries each`}/>
        <Tile label="Redis = PostgreSQL" value={r.summary.consistent?'yes':'NO'}/></div>}
      {r.timings&&<LatencyBars title="Request path (ms)" rows={Object.entries(r.timings)} unit="ms"/>}
      {r.lifecycle&&<LatencyBars title="Order lifecycle (real seconds)" rows={Object.entries(r.lifecycle).filter(([k])=>!k.includes('(ms)'))} unit="s"/>}
      {r.lifecycle&&<LatencyBars title="Outbox → Kafka (ms)" rows={Object.entries(r.lifecycle).filter(([k])=>k.includes('(ms)'))} unit="ms"/>}
      {r.bottlenecks&&<><h4>What limited this run</h4><ul>{r.bottlenecks.map((b:string)=><li key={b}>{b}</li>)}</ul></>}
      {r.log&&<pre className="log">{r.log.join('\n')}</pre>}
      {r.error&&<p className="error">{r.error}</p>}
    </div>}
  </div>;
}

function Users({onImpersonate}:{onImpersonate:(t:string,u:any)=>void}){
  const [users]=usePoll(()=>api('/admin/users'),30000);
  const [showSim,setShowSim]=useState(false);
  const imp=async(u:any)=>{const r=await api(`/admin/impersonate/${u.id}`,{body:{}});onImpersonate(r.token,r.user)};
  const list=(users||[]).filter((u:any)=>showSim||!u.isSim);
  return <div className="card"><label className="check"><input type="checkbox" checked={showSim} onChange={e=>setShowSim(e.target.checked)}/> show the 40 simulated couriers</label>
    <div className="tablewrap"><table><thead><tr><th>Role</th><th>Name</th><th>Email</th><th>Password</th><th></th></tr></thead>
      <tbody>{list.map((u:any)=><tr key={u.id}><td><span className={`role ${u.role}`}>{u.role.toLowerCase()}</span></td><td>{u.name}{u.restaurant&&<div className="muted small">{u.restaurant}</div>}</td>
        <td>{u.email}</td><td><code>{u.password}</code></td><td>{u.role!=='ADMIN'&&<button onClick={()=>imp(u)}>Act as</button>}</td></tr>)}</tbody></table></div></div>;
}

// ---- Shell -----------------------------------------------------------------------------------------------------------
function Login({onLogin}:{onLogin:(t:string,u:any)=>void}){
  const [msg,setMsg]=useState('');
  const login=async(a:any)=>{try{const r=await api('/auth/login',{body:{email:a.email,password:a.password}});onLogin(r.token,r.user)}catch(e:any){setMsg(e.message)}};
  return <>
    <div className="card"><h2>Log in as</h2><p className="muted">Passwords are plain text and shown here on purpose: this is a lab, not a product.</p>
      <table className="clickable"><thead><tr><th>Account</th><th>Email</th><th>Password</th><th>Try</th></tr></thead>
        <tbody>{ACCOUNTS.map(a=><tr key={a.email} onClick={()=>login(a)}><td><span className={`role ${a.role}`}>{a.label}</span></td><td>{a.email}</td><td><code>{a.password}</code></td><td className="muted">{a.hint}</td></tr>)}</tbody></table>
      {msg&&<p className="error">{msg}</p>}</div>
    <div className="card"><h3>Services</h3><table><tbody>{SERVICES.map(s=><tr key={s.name}><td>{s.name}</td><td>{s.href?<a href={s.value} target="_blank">{s.value}</a>:<code>{s.value}</code>}</td><td className="muted">{s.usedFor}</td></tr>)}</tbody></table></div>
  </>;
}

function App(){
  const [user,setUser]=useState<any>(()=>{try{return JSON.parse(localStorage.getItem('user')||'null')}catch{return null}});
  const set=(t:string|null,u:any)=>{if(t){localStorage.setItem('token',t);localStorage.setItem('user',JSON.stringify(u))}else{localStorage.removeItem('token');localStorage.removeItem('user')}setUser(u)};
  const impersonate=(t:string,u:any)=>{localStorage.setItem('adminToken',localStorage.getItem('token')||'');localStorage.setItem('adminUser',localStorage.getItem('user')||'');set(t,u)};
  const back=()=>{const t=localStorage.getItem('adminToken'),u=localStorage.getItem('adminUser');localStorage.removeItem('adminToken');localStorage.removeItem('adminUser');set(t,JSON.parse(u||'null'))};
  const logout=()=>{localStorage.removeItem('adminToken');set(null,null)};
  return <main>
    <header><h1>🛵 Food Delivery Lab <small>Bangkok</small></h1>
      {user&&<div className="row"><span>{user.name} <span className={`role ${user.role}`}>{user.role.toLowerCase()}</span></span>
        {localStorage.getItem('adminToken')&&<button onClick={back}>Back to admin</button>}<button onClick={logout}>Log out</button></div>}</header>
    {!user?<Login onLogin={set}/>:user.role==='CUSTOMER'?<Customer/>:user.role==='RESTAURANT'?<Restaurant/>:user.role==='COURIER'?<Courier/>:<Admin onImpersonate={impersonate}/>}
  </main>;
}

createRoot(document.getElementById('root')!).render(<App/>);
