import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

const CFG = window.CATALOG;                 // {title, base, attribution}
const app = document.getElementById("app");
const crumbs = document.getElementById("crumbs");
const brand = document.getElementById("brand");
if(brand) brand.textContent = CFG.title;
const esc = s => String(s==null?"":s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const human = s => s.replace(/__tab$/," (tabla)").replace(/_/g," ").replace(/\b\w/g,c=>c.toUpperCase());
const getJSON = async u => (await fetch(u)).json();
const dataURL = p => `${CFG.base}/${p}`;
const s3of = u => u.replace("https://storage.googleapis.com/","s3://");

let _db=null;
async function db(){
  if(_db) return _db;
  const b=await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const w=await duckdb.createWorker(b.mainWorker);
  const d=new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(),w);
  await d.instantiate(b.mainModule,b.pthreadWorker);
  const c=await d.connect();
  try{ await c.query("INSTALL spatial;LOAD spatial;"); }catch(e){}
  _db={d,c}; return _db;
}
async function q(sql){ const {c}=await db(); const r=await c.query(sql); return r.toArray().map(x=>x.toJSON()); }

let INDEX=null;
async function idx(){ if(!INDEX) INDEX=await getJSON("index.json"); return INDEX; }
function param(){ return new URL(location.href).searchParams.get("ds"); }
function go(h){ history.pushState({},"",h); route(); }
document.addEventListener("click",e=>{const a=e.target.closest("a[data-nav]");if(a){e.preventDefault();go(a.getAttribute("href"));}});
window.addEventListener("popstate",route);

async function route(){
  try{ const ds=param(); return ds ? detail(ds) : list(); }
  catch(e){ app.innerHTML=`<h1>Error</h1><p class="muted">${esc(e.message||e)}</p>`; }
}

async function list(){
  crumbs.innerHTML="";
  const ix=await idx();
  const nv=ix.datasets.filter(d=>d.type==="vector").length, nt=ix.datasets.filter(d=>d.type==="table").length, nr=ix.datasets.filter(d=>d.type==="raster").length;
  app.innerHTML=`<h1>${esc(CFG.title)}</h1>
   <p class="lead">${ix.datasets.length} datasets en formato Portolan v3 · vector ${nv} · tabla ${nt} · ráster ${nr}. Consulta directa en DuckDB (<code>read_parquet</code>) o Snowflake (Iceberg); preview en el navegador.</p>
   <div class="toolbar"><input class="search" id="q" placeholder="Buscar dataset…">
     <div class="filters"><button data-f="all" class="on">Todos</button>
       <button data-f="vector">Vector</button><button data-f="table">Tabla</button><button data-f="raster">Ráster</button></div></div>
   <div class="count-note" id="cn"></div>
   <table class="list"><tbody id="rows"></tbody></table>`;
  let filter="all",term="";
  const T={vector:["chip vector","vector"],table:["chip table","tabla"],raster:["chip raster","ráster"]};
  function render(){
    const ds=ix.datasets.filter(d=>(filter==="all"||d.type===filter)&&(!term||d.id.toLowerCase().includes(term)));
    document.getElementById("cn").textContent=`${ds.length} datasets`;
    document.getElementById("rows").innerHTML=ds.slice(0,500).map(d=>`
      <tr><td class="t"><span class="${T[d.type][0]}">${T[d.type][1]}</span></td>
      <td><a data-nav href="?ds=${encodeURIComponent(d.id)}">${esc(human(d.id))}</a>
      <div class="muted" style="font-size:12px">${esc(d.id)}</div></td></tr>`).join("")
      +(ds.length>500?`<tr><td></td><td class="muted">… y ${ds.length-500} más (afina la búsqueda)</td></tr>`:"");
  }
  document.getElementById("q").addEventListener("input",e=>{term=e.target.value.toLowerCase().trim();render();});
  document.querySelectorAll(".filters button").forEach(b=>b.addEventListener("click",()=>{
    document.querySelectorAll(".filters button").forEach(x=>x.classList.remove("on"));b.classList.add("on");filter=b.dataset.f;render();}));
  render();
}

async function detail(ds){
  crumbs.innerHTML=` / <a data-nav href="?">${esc(CFG.title)}</a> / ${esc(human(ds))}`;
  const ix=await idx();
  const e=ix.datasets.find(d=>d.id===ds)||{id:ds,type:"table",crs:"4326",parquet:`v3/${ds}/data/${ds}.parquet`,meta:`v3/${ds}/metadata/v1.metadata.json`};
  const type=e.type, epsg=String(e.crs||"4326"), isVec=type==="vector", isRas=type==="raster";
  const pqUrl=dataURL(e.parquet||`v3/${ds}/data/${ds}.parquet`);
  const metaUrl=dataURL(e.meta||`v3/${ds}/metadata/v1.metadata.json`);
  const cogUrl=e.cog?dataURL(e.cog):null;
  const tname=ds.replace(/[^a-z0-9_]/gi,"_");
  const s3 = s3of(pqUrl);
  const duckSnip = isRas
    ? `# ráster COG\nrio info '${cogUrl}'`
    : `${isVec?"INSTALL spatial;LOAD spatial;\n":""}INSTALL httpfs;LOAD httpfs;\nCREATE SECRET g (TYPE s3, PROVIDER config, KEY_ID '', SECRET '',\n  ENDPOINT 'storage.googleapis.com', URL_STYLE 'path', USE_SSL true, REGION 'auto');\nSELECT * FROM read_parquet('${s3}') LIMIT 100;`;
  const sfSnip = isRas ? `-- ráster: no aplica Iceberg`
    : `CREATE OR REPLACE ICEBERG TABLE ${tname}\n  EXTERNAL_VOLUME='<vol_misma_region>' CATALOG='<object_store_cat>'\n  METADATA_FILE_PATH='${(e.meta||`v3/${ds}/metadata/v1.metadata.json`)}';\n${isVec?`-- poda nativa por geom (SRID ${epsg}):\nSELECT * FROM ${tname}\nWHERE ST_INTERSECTS(geom, ST_GEOMFROMWKT('POLYGON((...))', ${epsg})) LIMIT 100;`:`SELECT * FROM ${tname} LIMIT 100;`}`;

  app.innerHTML=`<h1>${esc(human(ds))}</h1>
   <p class="lead"><span class="chip ${type}">${isVec?"vector":isRas?"ráster":"tabla"}</span>
     &nbsp;<span class="muted">${esc(ds)}</span></p>
   <div class="meta">
     <div class="kv"><div class="k">Tipo</div><div class="v">${isVec?"Vector":isRas?"Ráster (COG)":"Tabla"}</div></div>
     ${!isRas?`<div class="kv"><div class="k">Filas</div><div class="v" id="m-rows"><span class="spin"></span></div></div>
     <div class="kv"><div class="k">Columnas</div><div class="v" id="m-cols">—</div></div>`:""}
     ${isVec?`<div class="kv"><div class="k">CRS</div><div class="v">EPSG:${epsg}</div></div>`:""}
   </div>
   <div class="tabs" id="tabs">
     ${isVec?`<button data-t="map" class="on">Mapa</button>`:""}
     ${!isRas?`<button data-t="table" class="${isVec?"":"on"}">Datos</button>`:""}
     <button data-t="use" class="${isRas?"on":""}">Uso</button>
     <button data-t="fields">Campos</button></div>
   <div id="pane"></div>`;

  const panes={
    use:`<h2>Acceso</h2><p class="muted">Parquet: <a href="${pqUrl}">${esc(e.partitioned?"(particionado por provincia)":ds+".parquet")}</a>${cogUrl?` · COG: <a href="${cogUrl}">${esc(ds)}.tif</a>`:""}</p>
      <h2>DuckDB</h2><pre class="code"><button class="copy">copiar</button>${esc(duckSnip)}</pre>
      <h2>Snowflake (Iceberg externo)</h2><pre class="code"><button class="copy">copiar</button>${esc(sfSnip)}</pre>`,
    fields:`<h2>Campos</h2><div class="data-table-wrap"><table class="data fields" id="ft"><thead><tr><th>columna</th><th>tipo</th><th>descripción</th></tr></thead><tbody><tr><td colspan=3 class="muted">cargando…</td></tr></tbody></table></div>`,
    table:`<div class="data-table-wrap"><table class="data" id="dt"><thead></thead><tbody><tr><td class="muted"><span class="spin"></span> consultando parquet en el navegador…</td></tr></tbody></table></div><p class="count-note">Primeras 100 filas (DuckDB-WASM).</p>`,
    map: isRas?`<p class="muted">Ráster COG: ábrelo en QGIS o un visor COG: <a href="${cogUrl}">${esc(ds)}.tif</a></p>`
      :`<div id="map"></div><p class="count-note">Hasta 2.000 geometrías de muestra${epsg!=="4326"?", reproyectadas a 4326":""}.</p>`
  };
  const pane=document.getElementById("pane");
  let mapDone=false, tabDone=false, fieldsCache=null;
  const loadFields=async()=>{ if(fieldsCache)return fieldsCache; try{const m=await getJSON(metaUrl);const sc=m.schemas?m.schemas.find(s=>s["schema-id"]===m["current-schema-id"]):m.schema;fieldsCache=sc.fields;}catch(e){fieldsCache=[];}return fieldsCache;};
  async function afterTab(t){
    if(t==="fields"){const f=await loadFields();document.querySelector("#ft tbody").innerHTML=(f.length?f:[]).map(x=>`<tr><td class="fn">${esc(x.name)}</td><td class="muted">${esc(typeof x.type==="object"?"struct":x.type)}</td><td>${esc(x.doc||"")}</td></tr>`).join("")||`<tr><td colspan=3 class="muted">sin esquema</td></tr>`;}
    if(t==="table"&&!tabDone){tabDone=true;try{
      const f=await loadFields();const cols=f.map(x=>x.name).filter(n=>n!=="geom");const sel=cols.length?cols.map(n=>`"${n}"`).join(","):"* EXCLUDE(geom)";
      const rows=await q(`SELECT ${sel} FROM read_parquet('${pqUrl}') LIMIT 100`);
      const head=cols.length?cols:Object.keys(rows[0]||{});
      document.querySelector("#dt thead").innerHTML=`<tr>${head.map(h=>`<th>${esc(h)}</th>`).join("")}</tr>`;
      document.querySelector("#dt tbody").innerHTML=rows.map(r=>`<tr>${head.map(h=>`<td>${esc(r[h])}</td>`).join("")}</tr>`).join("")||`<tr><td class="muted">sin filas</td></tr>`;
    }catch(err){document.querySelector("#dt tbody").innerHTML=`<tr><td class="muted">No se pudo leer: ${esc(err.message||err)}</td></tr>`;}}
    if(t==="map"&&!mapDone&&!isRas){mapDone=true;await loadMap();}
  }
  const show=t=>{document.querySelectorAll("#tabs button").forEach(b=>b.classList.toggle("on",b.dataset.t===t));pane.innerHTML=panes[t]||"";afterTab(t);};
  document.querySelectorAll("#tabs button").forEach(b=>b.addEventListener("click",()=>show(b.dataset.t)));
  pane.addEventListener("click",ev=>{const cp=ev.target.closest(".copy");if(cp){navigator.clipboard.writeText(cp.parentElement.innerText.replace(/^copiar\n?/,""));cp.textContent="¡copiado!";setTimeout(()=>cp.textContent="copiar",1200);}});

  if(!isRas){(async()=>{try{const f=await loadFields();document.getElementById("m-cols").textContent=(f.length||"—");const r=await q(`SELECT count(*) n FROM read_parquet('${pqUrl}')`);document.getElementById("m-rows").textContent=Number(r[0].n).toLocaleString("es");}catch(e){document.getElementById("m-rows").textContent="—";}})();}

  async function loadMap(){
    const map=new maplibregl.Map({container:"map",style:{version:8,sources:{c:{type:"raster",tiles:["https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png"],tileSize:256,attribution:"© OpenStreetMap, © CARTO"}},layers:[{id:"c",type:"raster",source:"c"}]},center:[-3.7,40.42],zoom:7});
    await new Promise(r=>map.on("load",r));
    try{
      const gx=epsg==="4326"?"geom":`ST_Transform(geom,'EPSG:${epsg}','EPSG:4326')`;
      const rows=await q(`SELECT ST_AsGeoJSON(${gx}) g FROM read_parquet('${pqUrl}') WHERE geom IS NOT NULL LIMIT 2000`);
      const fc={type:"FeatureCollection",features:rows.map(r=>({type:"Feature",geometry:JSON.parse(r.g)}))};
      map.addSource("d",{type:"geojson",data:fc});
      map.addLayer({id:"fl",type:"fill",source:"d",filter:["==","$type","Polygon"],paint:{"fill-color":"#2d6cdf","fill-opacity":.22,"fill-outline-color":"#2d6cdf"}});
      map.addLayer({id:"ln",type:"line",source:"d",filter:["==","$type","LineString"],paint:{"line-color":"#2d6cdf","line-width":1.5}});
      map.addLayer({id:"pt",type:"circle",source:"d",filter:["==","$type","Point"],paint:{"circle-radius":3.5,"circle-color":"#2d6cdf","circle-opacity":.7}});
      const b=new maplibregl.LngLatBounds();const flat=s=>{if(typeof s[0]==="number")b.extend(s);else s.forEach(flat);};
      fc.features.forEach(f=>{const co=f.geometry&&f.geometry.coordinates;if(co)try{flat(co);}catch(e){}});
      if(!b.isEmpty())map.fitBounds(b,{padding:30,maxZoom:14,duration:0});
    }catch(err){document.getElementById("map").insertAdjacentHTML("beforeend",`<div style="padding:14px" class="muted">No se pudo cargar la geometría (${esc(err.message||err)}). EPSG:${epsg}.</div>`);}
  }
  show(isVec?"map":isRas?"use":"table");
}
route();
