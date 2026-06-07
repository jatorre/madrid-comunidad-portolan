import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

const BASE = "https://storage.googleapis.com/carto-portolan-madrid";
const CAT_CRS = {"comunidad-madrid":"25830","madrid-city":"25830","madrid-opendata":"4326"};
const app = document.getElementById("app");
const crumbs = document.getElementById("crumbs");
const esc = s => String(s==null?"":s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const human = s => s.replace(/__tab$/," (tabla)").replace(/_/g," ").replace(/\b\w/g,c=>c.toUpperCase());
const getJSON = async u => (await fetch(u)).json();

let _db=null;
async function db(){
  if(_db) return _db;
  const b = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const w = await duckdb.createWorker(b.mainWorker);
  const d = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), w);
  await d.instantiate(b.mainModule, b.pthreadWorker);
  const c = await d.connect();
  try{ await c.query("INSTALL spatial;LOAD spatial;"); }catch(e){}
  _db = {d,c}; return _db;
}
async function q(sql){ const {c}=await db(); const r=await c.query(sql); return r.toArray().map(row=>row.toJSON()); }

function params(){ const u=new URL(location.href); return {c:u.searchParams.get("c"), ds:u.searchParams.get("ds")}; }
function go(href){ history.pushState({}, "", href); route(); }
document.addEventListener("click",e=>{const a=e.target.closest("a[data-nav]");if(a){e.preventDefault();go(a.getAttribute("href"));}});
window.addEventListener("popstate", route);

async function route(){
  const {c,ds}=params();
  try{ if(c&&ds) return await detail(c,ds); if(c) return await catalog(c); return await home(); }
  catch(e){ app.innerHTML=`<h1>Error</h1><p class="muted">${esc(e.message||e)}</p>`; }
}

async function home(){
  crumbs.innerHTML="";
  const m=await getJSON("catalogs.json");
  app.innerHTML=`<h1>Catálogos de datos</h1>
  <p class="lead">Datos abiertos re-expuestos en formato <b>Portolan v3</b>: un único juego de ficheros consultable en DuckDB (<code>read_parquet</code>) y Snowflake (Iceberg), con preview en el navegador.</p>
  <div class="grid">${m.catalogs.map(c=>`
    <a class="card" data-nav href="?c=${encodeURIComponent(c.id)}">
      <h3>${esc(c.title)}</h3>
      <p>${c.n_vector+c.n_table+c.n_raster} datasets</p>
      <div class="counts">
        ${c.n_vector?`<span class="chip vector">${c.n_vector} vector</span>`:""}
        ${c.n_table?`<span class="chip table">${c.n_table} tabla</span>`:""}
        ${c.n_raster?`<span class="chip raster">${c.n_raster} ráster</span>`:""}
      </div></a>`).join("")}</div>`;
}

async function catalog(c){
  crumbs.innerHTML=` / <a data-nav href="?c=${encodeURIComponent(c)}">${esc(human(c))}</a>`;
  const idx=await getJSON(`${encodeURIComponent(c)}.index.json`);
  app.innerHTML=`<h1>${esc(human(c))}</h1>
   <p class="lead">${idx.datasets.length} datasets · vector ${idx.n_vector} · tabla ${idx.n_table} · ráster ${idx.n_raster}</p>
   <div class="toolbar">
     <input class="search" id="q" placeholder="Buscar dataset…">
     <div class="filters">
       <button data-f="all" class="on">Todos</button>
       <button data-f="vector">Vector</button>
       <button data-f="table">Tabla</button>
       <button data-f="raster">Ráster</button>
     </div>
   </div>
   <div class="count-note" id="cn"></div>
   <table class="list"><tbody id="rows"></tbody></table>`;
  let filter="all", term="";
  const TYPE={vector:["chip vector","vector"],table:["chip table","tabla"],raster:["chip raster","ráster"]};
  function render(){
    const ds=idx.datasets.filter(d=>(filter==="all"||d.type===filter)&&(!term||d.id.toLowerCase().includes(term)));
    document.getElementById("cn").textContent=`${ds.length} datasets`;
    document.getElementById("rows").innerHTML=ds.slice(0,500).map(d=>`
      <tr><td class="t"><span class="${TYPE[d.type][0]}">${TYPE[d.type][1]}</span></td>
      <td><a data-nav href="?c=${encodeURIComponent(c)}&ds=${encodeURIComponent(d.id)}">${esc(human(d.id))}</a>
      <div class="muted" style="font-size:12px">${esc(d.id)}</div></td></tr>`).join("")
      + (ds.length>500?`<tr><td></td><td class="muted">… y ${ds.length-500} más (afina la búsqueda)</td></tr>`:"");
  }
  document.getElementById("q").addEventListener("input",e=>{term=e.target.value.toLowerCase().trim();render();});
  document.querySelectorAll(".filters button").forEach(b=>b.addEventListener("click",()=>{
    document.querySelectorAll(".filters button").forEach(x=>x.classList.remove("on"));b.classList.add("on");filter=b.dataset.f;render();}));
  render();
}

async function detail(c,ds){
  crumbs.innerHTML=` / <a data-nav href="?c=${encodeURIComponent(c)}">${esc(human(c))}</a> / ${esc(human(ds))}`;
  const idx=await getJSON(`${encodeURIComponent(c)}.index.json`);
  const entry=idx.datasets.find(d=>d.id===ds) || {id:ds,type:"table"};
  const type=entry.type, epsg=CAT_CRS[c]||"4326";
  const pq=`${BASE}/${c}/v3/${ds}/data/${ds}.parquet`;
  const meta=`${BASE}/${c}/v3/${ds}/metadata/v1.metadata.json`;
  const cog=`${BASE}/${c}/data/cog/${ds}.tif`;
  const isVec=type==="vector", isRas=type==="raster";

  // snippets
  const s3url = `s3://carto-portolan-madrid/${c}/v3/${ds}/data/${ds}.parquet`;
  const duckSnip = isRas
    ? `-- ráster COG: ábrelo con GDAL/rasterio o un visor COG\nrio info '${cog}'   # rasterio\n# o en DuckDB con la extensión spatial: ST_Read('${cog}')`
    : `${isVec?"INSTALL spatial;LOAD spatial;\n":""}INSTALL httpfs;LOAD httpfs;\nCREATE SECRET g (TYPE s3, PROVIDER config, KEY_ID '', SECRET '',\n  ENDPOINT 'storage.googleapis.com', URL_STYLE 'path', USE_SSL true, REGION 'auto');\nSELECT * FROM read_parquet('${s3url}') LIMIT 100;\n-- o por URL directa (sin secret):\n-- SELECT * FROM read_parquet('${pq}') LIMIT 100;`;
  const sfSnip = isRas ? `-- ráster: no aplica Iceberg`
    : `CREATE OR REPLACE ICEBERG TABLE ${ds.replace(/[^a-z0-9_]/gi,"_")}\n  EXTERNAL_VOLUME='<vol_misma_region>' CATALOG='<object_store_cat>'\n  METADATA_FILE_PATH='${c}/v3/${ds}/metadata/v1.metadata.json';\n${isVec?`-- poda nativa por geom (SRID ${epsg}):\nSELECT * FROM ${ds.replace(/[^a-z0-9_]/gi,"_")}\nWHERE ST_INTERSECTS(geom, ST_GEOMFROMWKT('POLYGON((...))', ${epsg})) LIMIT 100;`:`SELECT * FROM ${ds.replace(/[^a-z0-9_]/gi,"_")} LIMIT 100;`}`;

  app.innerHTML=`<h1>${esc(human(ds))}</h1>
    <p class="lead"><span class="chip ${type}">${type==="vector"?"vector":type==="raster"?"ráster":"tabla"}</span>
      &nbsp;<span class="muted">${esc(c)} · ${esc(ds)}</span></p>
    <div class="meta" id="meta">
      <div class="kv"><div class="k">Tipo</div><div class="v">${type==="vector"?"Vector":type==="raster"?"Ráster (COG)":"Tabla"}</div></div>
      ${!isRas?`<div class="kv"><div class="k">Filas</div><div class="v" id="m-rows"><span class="spin"></span></div></div>
      <div class="kv"><div class="k">Columnas</div><div class="v" id="m-cols">—</div></div>`:""}
      ${isVec?`<div class="kv"><div class="k">CRS</div><div class="v">EPSG:${epsg}</div></div>`:""}
    </div>
    <div class="tabs" id="tabs">
      ${isVec?`<button data-t="map" class="on">Mapa</button>`:""}
      ${!isRas?`<button data-t="table" class="${isVec?"":"on"}">Datos</button>`:""}
      <button data-t="use" class="${isRas?"on":""}">Uso</button>
      <button data-t="fields" class="">Campos</button>
    </div>
    <div id="pane"></div>`;

  const panes={};
  panes.use=`<h2>Acceso</h2>
    <p class="muted">Parquet directo: <a href="${pq}">${esc(ds)}.parquet</a> ${isRas?`· COG: <a href="${cog}">${esc(ds)}.tif</a>`:""}</p>
    <h2>DuckDB</h2><pre class="code"><button class="copy">copiar</button>${esc(duckSnip)}</pre>
    <h2>Snowflake (Iceberg externo)</h2><pre class="code"><button class="copy">copiar</button>${esc(sfSnip)}</pre>`;
  panes.fields=`<h2>Campos</h2><div class="data-table-wrap"><table class="data fields" id="fieldtab"><thead><tr><th>columna</th><th>tipo</th><th>descripción</th></tr></thead><tbody><tr><td colspan="3" class="muted">cargando…</td></tr></tbody></table></div>`;
  panes.table=`<div class="data-table-wrap"><table class="data" id="datatab"><thead></thead><tbody><tr><td class="muted"><span class="spin"></span> consultando parquet…</td></tr></tbody></table></div><p class="count-note">Primeras 100 filas, leídas en el navegador con DuckDB-WASM.</p>`;
  panes.map=`<div id="map"></div><p class="count-note">Hasta 2.000 geometrías de muestra, reproyectadas a 4326 para el mapa.</p>`;
  if(isRas) panes.map=`<p class="muted">Ráster COG. Ábrelo en QGIS o un visor COG: <a href="${cog}">${esc(ds)}.tif</a></p>`;

  const pane=document.getElementById("pane");
  const showTab=t=>{document.querySelectorAll("#tabs button").forEach(b=>b.classList.toggle("on",b.dataset.t===t));pane.innerHTML=panes[t]||"";afterTab(t);};
  document.querySelectorAll("#tabs button").forEach(b=>b.addEventListener("click",()=>showTab(b.dataset.t)));
  pane.addEventListener("click",e=>{const cp=e.target.closest(".copy");if(cp){navigator.clipboard.writeText(cp.parentElement.innerText.replace(/^copiar/,""));cp.textContent="¡copiado!";setTimeout(()=>cp.textContent="copiar",1200);}});

  let _fields=null, _mapDone=false, _tableDone=false;
  async function loadFields(){
    if(_fields) return _fields;
    try{ const m=await getJSON(meta); const sch=(m.schemas?m.schemas.find(s=>s["schema-id"]===m["current-schema-id"]):m.schema); _fields=sch.fields; }catch(e){ _fields=[]; }
    return _fields;
  }
  async function afterTab(t){
    if(t==="fields"){ const f=await loadFields();
      document.querySelector("#fieldtab tbody").innerHTML=(f.length?f:[]).map(x=>`<tr><td class="fn">${esc(x.name)}</td><td class="muted">${esc(typeof x.type==="object"?"struct":x.type)}</td><td>${esc(x.doc||"")}</td></tr>`).join("")||`<tr><td colspan=3 class="muted">sin esquema</td></tr>`;
    }
    if(t==="table"&&!_tableDone){ _tableDone=true; await loadTable(); }
    if(t==="map"&&!_mapDone){ _mapDone=true; await loadMap(); }
  }
  // meta counts (rows/cols) for non-raster
  if(!isRas){ (async()=>{ try{
      const f=await loadFields(); document.getElementById("m-cols").textContent=f.length||"—";
      const r=await q(`SELECT count(*) n FROM read_parquet('${pq}')`); document.getElementById("m-rows").textContent=Number(r[0].n).toLocaleString("es");
    }catch(e){ document.getElementById("m-rows").textContent="—"; } })(); }

  async function loadTable(){
    try{
      const f=await loadFields(); const cols=f.map(x=>x.name).filter(n=>n!=="geom");
      const sel=cols.map(n=>`"${n}"`).join(",")||"*";
      const rows=await q(`SELECT ${sel} FROM read_parquet('${pq}') LIMIT 100`);
      const head=cols.length?cols:Object.keys(rows[0]||{});
      document.querySelector("#datatab thead").innerHTML=`<tr>${head.map(h=>`<th>${esc(h)}</th>`).join("")}</tr>`;
      document.querySelector("#datatab tbody").innerHTML=rows.map(r=>`<tr>${head.map(h=>`<td>${esc(r[h])}</td>`).join("")}</tr>`).join("")||`<tr><td class="muted">sin filas</td></tr>`;
    }catch(e){ document.querySelector("#datatab tbody").innerHTML=`<tr><td class="muted">No se pudo leer: ${esc(e.message||e)}</td></tr>`; }
  }
  async function loadMap(){
    const map=new maplibregl.Map({container:"map",style:{version:8,sources:{carto:{type:"raster",tiles:["https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png"],tileSize:256,attribution:"© OpenStreetMap, © CARTO"}},layers:[{id:"carto",type:"raster",source:"carto"}]},center:[-3.7,40.42],zoom:8});
    await new Promise(r=>map.on("load",r));
    try{
      const gx = epsg==="4326" ? "geom" : `ST_Transform(geom,'EPSG:${epsg}','EPSG:4326')`;
      const rows=await q(`SELECT ST_AsGeoJSON(${gx}) g FROM read_parquet('${pq}') WHERE geom IS NOT NULL LIMIT 2000`);
      const fc={type:"FeatureCollection",features:rows.map(r=>({type:"Feature",geometry:JSON.parse(r.g)}))};
      map.addSource("d",{type:"geojson",data:fc});
      map.addLayer({id:"fill",type:"fill",source:"d",filter:["==","$type","Polygon"],paint:{"fill-color":"#2d6cdf","fill-opacity":.25,"fill-outline-color":"#2d6cdf"}});
      map.addLayer({id:"line",type:"line",source:"d",filter:["==","$type","LineString"],paint:{"line-color":"#2d6cdf","line-width":1.5}});
      map.addLayer({id:"pt",type:"circle",source:"d",filter:["==","$type","Point"],paint:{"circle-radius":3.5,"circle-color":"#2d6cdf","circle-opacity":.7}});
      // fit bounds
      let b=new maplibregl.LngLatBounds();
      const walk=g=>{if(!g)return;if(g.type==="Point")b.extend(g.coordinates);else if(g.coordinates)JSON.stringify(g.coordinates).match(/-?\d+\.\d+/g);};
      fc.features.forEach(f=>{const co=f.geometry&&f.geometry.coordinates;const flat=s=>{if(typeof s[0]==="number")b.extend(s);else s.forEach(flat);};if(co)try{flat(co);}catch(e){}});
      if(!b.isEmpty()) map.fitBounds(b,{padding:30,maxZoom:14,duration:0});
    }catch(e){ document.getElementById("map").insertAdjacentHTML("beforeend",`<div style="padding:14px" class="muted">No se pudo cargar la geometría (${esc(e.message||e)}). CRS EPSG:${epsg}.</div>`); }
  }
  showTab(isVec?"map":isRas?"use":"table");
}

route();
