import os, sqlite3, secrets, hashlib, jwt, schedule, time, threading, random, requests
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()
app = FastAPI(title="NexusStream API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DB_PATH = os.getenv("DB_PATH", "/data/nexusstream.db" if os.path.exists("/data") else "nexusstream.db")
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

SECRET_KEY = os.getenv("JWT_SECRET", secrets.token_urlsafe(32))
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "nexus-admin-2025")
YOUTUBE_KEY = os.getenv("YOUTUBE_API_KEY", "")
RESEND_KEY = os.getenv("RESEND_API_KEY", "")
ALGORITHM = "HS256"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript('''
    CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, phone TEXT UNIQUE, password_hash TEXT, trial_start TEXT, tokens INTEGER DEFAULT 5, created_at TEXT);
    CREATE TABLE IF NOT EXISTS token_ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount INTEGER, reason TEXT, created_at TEXT);
    CREATE TABLE IF NOT EXISTS movies (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, year INTEGER, category TEXT, content_type TEXT, license_type TEXT, status TEXT DEFAULT 'approved', youtube_id TEXT, url TEXT, thumbnail TEXT, duration TEXT, direct_download TEXT, added_date TEXT);
    CREATE TABLE IF NOT EXISTS ads (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, creative_url TEXT, target_category TEXT, status TEXT DEFAULT 'active', interaction_type TEXT DEFAULT 'hold', duration_sec INTEGER DEFAULT 5, token_reward INTEGER DEFAULT 3, views INTEGER DEFAULT 0, created_at TEXT);
    CREATE TABLE IF NOT EXISTS ad_interactions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, ad_id INTEGER, completed BOOLEAN, timestamp TEXT);
    CREATE TABLE IF NOT EXISTS platform_treasury (id INTEGER PRIMARY KEY CHECK (id=1), balance REAL DEFAULT 0.0, currency TEXT DEFAULT 'USD', last_updated TEXT);
    CREATE TABLE IF NOT EXISTS advertiser_leads (id INTEGER PRIMARY KEY AUTOINCREMENT, company TEXT, domain TEXT, contact_email TEXT, industry TEXT, score INTEGER, status TEXT DEFAULT 'new', outreach_sent BOOLEAN DEFAULT 0, created_at TEXT);
    CREATE TABLE IF NOT EXISTS agent_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT, status TEXT, details TEXT, timestamp TEXT);
    ''')
    try: c.execute("INSERT OR IGNORE INTO platform_treasury VALUES (1, 0.0, 'USD', ?)", (datetime.now().isoformat(),))
    except: pass
    conn.commit(); conn.close()

def seed_data():
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    if c.execute("SELECT COUNT(*) FROM movies").fetchone()[0] == 0:
        now = datetime.now().isoformat()
        c.executemany('INSERT INTO movies (title,year,category,content_type,license_type,status,youtube_id,url,thumbnail,duration,direct_download,added_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', [
            ("Night of the Living Dead", 1968, "Horror", "full_movie", "Public Domain", "approved", "", "https://archive.org/details/night_of_the_living_dead", "https://archive.org/download/night_of_the_living_dead/night_of_the_living_dead.jpg", "PT1h36m", "https://archive.org/download/night_of_the_living_dead/night_of_the_living_dead.mp4", now),
            ("Big Buck Bunny", 2008, "Animation", "full_movie", "CC BY", "approved", "aqz-KE-bpKQ", "https://www.youtube.com/watch?v=aqz-KE-bpKQ", "https://i.ytimg.com/vi/aqz-KE-bpKQ/hqdefault.jpg", "PT10m", "stream_only", now),
            ("Dune: Part Two", 2024, "Sci-Fi", "trailer", "Official", "approved", "Way9Dexny3w", "https://www.youtube.com/watch?v=Way9Dexny3w", "https://i.ytimg.com/vi/Way9Dexny3w/hqdefault.jpg", "PT2m", "stream_only", now)
        ])
        c.executemany('INSERT INTO ads (title,creative_url,target_category,duration_sec,token_reward,created_at) VALUES (?,?,?,?,?,?)', [
            ("Nexus Cloud", "https://images.unsplash.com/photo-1550751827-4bd374c3f58b?w=600", "tech", 5, 3, now),
            ("StreamSafe VPN", "https://images.unsplash.com/photo-1614064641938-3bbee52942c7?w=600", "privacy", 5, 3, now)
        ])
    conn.commit(); conn.close()

def log(task, status, details):
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("INSERT INTO agent_logs (task,status,details,timestamp) VALUES (?,?,?,?)", (task, status, str(details)[:500], datetime.now().isoformat()))
    conn.commit(); conn.close()

def verify_token(auth_header: str = Header(None)):
    if not auth_header or not auth_header.startswith("Bearer "): raise HTTPException(401, "Missing token")
    try: return jwt.decode(auth_header.split(" ")[1], SECRET_KEY, algorithms=[ALGORITHM])["user_id"]
    except: raise HTTPException(401, "Invalid token")

def hash_pwd(p): return hashlib.sha256(p.encode()).hexdigest()

class Agent:
    @staticmethod
    def fetch_youtube():
        if not YOUTUBE_KEY: return 0
        try:
            res = requests.get("https://www.googleapis.com/youtube/v3/search", params={"part":"snippet","q":"creative commons full movie","type":"video","videoDuration":"long","maxResults":5,"key":YOUTUBE_KEY})
            data = res.json(); conn=sqlite3.connect(DB_PATH); c=conn.cursor(); added=0
            for item in data.get("items", []):
                vid=item["id"]["videoId"]
                if c.execute("SELECT id FROM movies WHERE youtube_id=?",(vid,)).fetchone(): continue
                c.execute('INSERT INTO movies (title,year,category,content_type,license_type,status,youtube_id,url,thumbnail,duration,direct_download,added_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(item["snippet"]["title"],2023,"Indie","full_movie","Creative Commons","approved",vid,f"https://www.youtube.com/watch?v={vid}",item["snippet"]["thumbnails"]["high"]["url"],"PT90m","stream_only",datetime.now().isoformat())); added+=1
            conn.commit(); conn.close(); log("youtube","success",f"+{added} fetched"); return added
        except Exception as e: log("youtube","error",str(e)); return 0

    @staticmethod
    def find_advertisers():
        mock=[{"company":"CloudNest","domain":"cloudnest.io","contact":"partners@cloudnest.io","industry":"Cloud","score":92},{"company":"PixelForge","domain":"pixelforge.dev","contact":"biz@pixelforge.dev","industry":"Design","score":85}]
        conn=sqlite3.connect(DB_PATH); c=conn.cursor(); added=0
        for l in mock:
            if not c.execute("SELECT id FROM advertiser_leads WHERE domain=?",(l["domain"],)).fetchone():
                c.execute('INSERT INTO advertiser_leads (company,domain,contact_email,industry,score,created_at) VALUES (?,?,?,?,?,?)',(l["company"],l["domain"],l["contact"],l["industry"],l["score"],datetime.now().isoformat())); added+=1
        conn.commit(); conn.close(); log("agent","leads",f"+{added} discovered"); return added

    @staticmethod
    def send_outreach(lead_id):
        conn=sqlite3.connect(DB_PATH); c=conn.cursor()
        lead=c.execute("SELECT company,contact_email FROM advertiser_leads WHERE id=?",(lead_id,)).fetchone()
        if not lead: conn.close(); return {"error":"not_found"}
        if RESEND_KEY:
            try:
                requests.post("https://api.resend.com/emails", headers={"Authorization":f"Bearer {RESEND_KEY}"}, json={"from":"NexusAgent <onboarding@resend.dev>","to":lead[1],"subject":"Partnership: NexusStream","html":"<p>Hi team,<br>NexusStream offers compliant, high-engagement ad syncs. Interested in a trial?</p>"})
                c.execute("UPDATE advertiser_leads SET outreach_sent=1,status='contacted' WHERE id=?",(lead_id,)); log("outreach","sent",f"Email to {lead[1]}")
            except Exception as e: log("outreach","failed",str(e))
        else:
            c.execute("UPDATE advertiser_leads SET outreach_sent=1,status='contacted' WHERE id=?",(lead_id,)); log("outreach","queued",f"Would email {lead[1]} (add RESEND_API_KEY)")
        conn.commit(); conn.close(); return {"status":"queued","company":lead[0]}

    @staticmethod
    def run_cycle():
        Agent.fetch_youtube(); Agent.find_advertisers()
        conn=sqlite3.connect(DB_PATH); high=conn.execute("SELECT id FROM advertiser_leads WHERE score>=85 AND outreach_sent=0").fetchall()
        for row in high: Agent.send_outreach(row[0])
        conn.close(); log("agent","cycle","Daily sync complete")

def scheduler():
    schedule.every().day.at("08:00").do(Agent.run_cycle)
    while True: schedule.run_pending(); time.sleep(60)
threading.Thread(target=scheduler, daemon=True).start()

@app.on_event("startup")
def startup(): init_db(); seed_data()

@app.post("/api/auth/register")
def register(phone:str, password:str, email:str=None):
    if len(password)<4: raise HTTPException(400,"Password too short")
    conn=sqlite3.connect(DB_PATH); c=conn.cursor()
    try:
        c.execute("INSERT INTO users (email,phone,password_hash,trial_start,created_at) VALUES (?,?,?,?,?)",(email,phone,hash_pwd(password),datetime.now().isoformat(),datetime.now().isoformat()))
        uid=c.lastrowid; c.execute("INSERT INTO token_ledger (user_id,amount,reason,created_at) VALUES (?,?,?,?)",(uid,5,"Welcome",datetime.now().isoformat()))
        conn.commit(); token=jwt.encode({"user_id":uid,"exp":datetime.utcnow()+timedelta(days=30)},SECRET_KEY,ALGORITHM)
        return {"token":f"Bearer {token}","tokens":5,"trial":True}
    except sqlite3.IntegrityError: raise HTTPException(400,"Phone exists")
    finally: conn.close()

@app.post("/api/auth/login")
def login(phone:str, password:str):
    conn=sqlite3.connect(DB_PATH); u=conn.execute("SELECT id,password_hash,trial_start,tokens FROM users WHERE phone=?",(phone,)).fetchone(); conn.close()
    if not u or u[1]!=hash_pwd(password): raise HTTPException(401,"Invalid")
    return {"token":f"Bearer {jwt.encode({'user_id':u[0],'exp':datetime.utcnow()+timedelta(days=30)},SECRET_KEY,ALGORITHM)}","tokens":u[3],"trial":(datetime.now()-datetime.fromisoformat(u[2])).days<30}

@app.get("/api/user/status")
def status(uid:int=Depends(verify_token)):
    conn=sqlite3.connect(DB_PATH); r=conn.execute("SELECT trial_start,tokens FROM users WHERE id=?",(uid,)).fetchone(); conn.close()
    if not r: raise HTTPException(404,"User not found")
    return {"tokens":r[1],"trial_active":(datetime.now()-datetime.fromisoformat(r[0])).days<30,"days_left":max(0,30-(datetime.now()-datetime.fromisoformat(r[0])).days)}

@app.post("/api/user/claim-daily")
def claim(uid:int=Depends(verify_token)):
    conn=sqlite3.connect(DB_PATH); today=datetime.now().strftime("%Y-%m-%d")
    if conn.execute("SELECT date(created_at) FROM token_ledger WHERE user_id=? AND reason='Daily' ORDER BY created_at DESC LIMIT 1",(uid,)).fetchone(): conn.close(); raise HTTPException(400,"Already claimed")
    conn.execute("UPDATE users SET tokens=tokens+2 WHERE id=?",(uid,)); conn.execute("INSERT INTO token_ledger (user_id,amount,reason,created_at) VALUES (?,?,?,?)",(uid,2,"Daily",datetime.now().isoformat()))
    conn.commit(); conn.close(); return {"added":2,"message":"🌅 +2 Tokens"}

@app.get("/api/movies/full")
def get_full(): return [dict(r) for r in sqlite3.connect(DB_PATH).execute("SELECT * FROM movies WHERE content_type='full_movie' AND status='approved' ORDER BY added_date DESC LIMIT 20").fetchall()]
@app.get("/api/movies/trailers")
def get_trailers(): return [dict(r) for r in sqlite3.connect(DB_PATH).execute("SELECT * FROM movies WHERE content_type='trailer' AND status='approved' ORDER BY added_date DESC LIMIT 20").fetchall()]
@app.post("/api/movies/download/{mid}")
def dl(mid:int, uid:int=Depends(verify_token)):
    conn=sqlite3.connect(DB_PATH); u=conn.execute("SELECT trial_start,tokens FROM users WHERE id=?",(uid,)).fetchone()
    m=conn.execute("SELECT direct_download,license_type FROM movies WHERE id=?",(mid,)).fetchone()
    if not m or m[0]=="stream_only": conn.close(); raise HTTPException(403,"Streaming only")
    trial=(datetime.now()-datetime.fromisoformat(u[0])).days<30
    if not trial and u[1]<1: conn.close(); raise HTTPException(402,"Need 1 Token")
    if not trial:
        conn.execute("UPDATE users SET tokens=tokens-1 WHERE id=?",(uid,))
        conn.execute("INSERT INTO token_ledger (user_id,amount,reason,created_at) VALUES (?,?,?,?)",(uid,-1,"Download",datetime.now().isoformat()))
    conn.commit(); conn.close()
    return {"url":m[0],"note":m[1]}

@app.get("/api/ads/next")
def next_ad(cat:str="all"):
    conn=sqlite3.connect(DB_PATH); ad=conn.execute("SELECT id,title,creative_url,duration_sec,token_reward FROM ads WHERE status='active' ORDER BY views ASC LIMIT 1").fetchone()
    if ad: conn.execute("UPDATE ads SET views=views+1 WHERE id=?",(ad[0],)); conn.commit(); conn.close(); return {"ad_id":ad[0],"title":ad[1],"url":ad[2],"duration":ad[3],"reward":ad[4]}
    conn.close(); return {"skip":True}

@app.post("/api/ads/complete")
def complete(ad_id:int, uid:int=Depends(verify_token)):
    conn=sqlite3.connect(DB_PATH); conn.execute("INSERT INTO ad_interactions (user_id,ad_id,completed,timestamp) VALUES (?,?,?,?)",(uid,ad_id,True,datetime.now().isoformat()))
    r=conn.execute("SELECT token_reward FROM ads WHERE id=?",(ad_id,)).fetchone()
    if r and r[0]>0: conn.execute("UPDATE users SET tokens=tokens+? WHERE id=?",(r[0],uid)); conn.execute("INSERT INTO token_ledger (user_id,amount,reason,created_at) VALUES (?,?,?,?)",(uid,r[0],f"Ad #{ad_id}",datetime.now().isoformat()))
    conn.commit(); conn.close(); return {"tokens":r[0] if r else 0,"success":True}

@app.post("/api/agent/run")
def trigger(secret:str=Header(None)):
    if secret!=ADMIN_SECRET: raise HTTPException(403,"Unauthorized")
    Agent.run_cycle(); return {"status":"complete"}
@app.get("/api/agent/leads")
def leads(secret:str=Header(None)):
    if secret!=ADMIN_SECRET: raise HTTPException(403,"Unauthorized")
    return [dict(r) for r in sqlite3.connect(DB_PATH).execute("SELECT * FROM advertiser_leads ORDER BY score DESC").fetchall()]
@app.get("/api/agent/logs")
def logs(secret:str=Header(None)):
    if secret!=ADMIN_SECRET: raise HTTPException(403,"Unauthorized")
    return [dict(r) for r in sqlite3.connect(DB_PATH).execute("SELECT * FROM agent_logs ORDER BY timestamp DESC LIMIT 20").fetchall()]

@app.get("/api/admin/dashboard")
def dash(secret:str=Header(None)):
    if secret!=ADMIN_SECRET: raise HTTPException(403,"Unauthorized")
    conn=sqlite3.connect(DB_PATH)
    return {"ads":[dict(r) for r in conn.execute("SELECT id,title,target_category,status,views FROM ads ORDER BY created_at DESC").fetchall()], "users":conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]}

@app.post("/api/admin/ads/add")
def add_ad(secret:str=Header(None), title:str="", url:str="", category:str="all", reward:int=3):
    if secret!=ADMIN_SECRET: raise HTTPException(403,"Unauthorized")
    sqlite3.connect(DB_PATH).execute("INSERT INTO ads (title,creative_url,target_category,token_reward,created_at) VALUES (?,?,?,?,?)",(title,url,category,reward,datetime.now().isoformat()))
    return {"status":"added"}

@app.post("/api/admin/ads/toggle/{aid}")
def toggle(aid:int, secret:str=Header(None)):
    if secret!=ADMIN_SECRET: raise HTTPException(403,"Unauthorized")
    sqlite3.connect(DB_PATH).execute("UPDATE ads SET status=CASE WHEN status='active' THEN 'inactive' ELSE 'active' END WHERE id=?",(aid,))
    return {"status":"toggled"}

if __name__ == "__main__":
    import uvicorn; port=int(os.getenv("PORT", 8000)); uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")