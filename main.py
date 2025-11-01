import smtplib
import ssl
import os
import json
import time
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from uuid import uuid4
from contextlib import asynccontextmanager
import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
from dotenv import load_dotenv 
from database import get_database_session, create_tables, test_connection
from models import Company, TicketRequest, RequestLog, EmailLog, SystemConfig, CompanyUser, ReportSchedule
from schemas import *

load_dotenv() 
# ============================
# CONFIGURATION CLASS
# ============================
class Config:
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
    SMTP_USER = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
    UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/tmp")
    MAX_DATE_RANGE_DAYS = int(os.getenv("MAX_DATE_RANGE_DAYS", "365"))
    API_TIMEOUT = int(os.getenv("API_TIMEOUT", "60"))
    RESPONSE_TRUNCATE = int(os.getenv("RESPONSE_TRUNCATE_LENGTH", "10000"))
    


# ============================
# INITIALIZE CONFIG
# ============================
cfg = Config()

# Ensure upload directory exists
os.makedirs(cfg.UPLOAD_DIR, exist_ok=True)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

# ============= UTILITIES =============
def to_unix_ms(date_str: str, end_of_day: bool = False) -> int:
    """
    Convert date/datetime string to Unix milliseconds.
    Supports formats:
    - "2025-10-31" -> converts to 00:00:00 UTC (or 23:59:59 if end_of_day=True)
    - "2025-10-31 14:30:00" -> converts to exact time UTC
    - "2025-10-31T14:30:00" -> converts to exact time UTC
    """
    try:
        # Try parsing with time first
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"]:
            try:
                dt = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except ValueError:
                continue
        
        # Fall back to date only
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        
        # If end_of_day, set to 23:59:59
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59)
        
        return int(dt.timestamp() * 1000)
    except ValueError:
        raise HTTPException(400, f"Invalid date format: {date_str}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")

def validate_dates(start: str, end: Optional[str]):
    """Validate date strings"""
    try:
        # Parse start date
        start_dt = None
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
            try:
                start_dt = datetime.strptime(start, fmt)
                break
            except ValueError:
                continue
        
        if not start_dt:
            raise ValueError(f"Invalid start date: {start}")
        
        # Parse end date if provided
        if end:
            end_dt = None
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                try:
                    end_dt = datetime.strptime(end, fmt)
                    break
                except ValueError:
                    continue
            
            if not end_dt:
                raise ValueError(f"Invalid end date: {end}")
            
            if end_dt < start_dt:
                raise HTTPException(400, "End date/time must be after start date/time")
            
            if (end_dt - start_dt).days > cfg.MAX_DATE_RANGE_DAYS:
                logger.warning(f"Large date range: {(end_dt - start_dt).days} days")
                
    except ValueError as e:
        raise HTTPException(400, str(e))

def format_age(seconds: float) -> str:
    if not seconds or seconds <= 0:
        return ""
    s = int(seconds)
    d, h, m = s // 86400, (s % 86400) // 3600, (s % 3600) // 60
    if d: return f"{d}d {h}h {m}m"
    if h: return f"{h}h {m}m"
    return f"{m}m"

def convert_timestamp(ts: float) -> str:
    if not ts or ts == 0:
        return ""
    try:
        ts = ts / 1000 if ts > 10000000000 else ts
        return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return ""

def parse_cron(cron_str: str) -> dict:
    """Parse cron string to CronTrigger kwargs"""
    parts = cron_str.split()
    if len(parts) != 5:
        raise ValueError("Invalid cron format. Use: minute hour day month day_of_week")
    return {
        'minute': parts[0], 'hour': parts[1], 'day': parts[2],
        'month': parts[3], 'day_of_week': parts[4]
    }

# ============= EMAIL =============
async def send_email(to: str, subject: str, body: str, file: str, req_id: int):
    db = next(get_database_session())
    log = EmailLog(ticket_request_id=req_id, recipient_email=to, subject=subject, status="sending")
    db.add(log)
    db.commit()

    try:
        msg = EmailMessage()
        msg["Subject"], msg["From"], msg["To"] = subject, cfg.SMTP_USER, to
        msg.set_content(body)
        
        with open(file, "rb") as f:
            msg.add_attachment(f.read(), maintype="text", subtype="csv", filename=os.path.basename(file))
        
        with smtplib.SMTP_SSL(cfg.SMTP_SERVER, cfg.SMTP_PORT, context=ssl.create_default_context()) as server:
            server.login(cfg.SMTP_USER, cfg.SMTP_PASSWORD)
            server.send_message(msg)
        
        log.status = "sent"
        logger.info(f"Email sent: {to}")
    except Exception as e:
        logger.error(f"Email failed: {e}")
        log.status, log.error_message = "failed", str(e)
    finally:
        db.commit()
        db.close()

# ============= INIT =============
async def init_data():
    db = next(get_database_session())
    try:
        if db.query(Company).count() == 0:
            db.add(Company(name="Default Company", api_key="1708890166457x310841261781680100",
                          api_url="https://katicrm.com/api/1.1/wf/whtickets", description="Default"))
            logger.info("Created default company")
        
        if db.query(SystemConfig).count() == 0:
            db.add_all([
                SystemConfig(config_key="smtp_enabled", config_value="true"),
                SystemConfig(config_key="max_date_range_days", config_value=str(cfg.MAX_DATE_RANGE_DAYS))
            ])
            logger.info("Created config")
        db.commit()
    except Exception as e:
        logger.error(f"Init error: {e}")
        db.rollback()
    finally:
        db.close()

# ============= SCHEDULER MANAGEMENT =============
def load_schedules():
    """Load all active schedules and register them"""
    db = next(get_database_session())
    try:
        schedules = db.query(ReportSchedule).filter(
            ReportSchedule.is_active == True
        ).all()
        
        for s in schedules:
            try:
                trigger = CronTrigger(**parse_cron(s.cron_expression))
                scheduler.add_job(
                    run_scheduled_report,
                    trigger,
                    args=[s.id],
                    id=f"schedule_{s.id}",
                    replace_existing=True
                )
                logger.info(f"Loaded schedule {s.id} for {s.company.name}: {s.cron_expression}")
            except Exception as e:
                logger.error(f"Failed to load schedule {s.id}: {e}")
    finally:
        db.close()

async def run_scheduled_report(schedule_id: int):
    """Execute a scheduled report"""
    db = next(get_database_session())
    try:
        s = db.query(ReportSchedule).filter(ReportSchedule.id == schedule_id).first()
        if not s or not s.is_active:
            return
        
        # Calculate date range based on report type
        today = datetime.now()
        if s.report_type == "monthly":
            month_end = today.replace(day=1) - timedelta(days=1)
            start, end = month_end.replace(day=1), month_end
        elif s.report_type == "weekly":
            end = today - timedelta(days=today.weekday() + 1)
            start = end - timedelta(days=6)
        elif s.report_type == "daily":
            start = end = today - timedelta(days=1)
        else:  # custom
            start = datetime.strptime(s.date_start, "%Y-%m-%d") if s.date_start else today - timedelta(days=30)
            end = datetime.strptime(s.date_end, "%Y-%m-%d") if s.date_end else today
        
        # Get recipient emails
        recipients = s.recipients or ",".join([u.email for u in s.company.users if u.receive_reports])
        
        req = TicketRequestCreate(
            company_id=s.company_id,
            date_start=start.strftime("%Y-%m-%d"),
            date_end=end.strftime("%Y-%m-%d 23:59:59"),  # Include full day
            email_to=recipients
        )
        
        r = TicketRequest(
            company_id=s.company_id,
            date_start=req.date_start,
            date_end=req.date_end,
            email_to=req.email_to,
            status="scheduled"
        )
        db.add(r)
        db.commit()
        db.refresh(r)
        
        s.last_run = func.now()
        s.run_count += 1
        db.commit()
        
        logger.info(f"Running scheduled report {schedule_id} for {s.company.name}")
        await process_tickets(r.id, s.company, req)
        
    except Exception as e:
        logger.error(f"Scheduled report {schedule_id} failed: {e}")
    finally:
        db.close()

# ============= LIFESPAN =============
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting...")
    if not test_connection():
        raise Exception("DB connection failed")
    create_tables()
    await init_data()
    
    scheduler.start()
    load_schedules()
    logger.info("Scheduler started with active schedules")
    
    yield
    
    scheduler.shutdown()
    logger.info("Shutdown complete")

# ============= APP =============
app = FastAPI(title="KatiCRM Ticket System", version="3.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, 
                   allow_methods=["*"], allow_headers=["*"])

# ============= COMPANY ENDPOINTS =============
@app.get("/health")
async def health():
    return {"status": "healthy" if test_connection() else "degraded", "version": "3.0"}

@app.post("/companies", response_model=CompanyResponse)
async def create_company(company: CompanyCreate, db: Session = Depends(get_database_session)):
    if db.query(Company).filter(Company.name == company.name).first():
        raise HTTPException(400, "Company exists")
    c = Company(**company.dict())
    db.add(c)
    db.commit()
    db.refresh(c)
    logger.info(f"Created: {company.name}")
    return c

@app.get("/companies", response_model=List[CompanyResponse])
async def list_companies(active: bool = True, db: Session = Depends(get_database_session)):
    q = db.query(Company)
    if active:
        q = q.filter(Company.is_active == True)
    return q.order_by(Company.name).all()

@app.get("/companies/{id}", response_model=CompanyResponse)
async def get_company(id: int, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == id).first()
    if not c:
        raise HTTPException(404, "Not found")
    return c

@app.put("/companies/{id}", response_model=CompanyResponse)
async def update_company(id: int, update: CompanyUpdate, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == id).first()
    if not c:
        raise HTTPException(404, "Not found")
    
    for k, v in update.dict(exclude_unset=True).items():
        setattr(c, k, v)
    c.updated_at = func.now()
    db.commit()
    db.refresh(c)
    logger.info(f"Updated: {c.name}")
    return c

@app.delete("/companies/{id}")
async def delete_company(id: int, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == id).first()
    if not c:
        raise HTTPException(404, "Not found")
    c.is_active = False
    c.updated_at = func.now()
    db.commit()
    return {"success": True, "message": f"Deactivated {c.name}"}

# ============= USER ENDPOINTS =============
@app.post("/companies/{company_id}/users", response_model=UserResponse)
async def add_user(company_id: int, user: UserCreate, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == company_id).first()
    if not c:
        raise HTTPException(404, "Company not found")
    
    if db.query(CompanyUser).filter(CompanyUser.email == user.email, 
                                     CompanyUser.company_id == company_id).first():
        raise HTTPException(400, "User already exists")
    
    u = CompanyUser(company_id=company_id, **user.dict())
    db.add(u)
    db.commit()
    db.refresh(u)
    logger.info(f"Added user {user.email} to {c.name}")
    return u

@app.get("/companies/{company_id}/users", response_model=List[UserResponse])
async def list_users(company_id: int, db: Session = Depends(get_database_session)):
    return db.query(CompanyUser).filter(CompanyUser.company_id == company_id).all()

@app.put("/companies/{company_id}/users/{user_id}", response_model=UserResponse)
async def update_user(company_id: int, user_id: int, update: UserUpdate, 
                      db: Session = Depends(get_database_session)):
    u = db.query(CompanyUser).filter(
        CompanyUser.id == user_id, 
        CompanyUser.company_id == company_id
    ).first()
    if not u:
        raise HTTPException(404, "User not found")
    
    for k, v in update.dict(exclude_unset=True).items():
        setattr(u, k, v)
    u.updated_at = func.now()
    db.commit()
    db.refresh(u)
    return u

@app.delete("/companies/{company_id}/users/{user_id}")
async def delete_user(company_id: int, user_id: int, db: Session = Depends(get_database_session)):
    u = db.query(CompanyUser).filter(
        CompanyUser.id == user_id,
        CompanyUser.company_id == company_id
    ).first()
    if not u:
        raise HTTPException(404, "User not found")
    db.delete(u)
    db.commit()
    return {"success": True, "message": "User deleted"}

# ============= SCHEDULE ENDPOINTS =============
@app.post("/companies/{company_id}/schedules", response_model=ScheduleResponse)
async def create_schedule(company_id: int, schedule: ScheduleCreate, 
                          db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == company_id).first()
    if not c:
        raise HTTPException(404, "Company not found")
    
    if schedule.cron_expression:
        try:
            parse_cron(schedule.cron_expression)
        except:
            raise HTTPException(400, "Invalid cron expression. Use: minute hour day month day_of_week")
    
    s = ReportSchedule(company_id=company_id, **schedule.dict())
    db.add(s)
    db.commit()
    db.refresh(s)
    
    if s.is_active and s.cron_expression:
        try:
            trigger = CronTrigger(**parse_cron(s.cron_expression))
            scheduler.add_job(
                run_scheduled_report,
                trigger,
                args=[s.id],
                id=f"schedule_{s.id}",
                replace_existing=True
            )
            logger.info(f"Created schedule {s.id} for {c.name}")
        except Exception as e:
            logger.error(f"Failed to register schedule: {e}")
    
    return s

@app.get("/companies/{company_id}/schedules", response_model=List[ScheduleResponse])
async def list_schedules(company_id: int, db: Session = Depends(get_database_session)):
    return db.query(ReportSchedule).filter(ReportSchedule.company_id == company_id).all()

@app.put("/companies/{company_id}/schedules/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(company_id: int, schedule_id: int, update: ScheduleUpdate,
                          db: Session = Depends(get_database_session)):
    s = db.query(ReportSchedule).filter(
        ReportSchedule.id == schedule_id,
        ReportSchedule.company_id == company_id
    ).first()
    if not s:
        raise HTTPException(404, "Schedule not found")
    
    if update.cron_expression:
        try:
            parse_cron(update.cron_expression)
        except:
            raise HTTPException(400, "Invalid cron expression")
    
    for k, v in update.dict(exclude_unset=True).items():
        setattr(s, k, v)
    s.updated_at = func.now()
    db.commit()
    db.refresh(s)
    
    if s.is_active and s.cron_expression:
        try:
            trigger = CronTrigger(**parse_cron(s.cron_expression))
            scheduler.add_job(
                run_scheduled_report,
                trigger,
                args=[s.id],
                id=f"schedule_{s.id}",
                replace_existing=True
            )
        except Exception as e:
            logger.error(f"Failed to update schedule: {e}")
    else:
        try:
            scheduler.remove_job(f"schedule_{s.id}")
        except:
            pass
    
    return s

@app.delete("/companies/{company_id}/schedules/{schedule_id}")
async def delete_schedule(company_id: int, schedule_id: int, 
                          db: Session = Depends(get_database_session)):
    s = db.query(ReportSchedule).filter(
        ReportSchedule.id == schedule_id,
        ReportSchedule.company_id == company_id
    ).first()
    if not s:
        raise HTTPException(404, "Schedule not found")
    
    try:
        scheduler.remove_job(f"schedule_{s.id}")
    except:
        pass
    
    db.delete(s)
    db.commit()
    return {"success": True, "message": "Schedule deleted"}

@app.post("/companies/{company_id}/schedules/{schedule_id}/run")
async def trigger_schedule(company_id: int, schedule_id: int, bg: BackgroundTasks,
                           db: Session = Depends(get_database_session)):
    s = db.query(ReportSchedule).filter(
        ReportSchedule.id == schedule_id,
        ReportSchedule.company_id == company_id
    ).first()
    if not s:
        raise HTTPException(404, "Schedule not found")
    
    bg.add_task(run_scheduled_report, schedule_id)
    return {"success": True, "message": "Schedule triggered"}

# ============= TICKET ENDPOINTS =============
@app.post("/fetch-tickets", response_model=TicketRequestResponse)
async def fetch_tickets(req: TicketRequestCreate, bg: BackgroundTasks, 
                       db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == req.company_id, Company.is_active == True).first()
    if not c:
        raise HTTPException(404, "Company not found/inactive")
    
    validate_dates(req.date_start, req.date_end)
    
    if not req.email_to:
        emails = [u.email for u in c.users if u.receive_reports]
        if emails:
            req.email_to = ",".join(emails)
    
    r = TicketRequest(company_id=req.company_id, date_start=req.date_start, 
                     date_end=req.date_end, email_to=req.email_to, status="processing")
    db.add(r)
    db.commit()
    db.refresh(r)
    
    bg.add_task(process_tickets, r.id, c, req)
    logger.info(f"Request {r.id} for {c.name}")
    return r

# ============= PROCESSING =============
async def process_tickets(req_id: int, company: Company, req_data: TicketRequestCreate):
    db = next(get_database_session())
    start = time.time()
    
    try:
        r = db.query(TicketRequest).filter(TicketRequest.id == req_id).first()
        
        # Convert dates - use end_of_day=True for end date if only date provided
        date_start_unix = to_unix_ms(req_data.date_start, end_of_day=False)
        date_end_unix = to_unix_ms(req_data.date_end, end_of_day=True) if req_data.date_end else ""
        
        payload = {
            "API": company.api_key,
            "module": "Helpdesk",
            "date_start": str(date_start_unix),
            "date_end": str(date_end_unix),
            "ticket_id": "",
            "location": "",
            "status": "",
            "source": "",
            "category": "",
            "disposition": "",
            "sub_disposition": "",
            "comments": "",
            "created_by": "",
            "assigned_to": "",
            "asset_name": ""
        }
        
        logger.info(f"API Request - Start: {date_start_unix}, End: {date_end_unix}")
        
        log = RequestLog(ticket_request_id=req_id, api_url=company.api_url, 
                        request_payload=json.dumps(payload))
        db.add(log)
        db.commit()
        
        async with httpx.AsyncClient(timeout=cfg.API_TIMEOUT) as client:
            api_start = time.time()
            resp = await client.post(company.api_url, json=payload)
            log.response_status_code = resp.status_code
            log.request_duration_ms = int((time.time() - api_start) * 1000)
            
            if resp.status_code != 200:
                raise Exception(f"API error {resp.status_code}")
            
            data = resp.json()
            resp_txt = json.dumps(data)
            log.response_data = resp_txt[:cfg.RESPONSE_TRUNCATE] + "..." if len(resp_txt) > cfg.RESPONSE_TRUNCATE else resp_txt
            db.commit()
            
            logger.info(f"API Response - Tickets returned: {len(data) if data else 0}")
            
            if not data:
                r.status, r.total_tickets, r.completed_at = "completed", 0, func.now()
                r.processing_time_seconds = int(time.time() - start)
                db.commit()
                logger.info(f"No tickets found for request {req_id}")
                return
            
            df = pd.DataFrame(data)
            df.columns = df.columns.str.strip().str.lower()
            
            for col in ['created_date', 'ticket_closure_date']:
                if col in df.columns:
                    df[col] = df[col].replace(['', 'null', 'None', 'NaN'], None)
                    df[f'{col}_ts'] = pd.to_numeric(df[col], errors='coerce')
                    df[f'{col}_readable'] = df[f'{col}_ts'].apply(convert_timestamp)
            
            if 'age' in df.columns:
                df['age'] = df['age'].replace(['', 'null', 'None'], None)
                df['ticket_age'] = pd.to_numeric(df['age'], errors='coerce').apply(format_age)
                df = df.drop(columns=['age'])
            elif 'created_date_ts' in df.columns:
                df['ticket_age'] = df['created_date_ts'].apply(
                    lambda x: format_age((time.time() - x/1000) if pd.notna(x) and x > 0 else 0)
                )
            
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"tickets_{company.name.replace(' ', '_')}_{ts}_{uuid4().hex[:8]}.csv"
            fpath = os.path.join(cfg.UPLOAD_DIR, fname)
            df.to_csv(fpath, index=False, na_rep='')
            
            r.status, r.file_path, r.file_name = "completed", fpath, fname
            r.total_tickets, r.completed_at = len(df), func.now()
            r.processing_time_seconds = int(time.time() - start)
            db.commit()
            
            if req_data.email_to:
                for email in req_data.email_to.split(','):
                    email = email.strip()
                    if email:
                        await send_email(
                            email,
                            f"Tickets Report - {company.name}",
                            f"Report for {company.name}\n\nRange: {req_data.date_start} to {req_data.date_end or 'present'}\n"
                            f"Tickets: {len(df):,}\nTime: {int(time.time() - start)}s\n"
                            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                            fpath, req_id
                        )
            
            logger.info(f"Processed {len(df)} tickets for {company.name} in {int(time.time() - start)}s")
            
    except Exception as e:
        logger.error(f"Error {req_id}: {e}", exc_info=True)
        r = db.query(TicketRequest).filter(TicketRequest.id == req_id).first()
        if r:
            r.status, r.error_message = "failed", str(e)
            r.completed_at, r.processing_time_seconds = func.now(), int(time.time() - start)
            db.commit()
    finally:
        db.close()

@app.post("/test-payload")
async def test_payload(req: TicketRequestCreate, db: Session = Depends(get_database_session)):
    """Test endpoint to see exact payload being sent"""
    c = db.query(Company).filter(Company.id == req.company_id).first()
    if not c:
        raise HTTPException(404, "Company not found")
    
    date_start_unix = to_unix_ms(req.date_start, end_of_day=False)
    date_end_unix = to_unix_ms(req.date_end, end_of_day=True) if req.date_end else ""
    
    payload = {
        "API": c.api_key,
        "module": "Helpdesk",
        "date_start": str(date_start_unix),
        "date_end": str(date_end_unix),
        "ticket_id": "",
        "location": "",
        "status": "",
        "source": "",
        "category": "",
        "disposition": "",
        "sub_disposition": "",
        "comments": "",
        "created_by": "",
        "assigned_to": "",
        "asset_name": ""
    }
    
    return {
        "company": c.name,
        "date_start_input": req.date_start,
        "date_end_input": req.date_end,
        "date_start_unix": date_start_unix,
        "date_end_unix": date_end_unix,
        "date_start_readable": convert_timestamp(date_start_unix),
        "date_end_readable": convert_timestamp(date_end_unix) if date_end_unix else None,
        "payload_sent": payload
    }

@app.get("/test-scheduler")
async def test_scheduler():
    """Reload all schedules"""
    load_schedules()
    return {"status": "Schedules reloaded"}