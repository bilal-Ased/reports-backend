import smtplib
import ssl
import os
import json
import time
import re
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo
import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Header, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
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
# CONFIGURATION
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
    BEARER_TOKEN = os.getenv("eyJhbGciOiJub25lIiwiY3R5IjoiSldUIn0.5f8d3a7c9b2e4f6a1c0d7e9b3a2f4c6d8e0b1a2c")
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

cfg = Config()
os.makedirs(cfg.UPLOAD_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

# ============================
# SECURITY - Bearer Token Dependency
# ============================
bearer_scheme = HTTPBearer()

async def verify_token(credentials: HTTPAuthorizationCredentials = Security(bearer_scheme)):
    if credentials.scheme != "Bearer" or credentials.credentials != cfg.BEARER_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing Bearer token")
    return credentials.credentials

# Optional: Public endpoints (no token required)
async def public_endpoint():
    return True

# ============================
# UTILITIES
# ============================
def to_unix_ms(date_str: str, end_of_day: bool = False) -> int:
    if not date_str:
        raise ValueError("Date string cannot be empty")
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
            if end_of_day and fmt == "%Y-%m-%d":
                dt = dt.replace(hour=23, minute=59, second=59)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise HTTPException(400, f"Invalid date format: {date_str}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")

def validate_dates(start: str, end: Optional[str]) -> tuple[datetime, Optional[datetime]]:
    start_dt = _parse_date(start, "start")
    end_dt = _parse_date(end, "end") if end else None
    if end_dt and end_dt < start_dt:
        raise HTTPException(400, "End date/time must be after start date/time")
    if end_dt and (end_dt - start_dt).days > cfg.MAX_DATE_RANGE_DAYS:
        logger.warning(f"Large date range: {(end_dt - start_dt).days} days")
    return start_dt, end_dt

def _parse_date(date_str: str, label: str) -> datetime:
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise HTTPException(400, f"Invalid {label} date: {date_str}")

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
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return ""

def parse_cron(cron_str: str) -> dict:
    parts = cron_str.split()
    if len(parts) != 5:
        raise ValueError("Invalid cron format. Use: minute hour day month day_of_week")
    return {
        'minute': parts[0], 'hour': parts[1], 'day': parts[2],
        'month': parts[3], 'day_of_week': parts[4]
    }

def generate_filename(company_name: str, date_start: str, date_end: Optional[str]) -> str:
    safe_company = re.sub(r'[^A-Za-z0-9_-]', '_', company_name.strip())
    start_fmt = _extract_date_formatted(date_start)
    end_fmt = _extract_date_formatted(date_end) if date_end else datetime.now().strftime('%Y-%m-%d')
    return f"{safe_company}_{start_fmt}_to_{end_fmt}.csv"

def _extract_date_formatted(date_str: str) -> str:
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    match = re.search(r'(\d{4})-?(\d{2})-?(\d{2})', date_str)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return datetime.now().strftime('%Y-%m-%d')

# ============================
# SLACK NOTIFICATIONS
# ============================
async def send_slack_notification(message: str, level: str = "info"):
    """Send notification to Slack webhook"""
    if not cfg.SLACK_WEBHOOK_URL:
        logger.warning("Slack webhook URL not configured")
        return
    
    try:
        # Color coding based on level
        colors = {
            "info": "#36a64f",      # Green
            "warning": "#ff9900",   # Orange
            "error": "#ff0000",     # Red
            "success": "#00ff00"    # Bright green
        }
        
        color = colors.get(level, "#36a64f")
        eat_time = datetime.now(ZoneInfo('Africa/Nairobi')).strftime('%Y-%m-%d %H:%M:%S')
        
        payload = {
            "attachments": [{
                "color": color,
                "text": message,
                "footer": f"KatiCRM Ticket System | {eat_time} EAT",
                "ts": int(time.time())
            }]
        }
        
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(cfg.SLACK_WEBHOOK_URL, json=payload)
            if response.status_code == 200:
                logger.info(f"Slack notification sent: {level}")
            else:
                logger.error(f"Slack notification failed: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")

async def send_slack_report_summary(company_name: str, tickets_count: int, date_start: str, date_end: str, processing_time: int, recipients: str):
    """Send formatted report summary to Slack"""
    message = (
        f"*📊 Report Generated*\n"
        f"*Company:* {company_name}\n"
        f"*Date Range:* {date_start} to {date_end}\n"
        f"*Total Tickets:* {tickets_count:,}\n"
        f"*Processing Time:* {processing_time}s\n"
        f"*Recipients:* {recipients}"
    )
    await send_slack_notification(message, "success")

# ============================
# SLACK NOTIFICATIONS
# ============================
async def send_slack_notification(message: str, level: str = "info"):
    """Send notification to Slack webhook"""
    if not cfg.SLACK_WEBHOOK_URL:
        return
    
    try:
        # Color coding based on level
        colors = {
            "info": "#36a64f",      # Green
            "warning": "#ff9900",   # Orange
            "error": "#ff0000",     # Red
            "success": "#00ff00"    # Bright green
        }
        
        color = colors.get(level, "#36a64f")
        eat_time = datetime.now(ZoneInfo('Africa/Nairobi')).strftime('%Y-%m-%d %H:%M:%S')
        
        payload = {
            "attachments": [{
                "color": color,
                "text": message,
                "footer": "KatiCRM Ticket System",
                "ts": int(time.time()),
                "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png"
            }]
        }
        
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(cfg.SLACK_WEBHOOK_URL, json=payload)
            logger.info(f"Slack notification sent: {level}")
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")

async def send_slack_report_summary(company_name: str, tickets_count: int, date_start: str, date_end: str, processing_time: int, recipients: str):
    """Send formatted report summary to Slack"""
    message = (
        f"*📊 Report Generated*\n"
        f"*Company:* {company_name}\n"
        f"*Date Range:* {date_start} to {date_end}\n"
        f"*Total Tickets:* {tickets_count:,}\n"
        f"*Processing Time:* {processing_time}s\n"
        f"*Sent To:* {recipients}\n"
    )
    await send_slack_notification(message, "success")

# ============================
# EMAIL
# ============================
async def send_email(to: str, subject: str, body: str, file_path: str, req_id: int):
    db = next(get_database_session())
    log = EmailLog(ticket_request_id=req_id, recipient_email=to, subject=subject, status="sending")
    db.add(log)
    db.commit()
    try:
        msg = EmailMessage()
        msg["Subject"], msg["From"], msg["To"] = subject, cfg.SMTP_USER, to
        msg.set_content(body)
        with open(file_path, "rb") as f:
            msg.add_attachment(f.read(), maintype="text", subtype="csv", filename=os.path.basename(file_path))
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(cfg.SMTP_SERVER, cfg.SMTP_PORT, context=context) as server:
            server.login(cfg.SMTP_USER, cfg.SMTP_PASSWORD)
            server.send_message(msg)
        log.status = "sent"
        logger.info(f"Email sent to {to}")
    except Exception as e:
        logger.error(f"Email failed for {to}: {e}")
        log.status, log.error_message = "failed", str(e)
    finally:
        db.commit()
        db.close()

# ============================
# INITIALIZATION
# ============================
async def init_data():
    db = next(get_database_session())
    try:
        if db.query(Company).count() == 0:
            db.add(Company(
                name="Default Company",
                api_key="1708890166457x310841261781680100",
                api_url="https://katicrm.com/api/1.1/wf/whtickets",
                description="Default company"
            ))
            logger.info("Created default company")
        if db.query(SystemConfig).count() == 0:
            db.add_all([
                SystemConfig(config_key="smtp_enabled", config_value="true"),
                SystemConfig(config_key="max_date_range_days", config_value=str(cfg.MAX_DATE_RANGE_DAYS))
            ])
            logger.info("Created system config")
        db.commit()
        await send_slack_notification("✅ KatiCRM Ticket System started successfully", "success")
    except Exception as e:
        logger.error(f"Init error: {e}")
        db.rollback()
        await send_slack_notification(f"❌ Initialization error: {str(e)}", "error")
    finally:
        db.close()

# ============================
# SCHEDULER
# ============================
def load_schedules():
    db = next(get_database_session())
    try:
        schedules = db.query(ReportSchedule).filter(ReportSchedule.is_active == True).all()
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
                logger.info(f"Loaded schedule {s.id}: {s.cron_expression}")
            except Exception as e:
                logger.error(f"Failed to load schedule {s.id}: {e}")
    finally:
        db.close()

async def run_scheduled_report(schedule_id: int):
    db = next(get_database_session())
    try:
        s = db.query(ReportSchedule).filter(ReportSchedule.id == schedule_id).first()
        if not s or not s.is_active:
            return
        
        await send_slack_notification(f"📅 Starting scheduled report for {s.company.name}", "info")
        
        today = datetime.now()
        if s.report_type == "monthly":
            month_end = today.replace(day=1) - timedelta(days=1)
            start, end = month_end.replace(day=1), month_end
        elif s.report_type == "weekly":
            end = today - timedelta(days=today.weekday() + 1)
            start = end - timedelta(days=6)
        elif s.report_type == "daily":
            start = end = today - timedelta(days=1)
        else:
            start = datetime.strptime(s.date_start, "%Y-%m-%d") if s.date_start else today - timedelta(days=30)
            end = datetime.strptime(s.date_end, "%Y-%m-%d") if s.date_end else today
        recipients = s.recipients or ",".join([u.email for u in s.company.users if u.receive_reports])
        req = TicketRequestCreate(
            company_id=s.company_id,
            date_start=start.strftime("%Y-%m-%d"),
            date_end=end.strftime("%Y-%m-%d 23:59:59"),
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
        logger.info(f"Running scheduled report {schedule_id}")
        await process_tickets(r.id, s.company, req)
    except Exception as e:
        logger.error(f"Scheduled report {schedule_id} failed: {e}")
        await send_slack_notification(f"❌ Scheduled report failed for schedule {schedule_id}: {str(e)}", "error")
    finally:
        db.close()

# ============================
# LIFESPAN
# ============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application...")
    if not test_connection():
        raise Exception("Database connection failed")
    create_tables()
    await init_data()
    scheduler.start()
    load_schedules()
    logger.info("Scheduler started")
    yield
    scheduler.shutdown()
    logger.info("Shutdown complete")

# ============================
# APP
# ============================
app = FastAPI(title="KatiCRM Ticket System", version="3.1", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# ============================
# ENDPOINTS - PUBLIC (NO TOKEN)
# ============================
@app.get("/health")
async def health():
    return {"status": "healthy" if test_connection() else "degraded", "version": "3.1"}

@app.post("/test-payload")
async def test_payload(req: TicketRequestCreate, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == req.company_id).first()
    if not c:
        raise HTTPException(404, "Company not found")
    date_start_unix = to_unix_ms(req.date_start, end_of_day=False)
    date_end_unix = to_unix_ms(req.date_end, end_of_day=True) if req.date_end else ""
    payload = {
        "API": c.api_key, "module": "Helpdesk", "date_start": str(date_start_unix),
        "date_end": str(date_end_unix), "ticket_id": "", "location": "", "status": "",
        "source": "", "category": "", "disposition": "", "sub_disposition": "",
        "comments": "", "created_by": "", "assigned_to": "", "asset_name": ""
    }
    return {
        "company": c.name, "date_start_input": req.date_start, "date_end_input": req.date_end,
        "date_start_unix": date_start_unix, "date_end_unix": date_end_unix,
        "date_start_readable": convert_timestamp(date_start_unix),
        "date_end_readable": convert_timestamp(date_end_unix) if date_end_unix else None,
        "payload": payload, "filename": generate_filename(c.name, req.date_start, req.date_end)
    }

# ============================
# ENDPOINTS - PROTECTED (BEARER TOKEN REQUIRED)
# ============================
@app.post("/companies", response_model=CompanyResponse, dependencies=[Depends(verify_token)])
async def create_company(company: CompanyCreate, db: Session = Depends(get_database_session)):
    if db.query(Company).filter(Company.name == company.name).first():
        raise HTTPException(400, "Company already exists")
    c = Company(**company.dict())
    db.add(c)
    db.commit()
    db.refresh(c)
    logger.info(f"Created company: {company.name}")
    return c

@app.get("/companies", response_model=List[CompanyResponse], dependencies=[Depends(verify_token)])
async def list_companies(active: bool = True, db: Session = Depends(get_database_session)):
    q = db.query(Company)
    if active:
        q = q.filter(Company.is_active == True)
    return q.all()

    return q.order_by(Company.name).all()

@app.get("/companies/{id}", response_model=CompanyResponse, dependencies=[Depends(verify_token)])
async def get_company(id: int, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == id).first()
    if not c:
        raise HTTPException(404, "Company not found")
    return c

@app.put("/companies/{id}", response_model=CompanyResponse, dependencies=[Depends(verify_token)])
async def update_company(id: int, update: CompanyUpdate, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == id).first()
    if not c:
        raise HTTPException(404, "Company not found")
    for k, v in update.dict(exclude_unset=True).items():
        setattr(c, k, v)
    c.updated_at = func.now()
    db.commit()
    db.refresh(c)
    logger.info(f"Updated company: {c.name}")
    return c

@app.delete("/companies/{id}", dependencies=[Depends(verify_token)])
async def delete_company(id: int, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == id).first()
    if not c:
        raise HTTPException(404, "Company not found")
    c.is_active = False
    c.updated_at = func.now()
    db.commit()
    return {"success": True, "message": f"Deactivated {c.name}"}

@app.post("/companies/{company_id}/users", response_model=UserResponse, dependencies=[Depends(verify_token)])
async def add_user(company_id: int, user: UserCreate, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == company_id).first()
    if not c:
        raise HTTPException(404, "Company not found")
    if db.query(CompanyUser).filter(CompanyUser.email == user.email, CompanyUser.company_id == company_id).first():
        raise HTTPException(400, "User already exists")
    u = CompanyUser(company_id=company_id, **user.dict())
    db.add(u)
    db.commit()
    db.refresh(u)
    logger.info(f"Added user {user.email} to {c.name}")
    return u

@app.get("/companies/{company_id}/users", response_model=List[UserResponse], dependencies=[Depends(verify_token)])
async def list_users(company_id: int, db: Session = Depends(get_database_session)):
    return db.query(CompanyUser).filter(CompanyUser.company_id == company_id).all()

@app.put("/companies/{company_id}/users/{user_id}", response_model=UserResponse, dependencies=[Depends(verify_token)])
async def update_user(company_id: int, user_id: int, update: UserUpdate, db: Session = Depends(get_database_session)):
    u = db.query(CompanyUser).filter(CompanyUser.id == user_id, CompanyUser.company_id == company_id).first()
    if not u:
        raise HTTPException(404, "User not found")
    for k, v in update.dict(exclude_unset=True).items():
        setattr(u, k, v)
    u.updated_at = func.now()
    db.commit()
    db.refresh(u)
    return u

@app.delete("/companies/{company_id}/users/{user_id}", dependencies=[Depends(verify_token)])
async def delete_user(company_id: int, user_id: int, db: Session = Depends(get_database_session)):
    u = db.query(CompanyUser).filter(CompanyUser.id == user_id, CompanyUser.company_id == company_id).first()
    if not u:
        raise HTTPException(404, "User not found")
    db.delete(u)
    db.commit()
    return {"success": True, "message": "User deleted"}

@app.post("/companies/{company_id}/schedules", response_model=ScheduleResponse, dependencies=[Depends(verify_token)])
async def create_schedule(company_id: int, schedule: ScheduleCreate, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == company_id).first()
    if not c:
        raise HTTPException(404, "Company not found")
    if schedule.cron_expression:
        try:
            parse_cron(schedule.cron_expression)
        except:
            raise HTTPException(400, "Invalid cron expression")
    s = ReportSchedule(company_id=company_id, **schedule.dict())
    db.add(s)
    db.commit()
    db.refresh(s)
    if s.is_active and s.cron_expression:
        try:
            trigger = CronTrigger(**parse_cron(s.cron_expression))
            scheduler.add_job(run_scheduled_report, trigger, args=[s.id], id=f"schedule_{s.id}", replace_existing=True)
            logger.info(f"Created schedule {s.id}")
        except Exception as e:
            logger.error(f"Failed to register schedule: {e}")
    return s

@app.get("/companies/{company_id}/schedules", response_model=List[ScheduleResponse], dependencies=[Depends(verify_token)])
async def list_schedules(company_id: int, db: Session = Depends(get_database_session)):
    return db.query(ReportSchedule).filter(ReportSchedule.company_id == company_id).all()

@app.put("/companies/{company_id}/schedules/{schedule_id}", response_model=ScheduleResponse, dependencies=[Depends(verify_token)])
async def update_schedule(company_id: int, schedule_id: int, update: ScheduleUpdate, db: Session = Depends(get_database_session)):
    s = db.query(ReportSchedule).filter(ReportSchedule.id == schedule_id, ReportSchedule.company_id == company_id).first()
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
            scheduler.add_job(run_scheduled_report, trigger, args=[s.id], id=f"schedule_{s.id}", replace_existing=True)
        except Exception as e:
            logger.error(f"Failed to update schedule: {e}")
    else:
        try:
            scheduler.remove_job(f"schedule_{s.id}")
        except:
            pass
    return s

@app.delete("/companies/{company_id}/schedules/{schedule_id}", dependencies=[Depends(verify_token)])
async def delete_schedule(company_id: int, schedule_id: int, db: Session = Depends(get_database_session)):
    s = db.query(ReportSchedule).filter(ReportSchedule.id == schedule_id, ReportSchedule.company_id == company_id).first()
    if not s:
        raise HTTPException(404, "Schedule not found")
    try:
        scheduler.remove_job(f"schedule_{s.id}")
    except:
        pass
    db.delete(s)
    db.commit()
    return {"success": True, "message": "Schedule deleted"}

@app.post("/companies/{company_id}/schedules/{schedule_id}/run", dependencies=[Depends(verify_token)])
async def trigger_schedule(company_id: int, schedule_id: int, bg: BackgroundTasks, db: Session = Depends(get_database_session)):
    s = db.query(ReportSchedule).filter(ReportSchedule.id == schedule_id, ReportSchedule.company_id == company_id).first()
    if not s:
        raise HTTPException(404, "Schedule not found")
    bg.add_task(run_scheduled_report, schedule_id)
    return {"success": True, "message": "Schedule triggered"}

@app.post("/fetch-tickets", response_model=TicketRequestResponse, dependencies=[Depends(verify_token)])
async def fetch_tickets(req: TicketRequestCreate, bg: BackgroundTasks, db: Session = Depends(get_database_session)):
    c = db.query(Company).filter(Company.id == req.company_id, Company.is_active == True).first()
    if not c:
        raise HTTPException(404, "Company not found or inactive")
    validate_dates(req.date_start, req.date_end)
    if not req.email_to:
        emails = [u.email for u in c.users if u.receive_reports]
        if emails:
            req.email_to = ",".join(emails)
    r = TicketRequest(company_id=req.company_id, date_start=req.date_start, date_end=req.date_end, email_to=req.email_to, status="processing")
    db.add(r)
    db.commit()
    db.refresh(r)
    bg.add_task(process_tickets, r.id, c, req)
    logger.info(f"Created request {r.id} for {c.name}")
    await send_slack_notification(f"🎫 New ticket request created for *{c.name}* (Request ID: {r.id})", "info")
    return r

@app.get("/test-scheduler", dependencies=[Depends(verify_token)])
async def test_scheduler():
    load_schedules()
    return {"status": "Schedules reloaded", "message": "All active schedules have been reloaded"}

# ============================
# PROCESSING LOGIC
# ============================
async def process_tickets(req_id: int, company: Company, req_data: TicketRequestCreate):
    db = next(get_database_session())
    start_time = time.time()
    try:
        r = db.query(TicketRequest).filter(TicketRequest.id == req_id).first()
        date_start_unix = to_unix_ms(req_data.date_start, end_of_day=False)
        date_end_unix = to_unix_ms(req_data.date_end, end_of_day=True) if req_data.date_end else ""
        payload = {
            "API": company.api_key, "module": "Helpdesk", "date_start": str(date_start_unix),
            "date_end": str(date_end_unix), "ticket_id": "", "location": "", "status": "",
            "source": "", "category": "", "disposition": "", "sub_disposition": "",
            "comments": "", "created_by": "", "assigned_to": "", "asset_name": ""
        }
        logger.info(f"API Request - Start: {date_start_unix}, End: {date_end_unix}")
        log = RequestLog(ticket_request_id=req_id, api_url=company.api_url, request_payload=json.dumps(payload))
        db.add(log)
        db.commit()
        async with httpx.AsyncClient(timeout=cfg.API_TIMEOUT) as client:
            api_start = time.time()
            resp = await client.post(company.api_url, json=payload)
            log.response_status_code = resp.status_code
            log.request_duration_ms = int((time.time() - api_start) * 1000)
            if resp.status_code != 200:
                raise Exception(f"API error: {resp.status_code}")
            data = resp.json()
            resp_txt = json.dumps(data)
            log.response_data = (resp_txt[:cfg.RESPONSE_TRUNCATE] + "..." if len(resp_txt) > cfg.RESPONSE_TRUNCATE else resp_txt)
            db.commit()
            logger.info(f"API Response - Tickets: {len(data) if data else 0}")
            if not data:
                r.status = "completed"
                r.total_tickets = 0
                r.completed_at = func.now()
                r.processing_time_seconds = int(time.time() - start_time)
                db.commit()
                logger.info(f"No tickets found for request {req_id}")
                return
            df = pd.DataFrame(data)
            df.columns = df.columns.str.strip().str.lower()
            for col in ['created_date', 'ticket_closure_date']:
                if col in df.columns:
                    df[col] = df[col].replace(['', 'null', 'None', 'NaN', None], pd.NA)
                    df[f'{col}_parsed'] = pd.to_datetime(df[col], errors='coerce')
                    df[f'{col}_ts'] = df[f'{col}_parsed'].apply(lambda x: x.timestamp() if pd.notna(x) else None)
                    df[f'{col}_readable'] = df[f'{col}_parsed'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else '')
                    df = df.drop(columns=[f'{col}_parsed'])
            if 'created_date_ts' in df.columns:
                current_time = time.time()
                df['age'] = df['created_date_ts'].apply(lambda x: format_age(current_time - x) if pd.notna(x) and x > 0 else '')
            elif 'age' not in df.columns:
                df['age'] = ''
                logger.warning(f"Could not calculate age for request {req_id} - created_date_ts not found")
            filename = generate_filename(company.name, req_data.date_start, req_data.date_end)
            file_path = os.path.join(cfg.UPLOAD_DIR, filename)
            df.to_csv(file_path, index=False, na_rep='')
            r.status = "completed"
            r.file_path = file_path
            r.file_name = filename
            r.total_tickets = len(df)
            r.completed_at = func.now()
            r.processing_time_seconds = int(time.time() - start_time)
            db.commit()
            if req_data.email_to:
                for email in req_data.email_to.split(','):
                    email = email.strip()
                    if email:
                        subject = f"Tickets Report - {company.name}"
                        eat_time = datetime.now(ZoneInfo('Africa/Nairobi'))
                        body = (
                            f"Report for {company.name}\n\n"
                            f"Date Range: {req_data.date_start} to {req_data.date_end or 'present'}\n"
                            f"Total Tickets: {len(df):,}\n"
                            f"Processing Time: {int(time.time() - start_time)}s\n"
                            f"Generated: {eat_time.strftime('%Y-%m-%d %H:%M:%S')} EAT\n\n"
                            f"Please find the attached CSV report."
                        )
                        await send_email(email, subject, body, file_path, req_id)
            
            # Send Slack summary
            await send_slack_report_summary(
                company.name, 
                len(df), 
                req_data.date_start, 
                req_data.date_end or 'present',
                int(time.time() - start_time),
                req_data.email_to or "No recipients"
            )
            
            logger.info(f"Completed {len(df)} tickets for {company.name} in {int(time.time() - start_time)}s")
    except Exception as e:
        logger.error(f"Error processing request {req_id}: {e}", exc_info=True)
        await send_slack_notification(f"❌ Error processing request {req_id} for {company.name}: {str(e)}", "error")
        r = db.query(TicketRequest).filter(TicketRequest.id == req_id).first()
        if r:
            r.status = "failed"
            r.error_message = str(e)
            r.completed_at = func.now()
            r.processing_time_seconds = int(time.time() - start_time)
            db.commit()
    finally:
        db.close()

# ============================
# ADDITIONAL ENDPOINTS (PROTECTED)
# ============================
@app.get("/requests", response_model=List[TicketRequestResponse], dependencies=[Depends(verify_token)])
async def list_requests(company_id: Optional[int] = None, status: Optional[str] = None, limit: int = 50, db: Session = Depends(get_database_session)):
    q = db.query(TicketRequest)
    if company_id:
        q = q.filter(TicketRequest.company_id == company_id)
    if status:
        q = q.filter(TicketRequest.status == status)
    return q.order_by(TicketRequest.created_at.desc()).limit(limit).all()

@app.get("/requests/{request_id}", response_model=TicketRequestResponse, dependencies=[Depends(verify_token)])
async def get_request(request_id: int, db: Session = Depends(get_database_session)):
    r = db.query(TicketRequest).filter(TicketRequest.id == request_id).first()
    if not r:
        raise HTTPException(404, "Request not found")
    return r

@app.get("/requests/{request_id}/logs", dependencies=[Depends(verify_token)])
async def get_request_logs(request_id: int, db: Session = Depends(get_database_session)):
    r = db.query(TicketRequest).filter(TicketRequest.id == request_id).first()
    if not r:
        raise HTTPException(404, "Request not found")
    request_logs = db.query(RequestLog).filter(RequestLog.ticket_request_id == request_id).all()
    email_logs = db.query(EmailLog).filter(EmailLog.ticket_request_id == request_id).all()
    return {"request": r, "api_logs": request_logs, "email_logs": email_logs}


@app.get("/")
def read_root():
       return {"message": "Welcome to the KatiCRM Ticket System API"}