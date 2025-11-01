from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import asyncio
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
from typing import Optional
import logging
import os
import re

app = FastAPI(title="Bulk Email Sender")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_email(email: str) -> bool:
    """Simple email validation"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

class EmailRequest(BaseModel):
    to: str
    subject: str
    text: Optional[str] = None
    html: Optional[str] = None
    from_email: str
    count: int = 1000

class EmailConfig:
 SMTP_HOST = "smtp.gmail.com"
 SMTP_PORT = "465"
 SMTP_USER = "mughalbilal89@gmail.com"
 SMTP_PASS = "pgeizwxtxxdnepae"
 USE_TLS = True

async def create_smtp_connection():
    """Create an async SMTP connection"""
    smtp = aiosmtplib.SMTP(
        hostname=EmailConfig.SMTP_HOST,
        port=EmailConfig.SMTP_PORT,
        use_tls=EmailConfig.USE_TLS
    )
    await smtp.connect()
    await smtp.login(EmailConfig.SMTP_USER, EmailConfig.SMTP_PASS)
    return smtp

def create_email_message(to: str, subject: str, text: str = None, html: str = None, from_email: str = None, index: int = 0):
    """Create email message"""
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"{subject} #{index + 1}"  # Make each email unique
    msg['From'] = from_email
    msg['To'] = to
    
    if text:
        text_part = MIMEText(text, 'plain')
        msg.attach(text_part)
    
    if html:
        html_part = MIMEText(html, 'html')
        msg.attach(html_part)
    
    return msg

async def send_single_email(smtp, email_data: dict, semaphore: asyncio.Semaphore):
    """Send a single email with concurrency control"""
    async with semaphore:
        try:
            msg = create_email_message(**email_data)
            await smtp.send_message(msg)
            return {"success": True, "index": email_data["index"]}
        except Exception as e:
            logger.error(f"Failed to send email {email_data['index']}: {str(e)}")
            return {"success": False, "index": email_data["index"], "error": str(e)}

async def send_bulk_emails_batch(email_request: EmailRequest, batch_size: int = 50):
    """Send emails in batches with high concurrency"""
    start_time = time.time()
    results = []
    
    # Create semaphore to control concurrency
    semaphore = asyncio.Semaphore(batch_size)
    
    # Create SMTP connection pool
    smtp_connections = []
    connection_pool_size = min(10, batch_size // 5)  # Adjust based on your SMTP limits
    
    try:
        # Create multiple SMTP connections
        for _ in range(connection_pool_size):
            smtp = await create_smtp_connection()
            smtp_connections.append(smtp)
        
        # Prepare email data
        email_tasks = []
        for i in range(email_request.count):
            email_data = {
                "to": email_request.to,
                "subject": email_request.subject,
                "text": email_request.text,
                "html": email_request.html,
                "from_email": email_request.from_email,
                "index": i
            }
            
            # Round-robin SMTP connections
            smtp = smtp_connections[i % len(smtp_connections)]
            task = send_single_email(smtp, email_data, semaphore)
            email_tasks.append(task)
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*email_tasks, return_exceptions=True)
        
    finally:
        # Close all SMTP connections
        for smtp in smtp_connections:
            try:
                await smtp.quit()
            except:
                pass
    
    end_time = time.time()
    
    # Process results
    successful = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
    failed = len(results) - successful
    
    return {
        "total_sent": email_request.count,
        "successful": successful,
        "failed": failed,
        "time_taken": round(end_time - start_time, 2),
        "emails_per_second": round(email_request.count / (end_time - start_time), 2),
        "results": results if failed > 0 else "All emails sent successfully"
    }

async def send_ultra_fast_concurrent(email_request: EmailRequest):
    """Ultra-fast concurrent sending - all emails at once"""
    start_time = time.time()
    
    # Create a large semaphore for maximum concurrency
    semaphore = asyncio.Semaphore(100)  # Adjust based on your server capacity
    
    # Create multiple SMTP connections
    smtp_connections = []
    connection_pool_size = 20  # More connections for maximum speed
    
    try:
        for _ in range(connection_pool_size):
            smtp = await create_smtp_connection()
            smtp_connections.append(smtp)
        
        # Create all tasks at once
        tasks = []
        for i in range(email_request.count):
            email_data = {
                "to": email_request.to,
                "subject": email_request.subject,
                "text": email_request.text,
                "html": email_request.html,
                "from_email": email_request.from_email,
                "index": i
            }
            
            smtp = smtp_connections[i % len(smtp_connections)]
            task = send_single_email(smtp, email_data, semaphore)
            tasks.append(task)
        
        # Execute ALL tasks simultaneously
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
    finally:
        for smtp in smtp_connections:
            try:
                await smtp.quit()
            except:
                pass
    
    end_time = time.time()
    
    successful = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
    failed = len(results) - successful
    
    return {
        "total_sent": email_request.count,
        "successful": successful,
        "failed": failed,
        "time_taken": round(end_time - start_time, 2),
        "emails_per_second": round(email_request.count / (end_time - start_time), 2)
    }

@app.post("/send-bulk-emails")
async def send_bulk_emails(email_request: EmailRequest, background_tasks: BackgroundTasks):
    """Send bulk emails with batch processing"""
    
    # Validate email addresses
    if not validate_email(email_request.to):
        raise HTTPException(status_code=400, detail=f"Invalid recipient email: {email_request.to}")
    
    if not validate_email(email_request.from_email):
        raise HTTPException(status_code=400, detail=f"Invalid sender email: {email_request.from_email}")
    
    if not EmailConfig.SMTP_USER or not EmailConfig.SMTP_PASS:
        raise HTTPException(
            status_code=500, 
            detail="SMTP credentials not configured. Set SMTP_USER and SMTP_PASS environment variables."
        )
    
    if not email_request.text and not email_request.html:
        raise HTTPException(status_code=400, detail="Either text or html content is required")
    
    try:
        result = await send_bulk_emails_batch(email_request)
        return result
    except Exception as e:
        logger.error(f"Bulk email sending failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to send emails: {str(e)}")

@app.post("/send-ultra-fast")
async def send_ultra_fast(email_request: EmailRequest):
    """Send all emails simultaneously for maximum speed"""
    
    # Validate email addresses
    if not validate_email(email_request.to):
        raise HTTPException(status_code=400, detail=f"Invalid recipient email: {email_request.to}")
    
    if not validate_email(email_request.from_email):
        raise HTTPException(status_code=400, detail=f"Invalid sender email: {email_request.from_email}")
    
    if not EmailConfig.SMTP_USER or not EmailConfig.SMTP_PASS:
        raise HTTPException(
            status_code=500, 
            detail="SMTP credentials not configured"
        )
    
    if email_request.count > 2000:
        raise HTTPException(
            status_code=400, 
            detail="Maximum 2000 emails allowed for ultra-fast mode"
        )
    
    if not email_request.text and not email_request.html:
        raise HTTPException(status_code=400, detail="Either text or html content is required")
    
    try:
        result = await send_ultra_fast_concurrent(email_request)
        return result
    except Exception as e:
        logger.error(f"Ultra-fast email sending failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to send emails: {str(e)}")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "Bulk email service is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1  # Use 1 worker for async operations
    )