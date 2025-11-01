from pydantic import BaseModel, validator,ConfigDict
from typing import Optional, List
from datetime import datetime

# === COMPANY SCHEMAS ===
class CompanyBase(BaseModel):
    """Base company schema with common fields."""
    name: str
    api_url: str = "https://katicrm.com/api/1.1/wf/whtickets"
    description: Optional[str] = None

class CompanyCreate(CompanyBase):
    """Schema for creating a new company."""
    api_key: str

class CompanyUpdate(BaseModel):
    """Schema for updating company details."""
    name: Optional[str] = None
    api_key: Optional[str] = None
    api_url: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None

class CompanyResponse(CompanyBase):
    """Schema for returning company data (without API key for security)."""
    id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class CompanyDropdown(BaseModel):
    """Simplified schema for dropdown lists."""
    id: int
    name: str
    
    class Config:
        from_attributes = True

# === USER SCHEMAS ===
class UserBase(BaseModel):
    """Base user schema with common fields."""
    email: str
    name: Optional[str] = None
    role: Optional[str] = None
    receive_reports: bool = True
    
    @validator('email')
    def validate_email(cls, v):
        """Basic email validation"""
        if '@' not in v or '.' not in v.split('@')[-1]:
            raise ValueError('Invalid email format')
        return v.lower().strip()

class UserCreate(UserBase):
    """Schema for creating a new user."""
    pass

class UserUpdate(BaseModel):
    """Schema for updating user details."""
    email: Optional[str] = None
    name: Optional[str] = None
    role: Optional[str] = None
    receive_reports: Optional[bool] = None
    is_active: Optional[bool] = None
    
    @validator('email')
    def validate_email(cls, v):
        """Basic email validation"""
        if v and ('@' not in v or '.' not in v.split('@')[-1]):
            raise ValueError('Invalid email format')
        return v.lower().strip() if v else v

class UserResponse(UserBase):
    """Schema for returning user data."""
    id: int
    company_id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

# === SCHEDULE SCHEMAS ===
class ScheduleBase(BaseModel):
    """Base schedule schema with common fields."""
    name: str
    description: Optional[str] = None
    report_type: str = "monthly"  # daily, weekly, monthly, custom
    cron_expression: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    recipients: Optional[str] = None
    is_active: bool = True
    
    @validator('report_type')
    def validate_report_type(cls, v):
        allowed = ['daily', 'weekly', 'monthly', 'custom']
        if v not in allowed:
            raise ValueError(f'report_type must be one of: {", ".join(allowed)}')
        return v
    
    @validator('cron_expression')
    def validate_cron(cls, v, values):
        if v:
            # Basic validation - ensure 5 parts
            parts = v.split()
            if len(parts) != 5:
                raise ValueError('cron_expression must have 5 parts: minute hour day month day_of_week')
        return v

class ScheduleCreate(ScheduleBase):
    """Schema for creating a new schedule."""
    pass

class ScheduleUpdate(BaseModel):
    """Schema for updating schedule details."""
    name: Optional[str] = None
    description: Optional[str] = None
    report_type: Optional[str] = None
    cron_expression: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    recipients: Optional[str] = None
    is_active: Optional[bool] = None
    
    @validator('report_type')
    def validate_report_type(cls, v):
        if v:
            allowed = ['daily', 'weekly', 'monthly', 'custom']
            if v not in allowed:
                raise ValueError(f'report_type must be one of: {", ".join(allowed)}')
        return v
    
    @validator('cron_expression')
    def validate_cron(cls, v):
        if v:
            parts = v.split()
            if len(parts) != 5:
                raise ValueError('cron_expression must have 5 parts: minute hour day month day_of_week')
        return v

class ScheduleResponse(ScheduleBase):
    """Schema for returning schedule data."""
    id: int
    company_id: int
    last_run: Optional[datetime]
    run_count: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class ScheduleWithCompany(ScheduleResponse):
    """Schema for schedule with company details."""
    company: CompanyDropdown
    
    class Config:
        from_attributes = True

# === TICKET REQUEST SCHEMAS ===
class TicketRequestBase(BaseModel):
    """Base ticket request schema."""
    date_start: str
    date_end: Optional[str] = None
    email_to: Optional[str] = None

class TicketRequestCreate(BaseModel):
    company_id: int
    date_start: str  # Keep as string for input (API accepts string)
    date_end: Optional[str] = None  # Keep as string for input
    email_to: Optional[str] = None
    
class TicketRequestResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    company_id: int
    date_start: datetime  # Changed from str
    date_end: Optional[datetime] = None  # Changed from str
    email_to: Optional[str] = None
    status: str
    file_path: Optional[str] = None
    file_name: Optional[str] = None
    total_tickets: Optional[int] = 0
    error_message: Optional[str] = None
    processing_time_seconds: Optional[int] = None
    created_at: datetime
    completed_at: Optional[datetime] = None


class TicketRequestWithCompany(TicketRequestResponse):
    """Schema for ticket request with company details."""
    company: CompanyDropdown
    
    class Config:
        from_attributes = True

# === API RESPONSE SCHEMAS ===
class ApiResponse(BaseModel):
    """Generic API response schema."""
    success: bool
    message: str
    data: Optional[dict] = None

# === STATISTICS SCHEMAS ===
class SystemStats(BaseModel):
    """Schema for overall system statistics."""
    total_companies: int
    active_companies: int
    total_requests: int
    requests_today: int
    total_tickets_processed: int

class CompanyStats(BaseModel):
    """Schema for company statistics."""
    company_id: int
    company_name: str
    total_requests: int
    completed_requests: int
    failed_requests: int
    total_tickets_processed: int