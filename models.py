from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text, ForeignKey, Float, text
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base


# -------------------------------
# Company Table
# -------------------------------
class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    api_key = Column(String(500), nullable=False)
    api_url = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, index=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    ticket_requests = relationship("TicketRequest", back_populates="company")
    users = relationship("CompanyUser", back_populates="company", cascade="all, delete-orphan")
    schedules = relationship("ReportSchedule", back_populates="company", cascade="all, delete-orphan")


# -------------------------------
# Company Users
# -------------------------------
class CompanyUser(Base):
    __tablename__ = "company_users"

    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)
    email = Column(String(255), nullable=False, index=True)
    name = Column(String(255), nullable=True)
    role = Column(String(100), nullable=True)
    receive_reports = Column(Boolean, default=True)
    is_active = Column(Boolean, default=True, index=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=text('CURRENT_TIMESTAMP'),
        nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP'),
        nullable=False
    )

    company = relationship("Company", back_populates="users")


# -------------------------------
# Report Schedule
# -------------------------------
class ReportSchedule(Base):
    __tablename__ = "report_schedules"

    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    report_type = Column(String(50), default="monthly")
    cron_expression = Column(String(100), nullable=True)
    date_start = Column(String(20), nullable=True)
    date_end = Column(String(20), nullable=True)
    recipients = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, index=True)
    last_run = Column(DateTime(timezone=True), nullable=True)
    run_count = Column(Integer, default=0)

    created_at = Column(
        DateTime(timezone=True),
        server_default=text('CURRENT_TIMESTAMP'),
        nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP'),
        nullable=False
    )

    company = relationship("Company", back_populates="schedules")


# -------------------------------
# Ticket Request
# -------------------------------
class TicketRequest(Base):
    __tablename__ = "ticket_requests"

    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)
    date_start = Column(String(20), nullable=False)
    date_end = Column(String(20), nullable=True)
    email_to = Column(Text, nullable=True)
    status = Column(String(50), default="pending", index=True)
    file_path = Column(String(500), nullable=True)
    file_name = Column(String(255), nullable=True)
    total_tickets = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    processing_time_seconds = Column(Integer, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=text('CURRENT_TIMESTAMP'),
        nullable=False,
        index=True
    )
    completed_at = Column(DateTime(timezone=True), nullable=True)

    company = relationship("Company", back_populates="ticket_requests")
    request_logs = relationship("RequestLog", back_populates="ticket_request", cascade="all, delete-orphan")
    email_logs = relationship("EmailLog", back_populates="ticket_request", cascade="all, delete-orphan")


# -------------------------------
# Request Log
# -------------------------------
class RequestLog(Base):
    __tablename__ = "request_logs"

    id = Column(Integer, primary_key=True, index=True)
    ticket_request_id = Column(Integer, ForeignKey("ticket_requests.id", ondelete="CASCADE"), nullable=False, index=True)
    api_url = Column(String(500), nullable=False)
    request_payload = Column(Text, nullable=True)
    response_status_code = Column(Integer, nullable=True, index=True)
    response_data = Column(Text, nullable=True)
    request_duration_ms = Column(Integer, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=text('CURRENT_TIMESTAMP'),
        nullable=False
    )

    ticket_request = relationship("TicketRequest", back_populates="request_logs")


# -------------------------------
# Email Log
# -------------------------------
class EmailLog(Base):
    __tablename__ = "email_logs"

    id = Column(Integer, primary_key=True, index=True)
    ticket_request_id = Column(Integer, ForeignKey("ticket_requests.id", ondelete="CASCADE"), nullable=False, index=True)
    recipient_email = Column(String(255), nullable=False, index=True)
    subject = Column(String(500), nullable=True)
    status = Column(String(50), default="pending", index=True)
    error_message = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=text('CURRENT_TIMESTAMP'),
        nullable=False
    )

    ticket_request = relationship("TicketRequest", back_populates="email_logs")


# -------------------------------
# System Config
# -------------------------------
class SystemConfig(Base):
    __tablename__ = "system_config"

    id = Column(Integer, primary_key=True, index=True)
    config_key = Column(String(255), unique=True, nullable=False, index=True)
    config_value = Column(Text, nullable=True)
    description = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=text('CURRENT_TIMESTAMP'),
        nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=text('CURRENT_TIMESTAMP'),
        nullable=False
    )
