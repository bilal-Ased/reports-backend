import pytest 
from app.main import to_unix_ms, parse_cron, format_age, convert_timestamp
from fastapi import HTTPException
def test_to_unix_ms_valid():
    ts = to_unix_ms("2025-01-01 12:00:00")
    assert isinstance(ts,int)
    assert ts > 1700000000000 #unix for year 2023
    
    
def test_to_unix_ms_invalid():
    with pytest.raises(HTTPException):
     to_unix_ms("random date")
     
def test_prase_cron_valid():
    cron = parse_cron("*/5 * * * *")
    assert cron["minute"] == "*/5"
    assert cron["hour"] == "*"
    

def test_prase_cron_invalid():
    with pytest.raises(ValueError):
        parse_cron("Wrong Format")
        
def test_age_format():
    assert format_age(65) == "1m"        
    assert format_age(40000).endswith("m")
    assert format_age(-1) == ""     


def time_convert_timestamp():
    ts = convert_timestamp(1730000000000)
    assert convert_timestamp(0) == "0"
    