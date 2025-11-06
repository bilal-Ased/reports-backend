import os 
import pytest 
from fastapi.testclient import TestClient
from app.main import app, cfg

client = TestClient(app)


@pytest.fixture 
def token_header():
    return {"Authorization": f"Bearer {cfg.BEARER_TOKEN}"}



def test_health():
    res = client.get("/health")
    assert res.status_code == 200
    assert "status" in res.json()
    

def test_list_companies_unauthorized():
    res = client.get("/companies")
    assert res.status_code == "401"
    

def test_list_companies_authorized():
    res = client.get("/companies", headers=token_header)
    assert res.status_code == "200"
    assert isinstance(res.json(),list)



def test_payload_valid(token_header):
    companies = client.get("/companies", headers=token_header)
    if not companies:
        pytest.skip("no company data")
        cid = compabies[0] ["id"]
        payload = {"company_id": cid , "date_start" :"2025-01-01", "date_end":"2025-01-02"}
        
        res = client.post("/test-payload", json=payload)
        assert res.status_code == 200
        data = res.json()
        assert "payload" in data