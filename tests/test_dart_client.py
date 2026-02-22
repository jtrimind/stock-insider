import os
import sys
import io
import zipfile
import pytest
import responses
import tempfile
from unittest.mock import patch

# Add parent directory to sys.path to import dart_client
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dart_client import DARTClient

@pytest.fixture
def mock_env(monkeypatch):
    """Ensure we have a clean environment with a dummy DART API key."""
    monkeypatch.setenv("DART_API_KEY", "dummy_test_key")

@pytest.fixture
def client(mock_env):
    """Return a fresh DARTClient instance with an isolated cache directory."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        c = DARTClient()
        c._cache_dir = tmpdirname
        yield c

def test_missing_api_key(monkeypatch):
    """Test that client raises a ValueError if initialized without an API key."""
    monkeypatch.delenv("DART_API_KEY", raising=False)
    with pytest.raises(ValueError):
        DARTClient(api_key=None)

@responses.activate
def test_get_corp_codes(client):
    """Test downloading, extracting, and parsing the corpCode.xml."""
    # Build a mock zip file containing CORPCODE.xml
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <result>
        <list>
            <corp_code>00126380</corp_code>
            <corp_name>삼성전자</corp_name>
            <stock_code>005930</stock_code>
            <modify_date>20230101</modify_date>
        </list>
        <list>
            <corp_code>00164779</corp_code>
            <corp_name>SK하이닉스</corp_name>
            <stock_code>000660</stock_code>
            <modify_date>20230101</modify_date>
        </list>
    </result>
    """
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("CORPCODE.xml", xml_content)
        
    responses.add(
        responses.GET,
        "https://opendart.fss.or.kr/api/corpCode.xml",
        body=zip_buffer.getvalue(),
        status=200,
        content_type="application/zip"
    )
    
    codes = client.get_corp_codes()
    
    assert len(codes) == 2
    assert codes["삼성전자"] == "00126380"
    assert codes["SK하이닉스"] == "00164779"
    
    # Test getting code by name wrapper
    assert client.get_corp_code_by_name("삼성전자") == "00126380"
    assert client.get_corp_code_by_name("없는회사") is None

@responses.activate
def test_get_disclosures_success(client):
    """Test returning parsed reports when API status is 000 (success)."""
    mock_response = {
        "status": "000",
        "message": "정상",
        "page_no": 1,
        "page_count": 10,
        "total_count": 1,
        "total_page": 1,
        "list": [
            {
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "stock_code": "005930",
                "corp_cls": "Y",
                "report_nm": "임원ㆍ주요주주특정증권등소유상황보고서",
                "rcept_no": "20230801000001",
                "flr_nm": "홍길동",
                "rcept_dt": "20230801",
                "rm": ""
            }
        ]
    }
    
    responses.add(
        responses.GET,
        "https://opendart.fss.or.kr/api/list.json",
        json=mock_response,
        status=200
    )
    
    reports = client.get_disclosures(corp_code="00126380", pblntf_detail_ty="D002")
    assert len(reports) == 1
    assert reports[0]["corp_name"] == "삼성전자"
    assert reports[0]["flr_nm"] == "홍길동"

@responses.activate
def test_get_disclosures_empty(client):
    """Test returning an empty list when API status is 013 (no data)."""
    mock_response = {
        "status": "013",
        "message": "조회된 데이타가 없습니다."
    }
    
    responses.add(
        responses.GET,
        "https://opendart.fss.or.kr/api/list.json",
        json=mock_response,
        status=200
    )
    
    reports = client.get_disclosures(corp_code="00126380", pblntf_detail_ty="D002")
    assert reports == []
