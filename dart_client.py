import os
import sys
import io
import zipfile
import json
import xml.etree.ElementTree as ET
import requests
from dotenv import load_dotenv
from typing import Dict, List, Optional
from datetime import datetime, timedelta

# Ensure print statements correctly output Korean characters on Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

class DARTClient:
    BASE_URL = "https://opendart.fss.or.kr/api"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("DART_API_KEY")
        if not self.api_key:
            raise ValueError("DART_API_KEY is not set in environment variables or passed to the client.")
        
        # Internal cache for corp_code mapping
        self._corp_code_map: Dict[str, str] = {}
        # Path for the cache directory
        self._cache_dir = os.path.join(os.path.dirname(__file__), '.cache')
        os.makedirs(self._cache_dir, exist_ok=True)

    def get_corp_codes(self, force_refresh: bool = False) -> Dict[str, str]:
        """Fetches and parses the corpCode.xml to map company names back to DART corp_codes.
        Uses a local JSON file cache per day to prevent redundant downloads."""
        if self._corp_code_map and not force_refresh:
            return self._corp_code_map

        # Use current date as cache key (YYYYMMDD)
        today_str = datetime.now().strftime("%Y%m%d")
        cache_file = os.path.join(self._cache_dir, f"corp_codes_{today_str}.json")

        if not force_refresh and os.path.exists(cache_file):
            print(f"Loading corp codes from local cache: {cache_file}")
            with open(cache_file, 'r', encoding='utf-8') as f:
                self._corp_code_map = json.load(f)
            return self._corp_code_map

        url = f"{self.BASE_URL}/corpCode.xml"
        params = {"crtfc_key": self.api_key}
        
        print("Fetching corp codes from DART API...")
        response = requests.get(url, params=params)
        response.raise_for_status()

        # The response is a ZIP file containing corpCode.xml
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            with z.open('CORPCODE.xml') as f:
                tree = ET.parse(f)
                root = tree.getroot()
                
                for list_item in root.findall('list'):
                    corp_name = list_item.findtext('corp_name')
                    corp_code = list_item.findtext('corp_code')
                    if corp_name and corp_code:
                        self._corp_code_map[corp_name] = corp_code

        # Save to cache
        print(f"Saving corp codes to local cache: {cache_file}")
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(self._corp_code_map, f, ensure_ascii=False, indent=2)

        print(f"Successfully loaded {len(self._corp_code_map)} company codes.")
        return self._corp_code_map

    def get_corp_code_by_name(self, company_name: str) -> Optional[str]:
        """Helper to get a corp_code for a specific company name."""
        if not self._corp_code_map:
            self.get_corp_codes()
        return self._corp_code_map.get(company_name)

    def get_disclosures(self, corp_code: str = "", bgn_de: str = "", end_de: str = "", pblntf_detail_ty: str = "D001", page_no: int = 1, page_count: int = 100) -> List[Dict]:
        """
        Fetches disclosures (공시검색) using the list.json API.
        
        Args:
            corp_code: DART 고유번호 (Optional if fetching all, but usually required for performance)
            bgn_de: 시작일 (YYYYMMDD)
            end_de: 종료일 (YYYYMMDD)
            pblntf_detail_ty: 공시상세유형 (D001="주식등의대량보유상황보고서", D002="임원ㆍ주요주주특정증권등소유상황보고서")
            page_no: 페이지 번호
            page_count: 페이지 건수 (1~100)
            
        Returns:
            List of dictionaries containing disclosure summary data.
        """
        url = f"{self.BASE_URL}/list.json"
        
        params = {
            "crtfc_key": self.api_key,
            "page_no": page_no,
            "page_count": page_count,
            "pblntf_detail_ty": pblntf_detail_ty,
        }
        
        if corp_code:
            params["corp_code"] = corp_code
        if bgn_de:
            params["bgn_de"] = bgn_de
        if end_de:
            params["end_de"] = end_de

        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get("status") == "000":
            return data.get("list", [])
        elif data.get("status") == "013":
            # 013: 조회된 데이타가 없습니다.
            return []
        else:
            print(f"API Error: {data.get('message')}")
            return []

if __name__ == "__main__":
    # Test block
    client = DARTClient()
    
    # 1. Test getting corp codes
    codes = client.get_corp_codes()
    
    # Check for Samsung Electronics
    samsung_code = client.get_corp_code_by_name("삼성전자")
    print(f"삼성전자 DART Corp Code: {samsung_code}")
    
    # 2. Test fetching insider disclosures 
    if samsung_code:
        print("\nFetching recent 5% rule (D001) / Insider holding disclosures (D002) for Samsung...")
        
        end_date = datetime.now()
        bgn_date = end_date - timedelta(days=180) # Past 6 months
        
        # Test D002: 임원ㆍ주요주주특정증권등소유상황보고서
        insider_reports = client.get_disclosures(
            corp_code=samsung_code, 
            pblntf_detail_ty="D002",
            bgn_de=bgn_date.strftime("%Y%m%d"),
            end_de=end_date.strftime("%Y%m%d")
        )
        print(f"Found {len(insider_reports)} D002 reports from the past 6 months.")
        for report in insider_reports[:3]:
            print(f"- [{report.get('rcept_dt')}] {report.get('corp_name')}: {report.get('report_nm')} ({report.get('flr_nm')})")
