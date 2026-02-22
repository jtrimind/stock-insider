import os
import sys
import io
import re
import zipfile
import json
import xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from typing import Dict, List, Optional
from datetime import datetime, timedelta

# Ensure print statements correctly output Korean characters on Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

class DARTClient:
    BASE_URL = "https://opendart.fss.or.kr/api"

    @staticmethod
    def get_document_url(rcept_no: str) -> str:
        """Helper to generate a direct link to the DART web viewer for a specific report."""
        return f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

    @staticmethod
    def _parse_html_table(table_soup) -> List[List[str]]:
        """Parse HTML table into a 2D list handling rowspans and colspans."""
        rows = table_soup.find_all('TR')
        spanned_cells = {}
        grid = []
        
        for r_idx, row in enumerate(rows):
            cells = row.find_all(['TH', 'TD', 'TE', 'TU', 'th', 'td', 'te', 'tu'])
            row_data = []
            c_idx = 0
            
            while (r_idx, c_idx) in spanned_cells:
                row_data.append(spanned_cells[(r_idx, c_idx)])
                c_idx += 1
                
            for cell in cells:
                while (r_idx, c_idx) in spanned_cells:
                    row_data.append(spanned_cells[(r_idx, c_idx)])
                    c_idx += 1
                    
                text = cell.get_text(separator=' ', strip=True).replace('\n', '')
                
                rowspan = int(cell.get('rowspan') or cell.get('ROWSPAN') or 1)
                colspan = int(cell.get('colspan') or cell.get('COLSPAN') or 1)
                
                for i in range(rowspan):
                    for j in range(colspan):
                        if i == 0 and j == 0:
                            continue
                        spanned_cells[(r_idx + i, c_idx + j)] = text
                        
                for _ in range(colspan):
                    row_data.append(text)
                    c_idx += 1
                    
            grid.append(row_data)
            
        return grid

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        """Parses various Korean date formats into YYYY-MM-DD ISO8601 string."""
        if not date_str or date_str == "-":
            return "-"
        
        # Match YYYY, MM, DD using greedy match on digits
        match = re.search(r"(\d{4})[^\d]*(\d{1,2})[^\d]*(\d{1,2})[^\d]*", date_str)
        if match:
            year, month, day = match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d}"
            
        return date_str.strip()

    @staticmethod
    def _extract_trade_info(grid: List[List[str]]) -> List[Dict[str, str]]:
        """Extracts Reason, Date, Change, and Unit Price dynamically from the grid."""
        if not grid or len(grid) < 2:
            return []
            
        reason_idx, date_idx, change_idx, price_idx = -1, -1, -1, -1
        
        for r in range(min(3, len(grid))):
            for c, val in enumerate(grid[r]):
                if "보고사유" in val:
                    reason_idx = c
                if "변동일" in val:
                    date_idx = c
                if "증감" in val:
                    change_idx = c
                if "단가" in val:
                    price_idx = c
                    
        if reason_idx == -1 or change_idx == -1 or price_idx == -1:
            return []
            
        trades = []
        for row in grid[2:]:
            if "합계" in row[0].replace(" ", "") or "총계" in row[0]:
                continue
                
            if len(row) > max(reason_idx, change_idx, price_idx):
                reason = row[reason_idx]
                change = row[change_idx]
                price = row[price_idx]
                raw_date = row[date_idx] if date_idx != -1 and len(row) > date_idx else "-"
                date = DARTClient._normalize_date(raw_date)
                
                if change and change != "-":
                    trades.append({
                        "reason": reason,
                        "date": date,
                        "change": change,
                        "price": price
                    })
                    
        return trades

    def get_insider_trade_details(self, rcept_no: str) -> List[Dict[str, str]]:
        """Fetches the official XML document from DART and parses the detailed trade history."""
        url = f"{self.BASE_URL}/document.xml"
        params = {
            "crtfc_key": self.api_key,
            "rcept_no": rcept_no
        }
        
        try:
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                for name in z.namelist():
                    if name.endswith(".xml"):
                        with z.open(name) as f:
                            xml_content = f.read().decode('utf-8')
                            soup = BeautifulSoup(xml_content, "xml")
                            tables = soup.find_all("TABLE")
                            
                            target_table = None
                            for table in tables:
                                text = table.get_text(strip=True)
                                if "보고사유" in text and ("단가" in text or "증감" in text or "소유주식수" in text):
                                    target_table = table
                                    break
                            
                            if not target_table:
                                return []
                            
                            grid = self._parse_html_table(target_table)
                            return self._extract_trade_info(grid)
            return []
        except Exception as e:
            # print(f"Failed to fetch details for {rcept_no}: {e}")
            return []

    def __init__(self, api_key: Optional[str] = None):
        if api_key:
            self.api_key = api_key
        else:
            # Try Streamlit Secrets first, then fallback to OS Environment variables
            try:
                import streamlit as st
                self.api_key = st.secrets.get("DART_API_KEY", os.getenv("DART_API_KEY"))
            except Exception:
                self.api_key = os.getenv("DART_API_KEY")
                
        if not self.api_key:
            raise ValueError("DART_API_KEY is not set in Streamlit secrets, environment variables, or passed to the client.")
        
        # Internal cache for corp_code mapping
        self._corp_data_map: Dict[str, Dict[str, str]] = {}
        # Path for the cache directory
        self._cache_dir = os.path.join(os.path.dirname(__file__), '.cache')
        os.makedirs(self._cache_dir, exist_ok=True)

    def get_corp_codes(self, force_refresh: bool = False) -> Dict[str, Dict[str, str]]:
        """Fetches and parses the corpCode.xml to map company names back to DART corp_codes and KRX stock_codes.
        Uses a local JSON file cache per day to prevent redundant downloads."""
        if self._corp_data_map and not force_refresh:
            return self._corp_data_map

        # Use current date as cache key (YYYYMMDD) - V2 bust cache for stock code addition
        today_str = datetime.now().strftime("%Y%m%d")
        cache_file = os.path.join(self._cache_dir, f"corp_codes_v2_{today_str}.json")

        if not force_refresh and os.path.exists(cache_file):
            print(f"Loading corp codes from local cache: {cache_file}")
            with open(cache_file, 'r', encoding='utf-8') as f:
                self._corp_data_map = json.load(f)
            return self._corp_data_map

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
                    stock_code = list_item.findtext('stock_code')
                    if corp_name and corp_code:
                        self._corp_data_map[corp_name] = {
                            "corp_code": corp_code,
                            "stock_code": stock_code.strip() if stock_code else ""
                        }

        # Save to cache
        print(f"Saving corp codes to local cache: {cache_file}")
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(self._corp_data_map, f, ensure_ascii=False, indent=2)

        print(f"Successfully loaded {len(self._corp_data_map)} company codes.")
        return self._corp_data_map

    def get_corp_code_by_name(self, company_name: str) -> Optional[str]:
        """Helper to get a corp_code for a specific company name."""
        if not self._corp_data_map:
            self.get_corp_codes()
        data = self._corp_data_map.get(company_name)
        return data["corp_code"] if data else None

    def get_company_info_by_name(self, company_name: str) -> Optional[Dict[str, str]]:
        """Helper to get both corp_code and stock_code for a specific company name."""
        if not self._corp_data_map:
            self.get_corp_codes()
        return self._corp_data_map.get(company_name)

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
