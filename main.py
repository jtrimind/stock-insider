import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from dart_client import DARTClient

# Configuration
st.set_page_config(page_title="Stock Insider Tracker", page_icon="📈", layout="wide")

@st.cache_resource
def get_dart_client():
    return DARTClient()

@st.cache_data(ttl=3600) # Cache for 1 hour to prevent spamming the API
def fetch_recent_disclosures(days: int = 7):
    """Fetch insider trading disclosures for the last N days across all companies."""
    client = get_dart_client()
    
    # Needs corp_codes mapped internally first
    client.get_corp_codes()
    
    end_date = datetime.now()
    bgn_date = end_date - timedelta(days=days)
    
    # We call with no corp_code to fetch everything globally for the day range
    # D002: 임원ㆍ주요주주특정증권등소유상황보고서
    reports = client.get_disclosures(
        corp_code="", 
        pblntf_detail_ty="D002",
        bgn_de=bgn_date.strftime("%Y%m%d"),
        end_de=end_date.strftime("%Y%m%d"),
        page_count=100
    )
    
    # Parse into DataFrame
    if reports:
        df = pd.DataFrame(reports)
        # Generate the direct viewer links
        df["viewer_url"] = df["rcept_no"].apply(client.get_document_url)
        return df
    return pd.DataFrame()

@st.cache_data(ttl=3600*24)
def get_trade_details(rcept_no: str):
    """"Fetch details for a specific report. Cached to prevent redundant XML fetching."""
    client = get_dart_client()
    return client.get_insider_trade_details(rcept_no)

def main():
    st.title("📈 Stock Insider (한국 주식 내부자 거래 추적)")
    st.markdown("임원 및 주요주주의 최근 지분 변동 공시를 1주일 단위로 확인합니다.")
    
    # Sidebar filtering
    st.sidebar.header("검색 필터 (Filters)")
    days_to_fetch = st.sidebar.slider("조회 기간 (최근 N일)", min_value=1, max_value=30, value=7)
    
    # Fetch Data
    with st.spinner("DART에서 데이터를 불러오는 중..."):
        df = fetch_recent_disclosures(days=days_to_fetch)
        
    if df.empty:
        st.info("해당 기간 동안의 내부자 거래 공시가 없습니다.")
        return

    # Fetch specific trade details for the displayed DataFrame
    progress_bar = st.progress(0, text="공시 원문에서 세부 변동사항 추출 중...")
    
    flattened_trades = []
    total_rows = len(df)
    for idx, row in enumerate(df.itertuples()):
        trades = get_trade_details(row.rcept_no)
        if trades:
            for t in trades:
                flattened_trades.append({
                    "rcept_dt": row.rcept_dt,
                    "corp_name": row.corp_name,
                    "flr_nm": row.flr_nm,
                    "trade_date": t.get("date", "-"),
                    "reason": t.get("reason", "-"),
                    "change": t.get("change", "-"),
                    "price": t.get("price", "-"),
                    "viewer_url": row.viewer_url
                })
        else:
            flattened_trades.append({
                "rcept_dt": row.rcept_dt,
                "corp_name": row.corp_name,
                "flr_nm": row.flr_nm,
                "trade_date": "-",
                "reason": "-",
                "change": "-",
                "price": "-",
                "viewer_url": row.viewer_url
            })
        progress_bar.progress((idx + 1) / total_rows, text=f"공시 원문 분석 중... ({idx+1}/{total_rows})")
    
    progress_bar.empty()
    
    display_df = pd.DataFrame(flattened_trades)

    # Clean up columns for display
    display_df.columns = ["공시일", "기업명", "보고자(임원/주주)", "변동일(거래일)", "보고사유", "증감(주)", "단가(원)", "원문 링크"]
    
    # Sort by '변동일(거래일)' descending if available, else fallback to '공시일'
    display_df = display_df.sort_values(by=["변동일(거래일)", "공시일"], ascending=[False, False])
    
    st.subheader(f"최근 {days_to_fetch}일 변동내역 피드 ({len(display_df)}건)")
    
    # Display the dataframe with clickable links
    st.dataframe(
        display_df,
        column_config={
            "원문 링크": st.column_config.LinkColumn("DART 뷰어 확인"),
        },
        width="stretch",
        hide_index=True
    )

if __name__ == "__main__":
    main()
