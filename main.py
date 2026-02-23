import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
from dart_client import DARTClient
from stock_data import StockDataClient

# Configuration
st.set_page_config(page_title="Stock Insider Tracker", page_icon="📈", layout="wide")

@st.cache_resource
def get_dart_client():
    return DARTClient()

@st.cache_data(ttl=3600) # Cache for 1 hour to prevent spamming the API
def fetch_disclosures_for_period(days: int = 7, corp_code: str = ""):
    """Fetch insider trading disclosures for the last N days, optionally filtered by corp_code.
    Safely chunks requests into 90-day intervals to bypass DART API's 3-month limit."""
    client = get_dart_client()
    
    # Needs corp_codes mapped internally first
    client.get_corp_codes()
    
    end_date = datetime.now()
    bgn_date = end_date - timedelta(days=days)
    
    # Chunk into 90-day intervals
    all_reports = []
    current_start = bgn_date
    
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=89), end_date)
        
        # D002: 임원ㆍ주요주주특정증권등소유상황보고서
        reports = client.get_disclosures(
            corp_code=corp_code, 
            pblntf_detail_ty="D002",
            bgn_de=current_start.strftime("%Y%m%d"),
            end_de=current_end.strftime("%Y%m%d"),
            page_count=100
        )
        if reports:
            all_reports.extend(reports)
            
        current_start = current_end + timedelta(days=1)
    
    # Parse into DataFrame
    if all_reports:
        df = pd.DataFrame(all_reports)
        # Handle potential duplicates if ranges overlap
        df = df.drop_duplicates(subset=['rcept_no'])
        # Generate the direct viewer links
        df["viewer_url"] = df["rcept_no"].apply(client.get_document_url)
        return df
    return pd.DataFrame()

@st.cache_data(ttl=3600*24)
def get_trade_details(rcept_no: str):
    """Fetch details for a specific report. Cached to prevent redundant XML fetching."""
    client = get_dart_client()
    return client.get_insider_trade_details(rcept_no)

def process_and_flatten_trades(df: pd.DataFrame) -> pd.DataFrame:
    """Extract XML details and flatten into individual transaction rows."""
    if df.empty:
        return pd.DataFrame()

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
    display_df.columns = ["공시일", "기업명", "보고자(임원/주주)", "변동일(거래일)", "보고사유", "증감(주)", "단가(원)", "원문 링크"]
    display_df = display_df.sort_values(by=["변동일(거래일)", "공시일"], ascending=[False, False])
    return display_df

def render_market_feed(days_to_fetch: int):
    st.subheader(f"최근 {days_to_fetch}일 시장 전체 변동내역 피드")
    
    with st.spinner("DART에서 데이터를 불러오는 중..."):
        df = fetch_disclosures_for_period(days=days_to_fetch)
        
    if df.empty:
        st.info("해당 기간 동안의 내부자 거래 공시가 없습니다.")
        return

    display_df = process_and_flatten_trades(df)
    
    st.dataframe(
        display_df,
        column_config={
            "원문 링크": st.column_config.LinkColumn("DART 뷰어 확인"),
        },
        width="stretch",
        hide_index=True
    )

def render_stock_detail(company_name: str, days_to_fetch: int):
    client = get_dart_client()
    info = client.get_company_info_by_name(company_name)
    
    if not info:
        st.error(f"'{company_name}'에 대한 상장사 정보를 DART에서 찾을 수 없습니다. (이름을 정확히 입력해주세요)")
        return
        
    corp_code = info["corp_code"]
    stock_code = info["stock_code"]
    
    st.subheader(f"🏢 {company_name} (종목코드: {stock_code if stock_code else '비상장'}) 상세 분석")
    
    # 1. Fetch Disclosures for this specific company
    with st.spinner(f"{company_name}의 공시 내역을 불러오는 중..."):
        df_disclosures = fetch_disclosures_for_period(days=days_to_fetch, corp_code=corp_code)
    
    display_df = pd.DataFrame()
    if not df_disclosures.empty:
        display_df = process_and_flatten_trades(df_disclosures)
        
    # 2. Fetch and render Stock Price Chart if stock_code exists
    if stock_code:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_to_fetch)
        
        with st.spinner(f"주가 데이터를 불러오는 중 (종목코드: {stock_code})..."):
            df_price = StockDataClient.get_ohlcv(
                stock_code=stock_code,
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d")
            )
        
        if not df_price.empty:
            # Ensure price date is a datetime object for charting
            df_price['date'] = pd.to_datetime(df_price['date'])
            
            # Base Line Chart for Stock Price
            base = alt.Chart(df_price).encode(
                x=alt.X('date:T', title='날짜'),
            )
            
            line = base.mark_line(color='gray', strokeWidth=2).encode(
                y=alt.Y('close:Q', title='종가 (원)', scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip('date:T', title='날짜', format='%Y-%m-%d'),
                    alt.Tooltip('close:Q', title='종가', format=',')
                ]
            )
            
            chart = line
            
            # 3. Add Insider Trades Overlay if they exist
            if not display_df.empty:
                # Filter rows where a valid trade date exists
                valid_trades = display_df[display_df['변동일(거래일)'] != '-'].copy()
                if not valid_trades.empty:
                    valid_trades['trade_date_dt'] = pd.to_datetime(valid_trades['변동일(거래일)'])
                    
                    # We need to map the stock price to the trade date so the marker sits exactly on the line
                    # Merge on nearest date or just use the date. 
                    valid_trades = valid_trades.sort_values('trade_date_dt')
                    df_price_sorted = df_price.sort_values('date')
                    
                    # Merge closing price into our trades so we know where to plot them vertically
                    merged_trades = pd.merge_asof(
                        valid_trades,
                        df_price_sorted,
                        left_on='trade_date_dt',
                        right_on='date',
                        direction='nearest'
                    )
                    
                    # Differentiate Buy vs Sell based on Reason or Change(Positive/Negative)
                    # For simplicity, we can look at the "보고사유" or "증감(주)" string lengths/contents
                    # Y/N Buy/Sell simple heuristic based on "장내매수", "장내매도"
                    def determine_action(reason):
                        if "매수" in reason or "취득" in reason:
                            return "장내매수(+)"
                        elif "매도" in reason or "처분" in reason:
                            return "장내매도(-)"
                        return "기타변동"
                        
                    merged_trades['action'] = merged_trades['보고사유'].apply(determine_action)
                    
                    # Color domain mapping
                    domain = ["장내매수(+)", "장내매도(-)", "기타변동"]
                    range_ = ["#00a86b", "#e91e63", "#ffc107"] # Green for buy, Pink for sell, Yellow for other
                    
                    points = alt.Chart(merged_trades).mark_circle(size=100, opacity=1).encode(
                        x='date:T',
                        y='close:Q',
                        color=alt.Color('action:N', scale=alt.Scale(domain=domain, range=range_), title="거래 유형"),
                        tooltip=[
                            alt.Tooltip('trade_date_dt:T', title='실제 변동일', format='%Y-%m-%d'),
                            alt.Tooltip('보고자(임원/주주):N', title='보고자'),
                            alt.Tooltip('보고사유:N', title='보고사유'),
                            alt.Tooltip('증감(주):N', title='수량 변동'),
                            alt.Tooltip('단가(원):N', title='취득/처분 단가'),
                            alt.Tooltip('close:Q', title='당일 종가', format=',')
                        ]
                    )
                    
                    chart = alt.layer(line, points).resolve_scale(color='independent')
            
            st.altair_chart(chart.interactive(), use_container_width=True)
        else:
            st.warning("선택한 기간(또는 휴장일) 동안의 주가 데이터를 가져올 수 없습니다.")
    else:
        st.info("비상장 종목이라 주가 차트를 지원하지 않습니다.")

    # 4. Render Table
    if not display_df.empty:
        st.markdown(f"**최근 {days_to_fetch}일 내부자 거래 내역 ({len(display_df)}건)**")
        st.dataframe(
            display_df,
            column_config={
                "원문 링크": st.column_config.LinkColumn("DART 뷰어 확인"),
            },
            width="stretch",
            hide_index=True
        )
    else:
        st.info(f"최근 {days_to_fetch}일 동안 {company_name}의 내부자 거래 공시가 없습니다.")

def main():
    st.title("📈 Stock Insider (한국 주식 내부자 거래 추적)")
    st.markdown("임원 및 주요주주의 최근 지분 변동 공시를 확인하고 가격 차트와 비교 분석합니다.")
    
    st.sidebar.header("검색 및 필터 (Search & Filters)")
    
    search_query = st.sidebar.text_input("종목 검색 (예: 삼성전자)", placeholder="기업명 입력 후 Enter")
    days_to_fetch = st.sidebar.slider("조회 기간 (최근 N일)", min_value=1, max_value=365, value=7)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ☕ 개발자 후원하기")
    st.sidebar.markdown(
        """
        <a href="https://www.buymeacoffee.com/kimbndt" target="_blank">
            <img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 40px !important;width: 145px !important;" >
        </a>
        <br><br>
        <span style="font-size: 0.8em; color: gray;">불쌍한 개발자에게 도움을!</span>
        """,
        unsafe_allow_html=True
    )
    
    if search_query:
        render_stock_detail(search_query.strip(), days_to_fetch)
    else:
        render_market_feed(days_to_fetch)

if __name__ == "__main__":
    main()
