import plotly.express as px
import yfinance as yf
import streamlit as st
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from streamlit_extras.metric_cards import style_metric_cards

def main():
    st.subheader("Track Performance of a Specific Stock over time")
    st.sidebar.write("Get Data for a Specific Stock and track its performance over time")

    stock = st.sidebar.text_input("Enter a Stock Ticker", "SNOW")
    start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime('2015-01-01'), min_value=pd.to_datetime('2010-01-01'))
    end_date = st.sidebar.date_input("End Date", value=pd.to_datetime('today'), min_value=start_date, max_value=pd.to_datetime('today'))
    submit = st.sidebar.button("Get Data", use_container_width=True)
    st.sidebar.subheader("Made with ❤️ by [Kanak](https://kanakmi.streamlit.app/)")

    if submit:
        try:
            df = get_data_from_snowflake(stock, start_date, end_date)
            st.balloons()
            render_ui(df, stock)
        except Exception as e:
            print(e)
            st.error("Invalid Ticker, please try again", icon="🚨")
    else:
        st.info("Enter a Stock Ticker and Date Range to get started", icon="💡")

def render_ui(df, stock):
    st.title(f"{stock} Stock Price")
    last_two_days = df.tail(2)
    change = last_two_days.iloc[1]['Close'] - last_two_days.iloc[0]['Close']
    change_percent = (change/last_two_days.iloc[0]['Close'])*100

    change = round(change, 2)
    change_percent = round(change_percent, 2)

    c1, c2 = st.columns(2)
    c1.metric(label="Current Price", value=round(last_two_days.iloc[1]['Close'], 3), delta=str(change))
    c2.metric(label="Day Change", value=change, delta=str(change_percent)+"%")
    c1.metric(label="Highest Price (in given date range)", value=round(df['High'].max(), 3))
    c2.metric(label="Lowest Price (in given date range)", value=round(df['Low'].min(), 3))
    c1.metric(label="Highest Single Day Profit (in given date range)", value=str(df['Daily_Return_Percent'].max())+"%")
    c2.metric(label="Highest Single Day Loss (in given date range)", value=str(df['Daily_Return_Percent'].min())+"%")

    style_metric_cards()

    tab1, tab2, tab3, tab4 = st.tabs(["Price Over Time Chart", "Daily Return % Chart", "Daily Volume Trade Chart", "Complete Data"])

    with tab1:
        st.plotly_chart(interactive_plot(df, 'Adj_Close', f'{stock} Stock Price'), use_container_width=True)
    with tab2:
        st.plotly_chart(interactive_plot(df, 'Daily_Return_Percent', f'{stock} Daily Return %'), use_container_width=True)
    with tab3:
        st.plotly_chart(interactive_plot(df, 'Volume', f'{stock} Daily Volume Trade'), use_container_width=True)
    with tab4:
        st.dataframe(df, use_container_width=True)
        csv = convert_df(df)
        c1, c2, c3 = st.columns([2,2,1])
        c2.download_button(
            label="Download Data as CSV", 
            data=csv, 
            file_name=f"{stock}_data.csv", 
            mime='text/csv'
        )

@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

def interactive_plot(df, y_data, title):
    fig = px.line(x=df['Date'], y=df[y_data], title=title, labels={'x': 'Date', 'y': y_data})
    return fig

@st.cache_data
def download_stock_data(ticker, start_date, end_date):
    stock_data = yf.download(ticker, start_date, end_date)
    if len(stock_data) == 0:
        return pd.DataFrame()
    df = pd.DataFrame(stock_data)
    df = df.reset_index()
    df['Date'] = df['Date'].dt.date
    df['Ticker'] = ticker
    df.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
    df['Daily_Return_Percent'] = df['Adj_Close']
    for j in range(1, len(df)):
        df['Daily_Return_Percent'][j] = round(((df['Adj_Close'][j]-df['Adj_Close'][j-1])/df['Adj_Close'][j-1])*100, 2)
    df['Daily_Return_Percent'][0] = 0
    return df

@st.cache_resource
def snowflake_connector():
    # Connect to Snowflake
    conn = create_engine(URL(
        user=st.secrets['user'],
        password=st.secrets['password'],
        account=st.secrets['account'],
        database=st.secrets['database'],
        schema=st.secrets['schema']
    ))
    return conn

@st.cache_data
def get_data_from_snowflake(ticker, start_date, end_date):
    conn = snowflake_connector()
    cur = conn.execute(f"SHOW TABLES LIKE '{ticker}'")
    result = cur.fetchone()
    if not result:
        df = download_stock_data(ticker, start_date, end_date)
        df.to_sql(ticker, conn, if_exists='replace', index=False, method='multi')
        print(f"Data for {ticker} inserted into Snowflake.")
    else:
        # Get the starting and ending date of the data already present in the table
        cur = conn.execute(f"SELECT MIN(\"Date\"), MAX(\"Date\") FROM {ticker}")
        result = cur.fetchone()
        min_date = result[0]
        max_date = result[1]
        if min_date > start_date:
            # Fetch the data from start_date to min_date
            df = download_stock_data(ticker, start_date, min_date)
            if len(df)>0:
                df = df[df['Date'] < min_date]
                df.to_sql(ticker, conn, if_exists='append', index=False, method='multi')
        if max_date < end_date:
            # Fetch the data from max_date to end_date
            df = download_stock_data(ticker, max_date, end_date)
            if len(df)>0:
                df = df[df['Date'] > max_date]
                df.to_sql(ticker, conn, if_exists='append', index=False, method='multi')
        
        # Fetch the data from start_date to end_date
        cur = conn.execute(f"SELECT * FROM {ticker} WHERE \"Date\" >= '{start_date}' AND \"Date\" <= '{end_date}'")
        result = cur.fetchall()
        df = pd.DataFrame(result)
        df.columns = result[0].keys()
        df = df.sort_values(by=['Date'])
    conn.dispose()
    return df

if __name__ == "__main__":
    main()