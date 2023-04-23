import plotly.graph_objs as go
import plotly.figure_factory as ff
import yfinance as yf
import streamlit as st
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from streamlit_extras.metric_cards import style_metric_cards

def main():
    st.subheader("Compare the performance of different stocks over time")
    st.sidebar.write("Compare the performance of different stocks over time")

    stocks = st.sidebar.text_input("Enter Stock Tickers, separated by commas", "AAPL,GOOG,AMZN,SNOW")
    stocks_list = [stock.strip().upper() for stock in stocks.split(",")]
    start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime('2015-01-01'), min_value=pd.to_datetime('2010-01-01'))
    end_date = st.sidebar.date_input("End Date", value=pd.to_datetime('today'), min_value=start_date, max_value=pd.to_datetime('today'))
    submit = st.sidebar.button("Get Data", use_container_width=True)
    st.sidebar.subheader("Made with ❤️ by [Kanak](https://kanakmi.streamlit.app/)")

    if submit:
        try:
            data = get_stock_data(stocks_list, start_date, end_date)
            st.balloons()
            render_ui(data)
        except Exception as e:
            print(e)
            st.error("Invalid Ticker, please try again", icon="🚨")
    else:
        st.info("Enter Stock Tickers and Date Range to get started", icon="💡")

def render_ui(data):
    metric_cards(data)
    get_interactive_plot(data, 'Adj_Close', "Absolute Stock Price Comparison", "Date", "Adjusted Close Price")
    get_normalised_interactive_plot(data, 'Adj_Close', "Normalized Stock Price Comparison", "Date", "Normalized Price")
    get_interactive_plot(data, 'Daily_Return_Percent', "Daily Return % Comparison", "Date", "Daily Return %")
    get_interactive_plot(data, 'Volume', "Volume Comparison", "Date", "Volume")
    get_normalised_interactive_plot(data, 'Volume', "Normalized Volume Comparison", "Date", "Normalized Volume")

    # create distplot of daily return % for each stock, also draw a curve showing volatility. Use figure factory
    fig = ff.create_distplot([df['Daily_Return_Percent'] for df in data], [df['Ticker'][0] for df in data], bin_size=0.5, show_rug=False)
    fig.update_layout(title="Daily Return % Distribution", xaxis_title="Daily Return %", yaxis_title="Probability Density")
    st.plotly_chart(fig, use_container_width=True)

def metric_cards(data):
    stocks = len(data)
    c = st.columns(stocks)
    # create a card for each stock showing last price and change in price from last date
    i = 0
    for df in data:
        df = df.tail(1)
        df = df.reset_index(drop=True)
        ticker = df['Ticker'][0]
        last_price = round(df['Adj_Close'][0], 2)
        change = df['Daily_Return_Percent'][0]
        c[i].metric(label=ticker, value=last_price, delta=str(change)+"%")
        i+=1
    style_metric_cards()

def get_interactive_plot(data, plot_type, title, xaxis_title, yaxis_title):
    fig = go.Figure()

    for i, df in enumerate(data):
        ticker = df['Ticker'][0]
        fig.add_trace(go.Scatter(x=df['Date'], y=df[plot_type], name=ticker))
    
    fig.update_layout(title=title, xaxis_title=xaxis_title, yaxis_title=yaxis_title)
    st.plotly_chart(fig, use_container_width=True)

def get_normalised_interactive_plot(data, plot_type, title, xaxis_title, yaxis_title):
    fig = go.Figure()

    for i, df in enumerate(data):
        ticker = df['Ticker'][0]
        df['Normalized Value'] = df[plot_type]/df[plot_type][0]
        fig.add_trace(go.Scatter(x=df['Date'], y=df['Normalized Value'], name=ticker))
    
    fig.update_layout(title=title, xaxis_title=xaxis_title, yaxis_title=yaxis_title)
    st.plotly_chart(fig, use_container_width=True)

@st.cache_data
def get_stock_data(tickers, start_date, end_date):
    data = []
    for ticker in tickers:
        df = get_data_from_snowflake(ticker, start_date, end_date)
        data.append(df)

    min_date = max(df['Date'].min() for df in data)
    filtered_data = []
    for df in data:
        filtered_df = df[df['Date'] >= min_date]
        filtered_df = filtered_df.reset_index(drop=True)
        filtered_data.append(filtered_df)

    del data
    return filtered_data

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
