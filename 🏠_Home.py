import streamlit as st

st.set_page_config(
    page_title="Stock Comparison App",
    page_icon="❄️",
    initial_sidebar_state="expanded"
)

markdown = """
# 📈 Stock Comparison App

This app allows you to compare the performance of different stocks over time.

## 🖥️ Usage

1. Enter a specific stock ticker or multiple stock tickers you want to compare, separated by commas.
2. Choose a start and end date for the data.
3. Click the "Get Data" button to retrieve and display the data.
4. View the metrics and analysis for each stock.

**Note:** When multiple stocks are selected, the app will compare the data for dates that are common to all stocks. For example, if you select AAPL, GOOG, and AMZN, the app will only compare the data for dates that are common to all three stocks.

## 💹 Metrics

The app displays the following metrics for each stock:

- **Absolute Stock Price Comparison:** A line chart comparing the absolute adjusted close price of each stock over time.
- **Normalized Stock Price Comparison:** A line chart comparing the normalized adjusted close price of each stock over time.
- **Daily Change % Comparison:** A line chart comparing the daily percentage change of each stock over time.
- **Volume Comparison:** A line chart comparing the trading volume of each stock over time.
- **Normalized Volume Comparison:** A line chart comparing the normalized trading volume of each stock over time.
- **Daily Change % Distribution:** A histogram of the daily percentage change distribution for each stock.

## 📒 Analysis

For each metric, the app provides an analysis of which stock performed better during the selected time period and why. 

## 🔮 Future Improvements

- Add more comparison metrics and analysis.
- Allow users to select different chart types and styles.
- Improve error handling and user feedback.

---
"""

st.markdown(markdown, unsafe_allow_html=True)

st.sidebar.subheader("Made with ❤️ by [Kanak](https://kanakmi.streamlit.app/)")
