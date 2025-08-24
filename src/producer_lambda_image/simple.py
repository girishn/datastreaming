import yfinance as yf

# Fetch data for Microsoft
data = yf.download("V", period="1mo")

# Get historical market data
# data = msft.history(period="1mo")
print(data)

