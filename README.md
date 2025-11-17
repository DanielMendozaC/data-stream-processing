# README file
# data-stream-processing


US Stock Market Hours:

Open: Monday-Friday, 9:30 AM - 4:00 PM ET
Closed: Nights, weekends, holidays


Create Alpaca Keys when log in -> Home, you will see a dashboard, scroll down, on the right side,
you'll see API keys, get your own. Add API key and secrets to the .env file.

To run the consumer service:

uvicorn stock_data_producer_realtime:app --reload