import pandas as pd
import os
from sqlalchemy import create_engine, text
from flask import Flask

app = Flask(__name__)

MARKETCAP_URL = "https://companiesmarketcap.com/?download=csv"
TRACKING_TABLES = [
    "magic_formula_buys_track",
    "magic_formula_sells_track",
    "intelligent_investor_buys_track",
    "combined_model_buys_track"
]

@app.route('/')
def run_price_update():
    """
    Fetches live prices and updates only the most recent snapshot in the tracking tables.
    """
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        if not DATABASE_URL:
            raise ValueError("No DATABASE_URL secret found.")
        
        engine = create_engine(DATABASE_URL)
        
        print("➡️ Fetching live market cap and price data...")
        market_cap_df = pd.read_csv(MARKETCAP_URL)
        prices_df = market_cap_df[['Symbol', 'price (USD)']].rename(columns={
            'Symbol': 'Ticker',
            'price (USD)': 'Price'
        })
        
        with engine.connect() as connection:
            for table in TRACKING_TABLES:
                try:
                    # Find the most recent snapshot date in the table
                    query = f"SELECT MAX(snapshot_date) FROM {table}"
                    result = connection.execute(text(query)).scalar()
                    
                    if result:
                        most_recent_date = result
                        print(f"Updating prices for {table} from snapshot date: {most_recent_date}")
                        
                        # Get the tickers from the most recent snapshot
                        query = f"SELECT \"Ticker\" FROM {table} WHERE snapshot_date = '{most_recent_date}'"
                        tickers_to_update = pd.read_sql(query, connection)
                        
                        # Merge with the live prices
                        update_data = pd.merge(tickers_to_update, prices_df, on='Ticker', how='left')
                        update_data.dropna(subset=['Price'], inplace=True)
                        
                        # Execute UPDATE statements
                        for _, row in update_data.iterrows():
                            update_query = text(f"""
                                UPDATE {table}
                                SET "Price" = :price
                                WHERE "Ticker" = :ticker AND snapshot_date = :date
                            """)
                            connection.execute(update_query, {"price": row['Price'], "ticker": row['Ticker'], "date": most_recent_date})
                        
                        print(f"   - ✅ Updated prices for {len(update_data)} tickers.")
                    else:
                        print(f"   - ⚠️ Table '{table}' is empty. Nothing to update.")

                except Exception as e:
                    print(f"   - ❌ Error processing table '{table}': {e}")
            
            # Important: Commit the transaction to save changes
            connection.commit()

        return "Price update script completed successfully.", 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return f"An error occurred: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)