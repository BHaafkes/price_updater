import pandas as pd
import os
import json
from flask import Flask
import firebase_admin
from firebase_admin import credentials, firestore

# --- Flask App & Firebase Initialisatie ---
app = Flask(__name__)
try:
    if not firebase_admin._apps:
        print("➡️ Initialiseren van Firebase App...")
        creds_json_string = os.environ.get('FIRESTORE_CREDENTIALS')
        if not creds_json_string:
            raise ValueError("FIRESTORE_CREDENTIALS secret niet gevonden.")
        creds_dict = json.loads(creds_json_string)
        cred = credentials.Certificate(creds_dict)
        firebase_admin.initialize_app(cred)
        print("✅ Firebase App succesvol geïnitialiseerd.")
except Exception as e:
    print(f"❌ FOUT tijdens initialisatie van Firebase: {e}")

# --- Configuratie ---
MARKETCAP_URL = "https://companiesmarketcap.com/?download=csv"
TRACKING_TABLES = [
    "magic_formula_buys_track",
    "magic_formula_sells_track",
    "intelligent_investor_buys_track",
    "combined_model_buys_track"
]

@app.route('/')
def run_price_update():
    """Haalt live prijzen op en werkt de meest recente snapshot bij."""
    try:
        db = firestore.client()
        print("➡️ Ophalen van live marktdata...")
        market_cap_df = pd.read_csv(MARKETCAP_URL)
        prices_df = market_cap_df[['Symbol', 'price (USD)']].rename(columns={
            'Symbol': 'Ticker',
            'price (USD)': 'current_price'
        })
        
        for table in TRACKING_TABLES:
            try:
                # Vind de meest recente snapshot datum
                collection_ref = db.collection(table)
                query = collection_ref.order_by('snapshot_date', direction=firestore.Query.DESCENDING).limit(1)
                docs = query.stream()
                
                most_recent_date = next(docs).to_dict()['snapshot_date'] if docs else None

                if most_recent_date:
                    print(f"Updating prijzen voor {table} van snapshot-datum: {most_recent_date}")
                    
                    # Haal alle documenten op met die datum
                    docs_to_update = collection_ref.where('snapshot_date', '==', most_recent_date).stream()
                    
                    batch = db.batch()
                    updated_count = 0
                    for doc in docs_to_update:
                        doc_data = doc.to_dict()
                        ticker = doc_data.get('Ticker')
                        
                        # Zoek de nieuwe prijs op
                        new_price_row = prices_df[prices_df['Ticker'] == ticker]
                        if not new_price_row.empty:
                            new_price = new_price_row.iloc[0]['current_price']
                            doc_ref = collection_ref.document(doc.id)
                            batch.update(doc_ref, {'current_price': new_price})
                            updated_count += 1
                    
                    batch.commit()
                    print(f"   - ✅ Prijzen bijgewerkt voor {updated_count} tickers.")
                else:
                    print(f"   - ⚠️ Collectie '{table}' is leeg. Niets om bij te werken.")

            except Exception as e:
                print(f"   - ❌ Fout bij verwerken van collectie '{table}': {e}")

        return "Prijsupdate succesvol voltooid.", 200

    except Exception as e:
        import traceback
        print("--- SCRIPT MISLUKT MET EEN FOUT ---")
        traceback.print_exc()
        return f"Er is een fout opgetreden: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
