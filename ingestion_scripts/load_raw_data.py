import os
from dotenv import load_dotenv
import pandas as pd
import random
from faker import Faker
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import uuid

# --- LOAD ENVIRONMENT VARIABLES ---
load_dotenv()  # This reads the .env file in your current directory

# --- CONFIGURATION ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "fintech_dw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Number of records to generate
NUM_USERS = 500
NUM_ACCOUNTS = NUM_USERS * 1.5
NUM_PRODUCTS = 50
NUM_TRANSACTIONS_PER_ACCOUNT = 10
NUM_TRADES_PER_ACCOUNT = 5
NUM_LOGINS_PER_USER = 20

# Initialize Faker
fake = Faker()

# --- HELPER FUNCTIONS ---
def get_db_engine():
    """Creates a SQLAlchemy engine."""
    try:
        engine = create_engine(DATABASE_URL)
        # Test connection
        with engine.connect() as conn:
            print("Database connection successful.")
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def load_df_to_db(df, table_name, engine):
    """Loads a DataFrame into a specified table."""
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully loaded {len(df)} records into '{table_name}'.")
    except Exception as e:
        print(f"Error loading data into '{table_name}': {e}")

# --- DATA GENERATION FUNCTIONS ---

def generate_raw_users(n):
    print(f"Generating {n} users...")
    data = []
    for _ in range(n):
        data.append({
            'user_id': str(uuid.uuid4()),
            'created_at': fake.date_time_between(start_date="-2y", end_date="now", tzinfo=None),
            'email': fake.email(),
            'status': random.choice(['active', 'suspended', 'pending_verification'])
        })
    return pd.DataFrame(data)

def generate_raw_user_profiles(user_ids):
    print(f"Generating profiles for {len(user_ids)} users...")
    data = []
    for user_id in user_ids:
        data.append({
            'user_id': user_id,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'dob': fake.date_of_birth(minimum_age=18, maximum_age=70),
            'address': fake.address().replace('\n', ', ')
        })
    return pd.DataFrame(data)

def generate_raw_kyc_checks(user_ids):
    print(f"Generating KYC checks for {len(user_ids)} users...")
    data = []
    for user_id in user_ids:
        data.append({
            'kyc_id': str(uuid.uuid4()),
            'user_id': user_id,
            'status': random.choice(['approved', 'failed', 'pending']),
            'checked_at': fake.date_time_between(start_date="-2y", end_date="now", tzinfo=None)
        })
    return pd.DataFrame(data)

def generate_raw_accounts(user_ids, n):
    print(f"Generating {int(n)} accounts...")
    data = []
    for _ in range(int(n)):
        data.append({
            'account_id': str(uuid.uuid4()),
            'user_id': random.choice(user_ids),
            'account_type': random.choice(['checking', 'investment', 'savings']),
            'opened_at': fake.date_time_between(start_date="-2y", end_date="now", tzinfo=None)
        })
    return pd.DataFrame(data)

def generate_raw_products(n):
    print(f"Generating {n} products...")
    data = []
    for _ in range(n):
        asset_class = random.choice(['crypto', 'stock'])
        name = fake.company() if asset_class == 'stock' else f"{fake.word().capitalize()}coin"
        symbol = name[:4].upper() if asset_class == 'stock' else name[:3].upper()
        data.append({
            'product_id': str(uuid.uuid4()),
            'ticker_symbol': f"{symbol}{random.randint(1, 99)}",
            'name': name,
            'asset_class': asset_class
        })
    return pd.DataFrame(data)

def generate_raw_transactions(account_ids, n_per_account):
    print(f"Generating {len(account_ids) * n_per_account} transactions...")
    data = []
    for account_id in account_ids:
        for _ in range(n_per_account):
            trans_type = random.choice(['deposit', 'withdrawal'])
            amount = round(random.uniform(10.0, 5000.0), 2)
            if trans_type == 'withdrawal':
                amount *= -1

            data.append({
                'transaction_id': str(uuid.uuid4()),
                'account_id': account_id,
                'timestamp': fake.date_time_between(start_date="-2y", end_date="now", tzinfo=None),
                'amount': amount,
                'type': trans_type
            })
    return pd.DataFrame(data)

def generate_raw_trades(account_ids, product_ids, n_per_account):
    print(f"Generating {len(account_ids) * n_per_account} trades...")
    data = []
    for account_id in account_ids:
        for _ in range(n_per_account):
            data.append({
                'trade_id': str(uuid.uuid4()),
                'account_id': account_id,
                'product_id': random.choice(product_ids),
                'timestamp': fake.date_time_between(start_date="-2y", end_date="now", tzinfo=None),
                'quantity': round(random.uniform(0.01, 100.0), 4),
                'price_per_unit': round(random.uniform(1.0, 50000.0), 2),
                'trade_type': random.choice(['buy', 'sell'])
            })
    return pd.DataFrame(data)

def generate_raw_market_prices(product_ids):
    print(f"Generating market prices for {len(product_ids)} products...")
    data = []
    today = datetime.now().date()
    for product_id in product_ids:
        price = round(random.uniform(1.0, 50000.0), 2)
        for i in range(365 * 2): # Two years of daily prices
            date = today - timedelta(days=i)
            # Simple random walk for price
            price *= (1 + random.uniform(-0.05, 0.05))
            price = max(0.01, price) # Price can't be negative
            data.append({
                'price_id': str(uuid.uuid4()),
                'product_id': product_id,
                'date': date,
                'close_price': round(price, 2)
            })
    return pd.DataFrame(data)

def generate_raw_user_logins(user_ids, n_per_user):
    print(f"Generating {len(user_ids) * n_per_user} login events...")
    data = []
    for user_id in user_ids:
        for _ in range(n_per_user):
            data.append({
                'login_id': str(uuid.uuid4()),
                'user_id': user_id,
                'login_timestamp': fake.date_time_between(start_date="-2y", end_date="now", tzinfo=None)
            })
    return pd.DataFrame(data)

def generate_raw_marketing_attribution(user_ids):
    print(f"Generating marketing attribution for {len(user_ids)} users...")
    data = []
    for user_id in user_ids:
        data.append({
            'user_id': user_id,
            'signup_source': random.choice(['google', 'facebook_ad', 'organic', 'referral']),
            'campaign': random.choice(['q4_promo', 'summer_drive', 'none'])
        })
    return pd.DataFrame(data)

# --- MAIN EXECUTION ---
def main():
    print("--- Starting Data Ingestion ---")
    engine = get_db_engine()

    # Generate data in order of dependency
    df_users = generate_raw_users(NUM_USERS)
    user_ids = df_users['user_id'].tolist()

    df_user_profiles = generate_raw_user_profiles(user_ids)
    df_kyc_checks = generate_raw_kyc_checks(user_ids)
    df_accounts = generate_raw_accounts(user_ids, NUM_ACCOUNTS)
    account_ids = df_accounts['account_id'].tolist()

    df_products = generate_raw_products(NUM_PRODUCTS)
    product_ids = df_products['product_id'].tolist()

    df_transactions = generate_raw_transactions(account_ids, NUM_TRANSACTIONS_PER_ACCOUNT)
    df_trades = generate_raw_trades(account_ids, product_ids, NUM_TRADES_PER_ACCOUNT)
    df_market_prices = generate_raw_market_prices(product_ids)
    df_user_logins = generate_raw_user_logins(user_ids, NUM_LOGINS_PER_USER)
    df_marketing_attribution = generate_raw_marketing_attribution(user_ids)

    # Load all dataframes to Postgres
    load_df_to_db(df_users, 'raw_users', engine)
    load_df_to_db(df_user_profiles, 'raw_user_profiles', engine)
    load_df_to_db(df_kyc_checks, 'raw_kyc_checks', engine)
    load_df_to_db(df_accounts, 'raw_accounts', engine)
    load_df_to_db(df_products, 'raw_products', engine)
    load_df_to_db(df_transactions, 'raw_transactions', engine)
    load_df_to_db(df_trades, 'raw_trades', engine)
    load_df_to_db(df_market_prices, 'raw_market_prices', engine)
    load_df_to_db(df_user_logins, 'raw_user_logins', engine)
    load_df_to_db(df_marketing_attribution, 'raw_marketing_attribution', engine)

    print("--- Data Ingestion Complete ---")

if __name__ == "__main__":
    main()