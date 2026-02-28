import pandas as pd
from sqlalchemy import create_engine
from transform import transform_data
def load():
    try:
        # Step 1: Get cleaned dataframe
        df = transform_data()
        print("✅ Data received from transform step")

        # Step 2: Rename columns to match SQL table (IMPORTANT)
        df.columns = [
            "transaction_id",
            "customer_id",
            "gender",
            "age",
            "product_category",
            "quantity",
            "price",
            "discount",
            "revenue",
            "order_date"
        ]

        # Step 3: Create MySQL connection
        engine = create_engine(
            "mysql+pymysql://root:Dipu%402002@localhost/salesdb"
        )

        # Step 4: Test connection
        conn = engine.connect()
        print("✅ Connected to MySQL")
        conn.close()

        # Step 5: Load into MySQL table
        df.to_sql(
            name="sales_data",
            con=engine,
            if_exists="append",
            index=False
        )

        print("🎉 Data loaded into MySQL successfully!")

    except Exception as e:
        print("❌ Error while loading data:", e)


if __name__ == "__main__":
    load()