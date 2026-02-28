import pandas as pd

def extract_data():
    df = pd.read_csv(r'C:\Users\dipud\OneDrive\Desktop\sales_etl_pipeline\data\retail_sales_dataset.csv')
    print("Data extracted successfully!")
    print(df.head())
    return df

if __name__ == "__main__":
    extract_data()