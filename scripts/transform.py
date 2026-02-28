import pandas as pd
from extract import extract_data
def transform_data():
    df = extract_data()   # get dataframe from extract step

    # remove duplicates
    df = df.drop_duplicates()

    # remove null values
    df = df.dropna()

    # convert date column if exists
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    # create revenue column if possible
    if 'quantity' in df.columns and 'price' in df.columns:
        df['revenue'] = df['quantity'] * df['price']

    print("✅ Data transformed successfully")
    print(df.head())

    return df


if __name__ == "__main__":
    transform_data()