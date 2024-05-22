import pandas as pd
import os

directory_path = '/home/xrdpuser/new_passivbot/passivbot/historical_data/ohlcvs_futures/BTCUSDT'
output_file_path = '/home/xrdpuser/Bitcoin-Prediction/combined_data.csv'

combined_df = pd.DataFrame()

for filename in os.listdir(directory_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory_path, filename)
        df = pd.read_csv(file_path)

        # Нормализация на заглавията на колоните (ако е необходимо)
        df.columns = [x.strip().title() for x in df.columns]

        # Проверка за наличие на необходимите колони
        if {'Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'}.issubset(df.columns):
            # Преименуване на колоната Volume
            df.rename(columns={'Volume': 'Volume_(BTC)'}, inplace=True)

            # Пресмятане на `Volume_(Currency)`
            df['Volume_(Currency)'] = df['Close'] * df['Volume_(BTC)']

            # Пресмятане на `Weighted_Price`
            df['Weighted_Price'] = (df['Open'] + df['High'] + df['Low'] + df['Close']) / 4

            # Проверка и преобразуване на Timestamp в datetime
            if isinstance(df['Timestamp'][0], str):
                df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            else:
                df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')

            # Избор на колони за крайния DataFrame
            df = df[['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume_(BTC)', 'Volume_(Currency)', 'Weighted_Price']]
            
            # Настройка на Timestamp като индекс
            df = df.set_index('Timestamp', drop=True)

            # Добавяне към обединения DataFrame
            combined_df = pd.concat([combined_df, df])

# Сортиране на DataFrame по колоната 'Timestamp'
combined_df.sort_index(inplace=True)

# Запис на обединения DataFrame
combined_df.to_csv(output_file_path)
print(f'Combined CSV file created at {output_file_path}')

