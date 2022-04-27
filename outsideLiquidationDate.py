from cdm_metadata_client import Client
from pandas.tseries.offsets import BDay
import datetime as dt
import pandas as pd
import numpy as np


class CDMData:

    @staticmethod
    def create_spac_research_file() -> pd.DataFrame:
        today = dt.date.today()
        use_date = today - BDay(1)
        t_f = use_date.strftime('%Y-%m-%d')
        client = Client('www.kubepocapi.aws.aurotfp.com/cdm-metadata-query-service')

        spac_research = 'exos.tfp.spac_research.securities.typed.'
        record_spac_research = client.get_record(spac_research + t_f)
        df_spac_research = record_spac_research.download()
        x = df_spac_research.explode('symbols').reset_index(drop=True)
        y = pd.json_normalize(x['symbols'])
        spac_research_df = pd.concat([x, y], axis=1)

        spac_research_df = spac_research_df[spac_research_df["type"] == "common"].reset_index(drop=True)
        return spac_research_df

    @staticmethod
    def create_liquidation_connector() -> pd.DataFrame:
        today = dt.date.today()
        use_date = today - BDay(1)
        t_f = use_date.strftime('%Y-%m-%d')

        client = Client('www.kubeprodapi.aws.aurotfp.com/cdm-metadata-query-service')

        spac_research = 'exos.tfp.spac_research.securities.typed.'
        record_spac_research = client.get_record(spac_research + t_f)
        df_spac_research = record_spac_research.download()
        return df_spac_research

    def get_liquidation_dates(self) -> pd.DataFrame:
        df_spac_research = self.create_spac_research_file()
        later_date_spacs = df_spac_research[df_spac_research["endDate"] < df_spac_research["outsideLiquidationDate"]]
        later_date_spacs = pd.DataFrame(later_date_spacs).reset_index(drop=True)
        return later_date_spacs

    @staticmethod
    def update_redeem_dates(dataset: pd.DataFrame, liquidation_dates: pd.DataFrame) -> pd.DataFrame:
        for row in liquidation_dates.iterrows():
            ticker = row[1]["rootSymbol"]
            liquid_date = pd.to_datetime(row[1]["outsideLiquidationDate"])
            dataset.to_csv("/Users/nicholaskrute/Documents/SPAC_Price_by_diffs/out_data/data.csv")
            if ticker in dataset["Common Ticker"].values:
                idx = np.where(dataset['Common Ticker'] == ticker)
                dataset.loc[dataset.index == idx[0][0], "Redeem Date"] = liquid_date + pd.offsets.MonthBegin(1)
        return dataset

    def get_expected_prices_at_liquidation(self, dataset: pd.DataFrame) -> pd.DataFrame:
        df_spac_research = self.create_spac_research_file()
        cdm_dataset = df_spac_research[["symbol", "estimatedCashAtLiquidation"]]
        for row in dataset.iterrows():
            ticker = row[1][1]
            newer_price = cdm_dataset[cdm_dataset["symbol"] == ticker]
            if len(newer_price) == 1:
                print("UPDATING PRICE PER SHARE IN TRUST FOR", ticker)
                dataset.loc[dataset["Common Ticker"] == ticker, "Cash per Share in Trust"] = \
                    newer_price["estimatedCashAtLiquidation"].values[0]
        return dataset

    @staticmethod
    def get_historical_prices(price_data: str, look_back: int = 1) -> pd.DataFrame:  # from price table
        df = pd.read_excel(price_data, sheet_name="spac_price_data")
        new_header = df.iloc[0]
        df = df[1:]
        df.columns = new_header
        return df.loc[2: 1+look_back].dropna(axis="columns")

    @staticmethod
    def get_yields(price_data: str) -> pd.DataFrame:
        df = pd.read_excel(price_data, sheet_name="Price Statistics")
        tickers = []
        yields = []
        columns = df.columns[3:]
        for ticker in columns:
            tickers.append(ticker)
            yields.append(df[ticker].loc[15])
        return pd.DataFrame(data={"Tickers": tickers, "Yields": yields})

    def get_spacs_from_dashboard(self, board: str, start_date: str, look_back: int = 5) -> pd.DataFrame:
        df = pd.read_excel(board, sheet_name="spac_research")
        df_filtered = df[(df["Announced"] == False)
                         & (df['type'] == 'common')
                         & (df['endDate'] > start_date)]
        df_filtered.reset_index(inplace=True)
        cantor_like_data = df_filtered[['name', 'symbol', 'Days to Go2', 'Market Price', 'estimatedCashAtLiquidation',
                                        'endDate', 'outsideLiquidationDate', 'YTM']]
        cantor_like_data["Exp Date"] = np.where(~df_filtered['outsideLiquidationDate'].isnull(),
                                                df_filtered['outsideLiquidationDate'], df_filtered['endDate'])
        cantor_like_data["IPO Size ($m)"] = 0
        cantor_like_data["Exp Date"] = pd.to_datetime(cantor_like_data["Exp Date"], format="%Y-%m-%d")
        cantor_like_data = cantor_like_data[['name', 'symbol', 'Days to Go2', 'Market Price',
                                             'estimatedCashAtLiquidation', 'Exp Date', 'YTM',
                                             'IPO Size ($m)']]

        columns = ["Issuer Name", "Common Ticker", "Remaining Life (Months)", "Previous Closing Price",
                   "Cash per Share in Trust", "Exp Date", "Ann. YTM - Last Reported", "IPO Size ($m)"]
        cantor_like_data.columns = columns
        df_to_return = cantor_like_data

        spac_price_data = self.get_historical_prices(board, look_back=look_back)
        for row in df_to_return.iterrows():
            issuer_name = row[1]["Issuer Name"]
            try:
                index = spac_price_data[issuer_name].first_valid_index()
                if index is None:
                    df_to_return.loc[df_to_return["Issuer Name"] == issuer_name, "Previous Closing Price"] = None
                else:
                    df_to_return.loc[df_to_return["Issuer Name"] == issuer_name, "Previous Closing Price"] = \
                        float(spac_price_data[issuer_name][index])
            except KeyError:
                df_to_return.loc[df_to_return["Issuer Name"] == issuer_name, "Previous Closing Price"] = None

        df_to_return = df_to_return.loc[df_to_return["Previous Closing Price"] > 8]
        df_to_return = df_to_return[~df_to_return['Previous Closing Price'].isnull()]
        return df_to_return.reset_index(drop=True)
