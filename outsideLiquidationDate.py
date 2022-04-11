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
        client = Client('www.kubeprodapi.aws.aurotfp.com/cdm-metadata-query-service')

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
