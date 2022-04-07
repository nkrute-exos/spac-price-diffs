from cdm_metadata_client import Client
from pandas.tseries.offsets import BDay
import datetime as dt
import pandas as pd
import numpy as np


class LiquidationDates:
    @staticmethod
    def get_liquidation_dates():
        today = dt.date.today()
        use_date = today - BDay(1)
        t_f = use_date.strftime('%Y-%m-%d')

        client = Client('www.kubeprodapi.aws.aurotfp.com/cdm-metadata-query-service')

        spac_research = 'exos.tfp.spac_research.securities.typed.'
        record_spac_research = client.get_record(spac_research+t_f)
        df_spac_research = record_spac_research.download()

        later_date_spacs = df_spac_research[df_spac_research["endDate"] < df_spac_research["outsideLiquidationDate"]]
        return pd.DataFrame(later_date_spacs).reset_index(drop=True)

    @staticmethod
    def update_redeem_dates(dataset: pd.DataFrame, liquidation_dates: pd.DataFrame) -> pd.DataFrame:
        for row in liquidation_dates.iterrows():
            ticker = row[1]["rootSymbol"]
            liquid_date = pd.to_datetime(row[1]["outsideLiquidationDate"])
            if ticker in dataset["Common Ticker"].values:
                idx = np.where(dataset['Common Ticker'] == ticker)
                dataset.loc[dataset.index == idx[0][0], "Redeem Date"] = liquid_date + pd.offsets.MonthBegin(1)
        return dataset
