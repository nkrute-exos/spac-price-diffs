import datetime

import pandas as pd
import numpy as np
import parameters.parameters as params


class SharedColumns:

    @staticmethod
    def create_dataframe() -> pd.DataFrame:
        date_range = pd.date_range(start=pd.to_datetime(params.start_date),
                                   end=pd.to_datetime(params.refinance_date), freq="MS")
        header_dataframe = pd.DataFrame(index=range(0, len(date_range)))
        header_dataframe["Date"] = date_range
        header_dataframe["WAL Years"] = (round((header_dataframe["Date"] - pd.to_datetime(params.start_date)) /
                                               np.timedelta64(1, "M"), 1) * 30) / 360
        header_dataframe["Libor"] = params.libor
        return header_dataframe
