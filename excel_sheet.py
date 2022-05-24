import pandas as pd
import datetime
from draw_models import NormalDistributionLoan, LinearDrawsLoan
from typing import Any
from pyxirr import xirr


class ExcelSheet:
    @staticmethod
    def build_empty_excel_sheet(amount_to_cover: Any,
                                start_date: datetime.date,
                                end_date: datetime.date) -> pd.DataFrame:
        columns = ["Date"]

        date_range = pd.date_range(start=start_date, end=end_date, freq="MS")

        df = pd.DataFrame(0, index=range(len(date_range)), columns=columns)
        df["Date"] = date_range

        for date, amount in amount_to_cover.items():
            df.loc[df["Date"] == date, "Amount"] = amount

        return df

    @staticmethod
    def get_redeem_dates(ranked_data: pd.DataFrame, sheet: pd.DataFrame) -> pd.DataFrame:
        for date in sheet["Date"]:
            date = pd.to_datetime(date).strftime("%Y-%m-%d")
            sliced_data = ranked_data.loc[ranked_data["Unnamed: 0"] == date]
            try:
                sheet.loc[sheet["Date"] == date, "Redeem Date"] = sliced_data["Redeem Date"].iloc[0]
            except IndexError:
                sheet.loc[sheet["Date"] == date, "Redeem Date"] = 0
        return sheet

    @staticmethod
    def get_weighted_avg_price(ranked_data: pd.DataFrame, sheet: pd.DataFrame) -> pd.DataFrame:
        sheet["Weighted Avg. Price"] = 0
        price_dict = dict(ranked_data.groupby("Unnamed: 0")["Previous Closing Price"].mean())
        for date, price in price_dict.items():
            date = pd.to_datetime(date) + pd.DateOffset(months=1)
            sheet.loc[sheet["Date"] == date, "Weighted Avg. Price"] = price
        return sheet

    @staticmethod
    def get_avg_trust_per_share(ranked_data: pd.DataFrame, sheet: pd.DataFrame) -> pd.DataFrame:
        sheet["Avg. Trust per Share"] = 0
        price_dict = dict(
            ranked_data.groupby("Unnamed: 0")["Predicted Cash in Trust"].mean())
        for date, price in price_dict.items():
            date = pd.to_datetime(date) + pd.DateOffset(months=1)
            sheet.loc[sheet["Date"] == date, "Avg. Trust per Share"] = price
        return sheet

    @staticmethod
    def get_redeem_date(ranked_data: pd.DataFrame, sheet: pd.DataFrame) -> pd.DataFrame:
        sheet["Redeem Date"] = ranked_data["Redeem Date"]
        return sheet

    @staticmethod
    def get_shares(sheet: pd.DataFrame) -> pd.DataFrame:
        sheet["# Shares"] = sheet["Amount"] / sheet["Weighted Avg. Price"]
        return sheet

    @staticmethod
    def get_mv(sheet: pd.DataFrame) -> pd.DataFrame:
        sheet["MV"] = sheet["# Shares"] * sheet["Weighted Avg. Price"]
        return sheet

    @staticmethod
    def get_prem(sheet: pd.DataFrame) -> pd.DataFrame:
        sheet["Prem"] = (sheet["Avg. Trust per Share"] - sheet["Weighted Avg. Price"]) * sheet["# Shares"]
        return sheet

    @staticmethod
    def get_value_out(sheet: pd.DataFrame) -> pd.DataFrame:
        values = sheet["Weighted Avg. Price"] * sheet["# Shares"]
        return sum(values)

    def get_port_prin(self, sheet: pd.DataFrame) -> pd.DataFrame:
        sheet["Port Prin"] = 0
        for date, val in sheet.groupby('Redeem Date')["MV"].sum().items():
            sheet.loc[sheet["Date"] == date, "Port Prin"] = val
        sheet["Port Prin"][0] = self.get_value_out(sheet) * -1
        return sheet

    @staticmethod
    def get_port_prem(sheet: pd.DataFrame) -> pd.DataFrame:
        sheet["Port Prem"] = 0
        for date, val in sheet.groupby('Redeem Date')["Prem"].sum().items():
            sheet.loc[sheet["Date"] == date, "Port Prem"] = val
        return sheet

    @staticmethod
    def get_port_cf(sheet: pd.DataFrame) -> pd.DataFrame:
        sheet["Port CF"] = sheet["Port Prin"] + sheet["Port Prem"]
        return sheet

    @staticmethod
    def percent_return(sheet: pd.DataFrame) -> float:
        percent = xirr(sheet[["Date", "Port CF"]])
        return percent

linear_draws = NormalDistributionLoan(loan_amount=99750000,
                                      draw_percent=.15,
                                      loan_duration=24,
                                      start_date=datetime.date(2022, 9, 1),
                                      end_date=datetime.date(2024, 8, 1))
amount_to_cover = linear_draws.calculate_draws()

sheet = ExcelSheet()
empty_df = sheet.build_empty_excel_sheet(amount_to_cover, datetime.date(2022, 5, 1), datetime.date(2025, 5, 1))
ranked_data = pd.read_csv("/Users/nicholaskrute/Documents/SPAC_Price_by_diffs/out_data/ranked_data.csv")

empty_df = sheet.get_redeem_dates(ranked_data=ranked_data, sheet=empty_df)
empty_df = sheet.get_weighted_avg_price(ranked_data=ranked_data, sheet=empty_df)
empty_df = sheet.get_avg_trust_per_share(ranked_data=ranked_data, sheet=empty_df)
empty_df = sheet.get_shares(sheet=empty_df)
empty_df = sheet.get_mv(sheet=empty_df)
empty_df = sheet.get_prem(sheet=empty_df)
empty_df = empty_df.fillna(0)
empty_df = sheet.get_port_prin(sheet=empty_df)
empty_df = sheet.get_port_prem(sheet=empty_df)
empty_df = sheet.get_port_cf(sheet=empty_df)
irr = sheet.percent_return(sheet=empty_df)

#with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
#    print(empty_df)
print("IRR =", round(irr*100, 2))
