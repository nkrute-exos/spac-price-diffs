"""Code to take an Excel sheet of spac cash per share in trust compared to the previous closing price
   and group by the month and year. Then only show the highest n number of price diffs"""
import numpy as np
import pandas as pd
import datetime
import copy
from outsideLiquidationDate import LiquidationDates


class NDiffs:
    file_path = "/Users/nicholaskrute/Documents/SPAC_Price_by_diffs/"
    headers = ["Issuer Name", "Common Ticker", "Remaining Life (months)", "Previous Closing Price",
               "Cash per Share in Trust", "Redeem Date", "Ann. YTM - Last Reported", "IPO Size ($m)"]
    spac_headers = ["SPAC_Issuer_", "SPAC_Ticker_", "SPAC_Price_", "SPAC_Cash_in_Trust_", "SPAC_Redeem_Date_",
                    "SPAC_IPO_Size_", "SPAC_Num_Shares_"]
    start_date = pd.to_datetime("5/1/2022")  # taken from the original doc in sheet2
    end_date = pd.to_datetime("5/1/2024")
    initial_investment = 100000

    @staticmethod
    def read_and_process_in_data(file_name: str) -> pd.DataFrame:
        spac_data = pd.read_excel(file_name)
        spac_data = spac_data.iloc[6:, 1:]  # want to figure out how to replace this
        spac_data.columns = NDiffs.headers
        spac_data = spac_data.reset_index(drop=True)
        spac_data["Redeem Date"] = pd.to_datetime(spac_data["Redeem Date"]) + pd.offsets.MonthBegin(1)
        liquidation_data = LiquidationDates.get_liquidation_dates()
        spac_data = LiquidationDates.update_redeem_dates(spac_data, liquidation_data)
        spac_data["Exp Date Year"] = spac_data["Redeem Date"].dt.strftime('%Y')
        spac_data["Exp Date Month"] = spac_data["Redeem Date"].dt.strftime('%m')
        spac_data["Profit Per 100K"] = (NDiffs.initial_investment / spac_data["Previous Closing Price"]) \
                                       * (spac_data["Cash per Share in Trust"] - spac_data["Previous Closing Price"])
        return spac_data

    def rank_by_diff_and_month(self, dataset: pd.DataFrame, n: int, month_year_cutoff: str = "1/1/1970"):
        sorted_data = dataset.sort_values(by=["Exp Date Year", "Exp Date Month", "Profit Per 100K"],
                                          ascending=[True, True, False])
        sorted_data = sorted_data.groupby('Redeem Date').head(n)
        sorted_data = sorted_data[["Issuer Name", "Common Ticker", "Remaining Life (months)", "Previous Closing Price",
                                   "Cash per Share in Trust", "Redeem Date", "Ann. YTM - Last Reported",
                                   "IPO Size ($m)", "Profit Per 100K"]]

        month_year_cutoff_obj = datetime.datetime.strptime(month_year_cutoff, "%m-%Y")
        sorted_data["Redeem Date"] = pd.to_datetime(sorted_data["Redeem Date"], format="%m-%Y")
        sorted_data = sorted_data[sorted_data["Redeem Date"] > month_year_cutoff_obj]
        sorted_data = sorted_data.set_index(sorted_data["Redeem Date"])
        sorted_data.index.name = "Index Date"
        self.find_shares_and_price(sorted_data)
        len_sorted_data = len(sorted_data)
        index_with_missing_dates = self.generate_index(start_date=sorted_data["Redeem Date"].iloc[0],
                                                       end_date=sorted_data["Redeem Date"].iloc[-1], n=n)
        extra_rows = pd.DataFrame(0, index=np.arange(len(index_with_missing_dates) - len(sorted_data)),
                                  columns=["Issuer Name", "Common Ticker", "Remaining Life (months)",
                                           "Previous Closing Price", "Cash per Share in Trust", "Redeem Date",
                                           "Ann. YTM - Last Reported", "IPO Size ($m)", "Profit Per 100K",
                                           "Number of Shares", "PnL"])
        sorted_data = pd.concat([sorted_data, extra_rows], axis=0)
        fully_missing_dates = index_with_missing_dates.difference(sorted_data["Redeem Date"])
        date_counts = sorted_data["Redeem Date"].value_counts()
        partially_missing_dates = date_counts[date_counts < n]

        missing_dates_partial = []
        for partially_missing_date in partially_missing_dates.iteritems():
            partially_missing_date_count = n - partially_missing_date[1]
            for x in range(partially_missing_date_count):
                missing_dates_partial.append(partially_missing_date[0])

        fully_missing_dates = list(np.repeat(fully_missing_dates, n))
        for date in missing_dates_partial:
            fully_missing_dates.append(date)

        sorted_data["Redeem Date"].iloc[len_sorted_data:] = fully_missing_dates
        sorted_data = sorted_data.sort_values(by=["Redeem Date"],
                                              ascending=[True])
        sorted_data.set_index(sorted_data["Redeem Date"], inplace=True)
        return sorted_data

    @staticmethod
    def find_weighted_ipo(dataset: pd.DataFrame) -> None:
        dataset["Weighted IPO"] = dataset["IPO Size ($m)"] \
                                  / dataset.groupby("Redeem Date Month Year")["IPO Size ($m)"].sum()

    @staticmethod
    def find_shares_and_price(dataset: pd.DataFrame) -> None:
        dataset["Number of Shares"] = NDiffs.initial_investment / dataset["Previous Closing Price"]
        dataset["PnL"] = (dataset["Cash per Share in Trust"] - dataset["Previous Closing Price"]) \
                         * dataset["Number of Shares"]

    def only_keep_top_n_spacs(self, dataset: pd.DataFrame, n: int) -> pd.DataFrame:
        rows_to_keep = [0] * n
        greatest_prices = [-99] * n
        greatest_price_rows = []
        for count, row in enumerate(dataset.iterrows(), start=1):
            current_price = float(row[1]["PnL"])
            if current_price >= greatest_prices[0]:
                greatest_prices[0] = float(current_price)
                rows_to_keep[0] = row[1].values
                try:
                    greatest_prices, rows_to_keep = zip(*sorted(zip(greatest_prices, rows_to_keep)))
                except ValueError:
                    continue
                greatest_prices = list(greatest_prices)
                rows_to_keep = list(rows_to_keep)

            if count % n == 0:
                greatest_price_rows.append(copy.deepcopy(rows_to_keep))

        df_highest_prices_repeated = pd.DataFrame(np.row_stack(greatest_price_rows))
        df_highest_prices_repeated.columns = ["Issuer Name", "Common Ticker", "Remaining Life (months)",
                                              "Previous Closing Price", "Cash per Share in Trust", "Redeem Date",
                                              "Ann. YTM - Last Reported", "IPO Size ($m)", "Profit Per 100K",
                                              "Number of Shares",
                                              "PnL"]
        date_index = self.generate_index(dataset.index[0], dataset.index[-1], n)
        df_highest_prices_repeated["Date"] = date_index[0:len(df_highest_prices_repeated.index)]
        df_highest_prices_repeated.set_index("Date", inplace=True)
        return df_highest_prices_repeated

    def generate_single_row_from_top_n_spacs(self, dataset: pd.DataFrame, n: int) -> pd.DataFrame:
        single_rows = []
        date_range = pd.date_range(start=dataset.index[0],
                                   end=dataset.index[-1], freq="MS")

        average_dataset = pd.DataFrame(index=date_range)
        average_dataset.index.name = "Date Index"
        group_by_obj = dataset.groupby(["Date Index"])
        average_dataset["Average Closing Price"] = group_by_obj["Previous Closing Price"].mean()
        average_dataset["Average Trust Value"] = group_by_obj["Cash per Share in Trust"].mean()
        average_dataset["Number of Shares"] = group_by_obj["Number of Shares Weighted"].sum()
        average_dataset["Profit Per 100K"] = group_by_obj["Profit Per 100K"].sum()
        for row in average_dataset.iterrows():
            ranked_to_go_through = dataset[dataset.index == row[0]]
            row_in_list = list(row[1].values)
            for ranked in ranked_to_go_through.iterrows():
                row_in_list.append(list(ranked)[1][0])  # issuer
                row_in_list.append(list(ranked)[1][1])  # ticker
                row_in_list.append(list(ranked)[1][3])  # price
                row_in_list.append(list(ranked)[1][4])  # cash in trust
                row_in_list.append(list(ranked)[1][5])  # redeem date
                row_in_list.append(list(ranked)[1][7])  # IPO size
                row_in_list.append(list(ranked)[1][11])  # number of shares
            single_rows.append(row_in_list)
        column_names = self.generate_column_names(n)
        average_dataset = pd.DataFrame(np.row_stack(single_rows), index=date_range, columns=column_names)
        return average_dataset

    @staticmethod
    def generate_index(start_date: datetime.date, end_date: datetime.date, n: int) -> list[datetime.date]:
        date_range = pd.date_range(start=start_date,
                                   end=end_date, freq="MS")
        date_range = np.repeat(date_range, n)
        return date_range

    @staticmethod
    def calc_number_of_shares_to_buy(dataset: pd.DataFrame, prices: dict, n: int) -> pd.DataFrame:
        dataset["Shares to Cover Evenly Split"] = 0
        for k, v in prices.items():
            divided_price = v / n
            dataset.loc[dataset.index == k, "Shares to Cover Evenly Split"] = \
                divided_price / dataset.loc[dataset.index == k, "Previous Closing Price"]
        return dataset

    @staticmethod
    def generate_column_names(n: int) -> list[str]:
        column_list = ["Average Closing Price", "Average Trust Value", "Number of Shares", "Profit Per 100K"]
        for n in range(n):
            for col in NDiffs.spac_headers:
                column_list.append(str(col + str(n + 1)))
        return column_list

    @staticmethod
    def extend_dataset(dataset: pd.DataFrame, end_date: datetime.date, n: int) -> pd.DataFrame:
        date_range = pd.date_range(start=dataset.index[-1] + pd.offsets.DateOffset(1),
                                   end=end_date, freq="MS")
        new_dates_len = len(date_range)
        date_range_repeated = np.repeat(date_range, n)
        last_n_rows = dataset[dataset.index == dataset.index[-1]]
        removed_index = last_n_rows.reset_index()
        extra_dates = pd.concat([removed_index]*new_dates_len, ignore_index=True)
        extra_dates.index = date_range_repeated
        #extra_dates = extra_dates.iloc[:, :-1]
        return pd.concat([dataset, extra_dates])

    @staticmethod
    def validate_length_of_index(dataset: pd.DataFrame, new_dataset: pd.DataFrame) -> bool:
        len_of_index_dataset = len(dataset.index)
        len_of_index_new_dataset = len(new_dataset.index)
        return len_of_index_dataset == len_of_index_new_dataset

    @staticmethod
    def write_data(dataset: pd.DataFrame, file_output_path: str) -> None:
        dataset.to_csv(file_output_path)

    @staticmethod
    def validate_no_out_of_range_dates(dataset: pd.DataFrame) -> bool:
        boolean_check_series = dataset.index >= dataset["Redeem Date"]
        boolean_check = False in boolean_check_series
        return boolean_check


if __name__ == "__main__":
    amount_to_cover = {"10/1/22": 4611111.11, "11/1/22": 4722222.22, "12/1/22": 4722222.22, "1/1/23": 4722222.22,
                       "2/1/23": 4722222.22, "3/1/23": 4722222.22, "4/1/23": 4722222.22, "5/1/23": 4722222.22,
                       "6/1/23": 4722222.22, "7/1/23": 4722222.22, "8/1/23": 4722222.22, "9/1/23": 4722222.22,
                       "10/1/23": 4722222.22, "11/1/23": 4722222.22}
    columns_to_keep = ["Issuer Name", "Common Ticker", "Previous Closing Price", "Cash per Share in Trust",
                       "Redeem Date", "Profit Per 100K", "Number of Shares", "PnL", "Shares to Cover Evenly Split"]
    num_spacs = 8
    ndiffs = NDiffs()
    returned_spac_data = ndiffs.read_and_process_in_data("".join([ndiffs.file_path,
                                                                  "in_data/", "2022_04_05_SPAC_reported_yields.xlsx"]))

    full_out = ndiffs.rank_by_diff_and_month(returned_spac_data, num_spacs, month_year_cutoff="05-2022")
    highest_prices = ndiffs.only_keep_top_n_spacs(full_out, num_spacs)
    highest_prices = ndiffs.calc_number_of_shares_to_buy(highest_prices, amount_to_cover, num_spacs)

    date_check = ndiffs.validate_no_out_of_range_dates(highest_prices[columns_to_keep])
    if date_check:
        print("ERROR IN DATES, PLEASE VALIDATE THE FIELDS")
    else:
        extended_data = ndiffs.extend_dataset(highest_prices[columns_to_keep], pd.to_datetime("5/1/2024"), num_spacs)
        ndiffs.write_data(full_out, "".join([ndiffs.file_path, "out_data/", "full_out.csv"]))
        ndiffs.write_data(extended_data, "".join([ndiffs.file_path, "out_data/", "ranked_data.csv"]))
