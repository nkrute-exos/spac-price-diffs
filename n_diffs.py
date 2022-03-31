"""Code to take an Excel sheet of spac cash per share in trust compared to the previous closing price
   and group by the month and year. Then only show the highest n number of price diffs"""
import typing

import numpy as np
import pandas as pd
import datetime
import copy


class NDiffs:
    file_path = "/Users/nicholaskrute/Documents/SPAC_Price_by_diffs/"
    headers = ["Issuer Name", "Common Ticket", "Remaining Life (months)", "Previous Closing Price",
               "Cash per Share in Trust", "Exp Date", "Ann. YTM - Last Reported", "IPO Size ($m)"]
    spac_headers = ["SPAC_Issuer_", "SPAC_Ticker_", "SPAC_Price_", "SPAC_Cash_in_Trust_", "SPAC_Redeem_Date_",
                    "SPAC_IPO_Size_", "SPAC_Num_Shares_"]
    start_date = pd.to_datetime("5/1/2022")  # taken from the original doc in sheet2
    end_date = pd.to_datetime("5/1/2024")
    initial_investment = 100000

    @staticmethod
    def read_and_process_in_data(file_name: typing.IO) -> pd.DataFrame:
        spac_data = pd.read_excel(file_name)
        spac_data = spac_data.iloc[6:, 1:]  # want to figure out how to replace this
        spac_data.columns = NDiffs.headers
        spac_data = spac_data.reset_index(drop=True)
        spac_data["Exp Date"] = pd.to_datetime(spac_data["Exp Date"]) + pd.offsets.MonthBegin(1)
        spac_data["Exp Date Month Year"] = spac_data["Exp Date"].dt.strftime('%m-%Y')
        spac_data["Exp Date Year"] = spac_data["Exp Date"].dt.strftime('%Y')
        spac_data["Exp Date Month"] = spac_data["Exp Date"].dt.strftime('%m')
        spac_data["Profit Per 100K"] = (NDiffs.initial_investment / spac_data["Previous Closing Price"]) \
                                      * (spac_data["Cash per Share in Trust"] - spac_data["Previous Closing Price"])
        return spac_data

    def rank_by_diff_and_month(self, dataset: pd.DataFrame, n: int = 1, month_year_cutoff: str = "1/1/1970"):
        sorted_data = dataset.sort_values(by=["Exp Date Year", "Exp Date Month", "Profit Per 100K"],
                                          ascending=[True, True, False])
        sorted_data = sorted_data.groupby('Exp Date Month Year').head(n)
        sorted_data = sorted_data[["Issuer Name", "Common Ticket", "Remaining Life (months)", "Previous Closing Price",
                                   "Cash per Share in Trust", "Exp Date", "Ann. YTM - Last Reported", "IPO Size ($m)",
                                   "Profit Per 100K", "Exp Date Month Year"]]

        month_year_cutoff_obj = datetime.datetime.strptime(month_year_cutoff, "%m-%Y")
        sorted_data["Exp Date Month Year"] = pd.to_datetime(sorted_data["Exp Date Month Year"], format="%m-%Y")
        sorted_data = sorted_data[sorted_data["Exp Date Month Year"] > month_year_cutoff_obj]
        sorted_data = sorted_data.set_index(sorted_data["Exp Date Month Year"])
        sorted_data.index.name = "Index Date"
        self.find_shares_and_price(sorted_data)
        return sorted_data

    @staticmethod
    def find_weighted_ipo(dataset: pd.DataFrame) -> None:
        dataset["Weighted IPO"] = dataset["IPO Size ($m)"] \
                                  / dataset.groupby("Exp Date Month Year")["IPO Size ($m)"].sum()

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
                                              "Previous Closing Price", "Cash per Share in Trust", "Exp Date",
                                              "Ann. YTM - Last Reported", "IPO Size ($m)", "Profit Per 100K",
                                              "Exp Date Month Year", "Number of Shares",
                                              "PnL"]
        date_index = self.generate_index(dataset.index[0], dataset.index[-1], n)
        df_highest_prices_repeated["Redeem Date"] = date_index[0:len(df_highest_prices_repeated.index)]
        df_highest_prices_repeated.set_index("Redeem Date", inplace=True)
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
    def generate_index(start_date: datetime.date, end_date: datetime.date, n: int ) -> list[datetime.date]:
        date_range = pd.date_range(start=start_date,
                                   end=end_date, freq="MS")
        date_range = np.repeat(date_range, n)
        return date_range

    @staticmethod
    def calc_number_of_shares_to_buy(dataset: pd.DataFrame, prices: dict, n: int) -> pd.DataFrame:
        dataset["Shares to Cover Evenly Split"] = 0
        for k, v in prices.items():
            divided_price = v / n
            dataset["Shares to Cover Evenly Split"][k] = divided_price / dataset["Previous Closing Price"][k]

        return dataset

    @staticmethod
    def generate_column_names(n: int) -> list[str]:
        column_list = ["Average Closing Price", "Average Trust Value", "Number of Shares", "Profit Per 100K"]
        for n in range(n):
            for col in NDiffs.spac_headers:
                column_list.append(str(col+str(n+1)))
        return column_list

    def extend_dataset(self, dataset: pd.DataFrame, end_date: datetime.date) -> pd.DataFrame:
        date_range = pd.date_range(start=dataset.index[0],
                                   end=end_date, freq="MS")
        reindexed_data = dataset.reindex(date_range)
        self.forward_fill_missing_data(reindexed_data)
        return reindexed_data

    @staticmethod
    def validate_correct_number_of_fields_per_group(dataset: pd.DataFrame, n: int) -> list[datetime.date]:
        boolean_check_of_nums = dataset.groupby(["Exp Date Month Year"]).size() == n
        invalid_dates = boolean_check_of_nums[boolean_check_of_nums == False]
        return invalid_dates

    @staticmethod
    def validate_length_of_index(dataset: pd.DataFrame, new_dataset: pd.DataFrame) -> bool:
        len_of_index_dataset = len(dataset.index)
        len_of_index_new_dataset = len(new_dataset.index)
        return len_of_index_dataset == len_of_index_new_dataset

    @staticmethod
    def forward_fill_missing_data(dataset: pd.DataFrame, n: int) -> None:
        final_n_valid_rows = dataset[dataset.index == dataset.index[-1]]
        dataset.ffill(final_n_valid_rows, inplace=True)

    @staticmethod
    def write_data(dataset: pd.DataFrame, file_output_path: str) -> None:
        dataset.to_csv(file_output_path)


if __name__ == "__main__":
    amount_to_cover = {"10/1/22": 4611111.11, "11/1/22": 4722222.22,  "12/1/22": 4722222.22, "1/1/23": 4722222.22,
                       "2/1/23": 4722222.22, "3/1/23": 4722222.22, "4/1/23": 4722222.22, "5/1/23": 4722222.22,
                       "6/1/23": 4722222.22, "7/1/23": 4722222.22, "8/1/23": 4722222.22, "9/1/23": 4722222.22,
                       "10/1/23": 4722222.22, "11/1/23": 4722222.22}
    columns_to_keep = ["Issuer Name", "Common Ticker", "Previous Closing Price", "Cash per Share in Trust", "Exp Date",
                       "Profit Per 100K", "Number of Shares", "PnL", "Shares to Cover Evenly Split"]
    num_spacs = 8
    ndiffs = NDiffs()
    returned_spac_data = ndiffs.read_and_process_in_data("".join([ndiffs.file_path,
                                                         "2022_03_29 - SPAC reported yields.xlsx"]))
    ndiffs.write_data(returned_spac_data, "".join([ndiffs.file_path, "full_out.csv"]))

    full_out = ndiffs.rank_by_diff_and_month(returned_spac_data, num_spacs, month_year_cutoff="05-2022")
    highest_prices = ndiffs.only_keep_top_n_spacs(full_out, num_spacs)
    highest_prices = ndiffs.calc_number_of_shares_to_buy(highest_prices, amount_to_cover, num_spacs)

    ndiffs.write_data(full_out, "".join([ndiffs.file_path, "full_out.csv"]))
    ndiffs.write_data(highest_prices[columns_to_keep], "".join([ndiffs.file_path, "ranked_data.csv"]))
