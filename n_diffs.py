"""Code to take an Excel sheet of spac cash per share in trust compared to the previous closing price
   and group by the month and year. Then only show the highest n number of price diffs"""
import numpy as np
import pandas as pd
import datetime
import copy


class NDiffs:
    file_path = "/Users/nicholaskrute/Documents/SPAC_Price_by_diffs/"
    headers = ["Issuer Name", "Common Ticket", "Remaining Life (months)", "Previous Closing Price",
               "Cash per Share in Trust", "Exp Date", "Ann. YTM - Last Reported", "IPO Size ($m)"]
    start_date = pd.to_datetime("5/1/2022")  # taken from the original doc in sheet2
    end_date = pd.to_datetime("5/1/2024")
    initial_investment = 100000

    @staticmethod
    def read_and_process_in_data(file_name):
        spac_data = pd.read_excel(file_name)
        spac_data = spac_data.iloc[6:, 1:]
        spac_data.columns = NDiffs.headers
        spac_data = spac_data.reset_index(drop=True)
        spac_data["Exp Date"] = pd.to_datetime(spac_data["Exp Date"]) + pd.offsets.MonthBegin(1)
        spac_data["Exp Date Month Year"] = spac_data["Exp Date"].dt.strftime('%m-%Y')
        spac_data["Exp Date Year"] = spac_data["Exp Date"].dt.strftime('%Y')
        spac_data["Exp Date Month"] = spac_data["Exp Date"].dt.strftime('%m')
        spac_data["Price Per 100K"] = (NDiffs.initial_investment / spac_data["Previous Closing Price"]) \
                                      * (spac_data["Cash per Share in Trust"] - spac_data["Previous Closing Price"])
        return spac_data

    def rank_by_diff_and_month(self, dataset, n=1, month_year_cutoff="1-1970"):
        sorted_data = dataset.sort_values(by=["Exp Date Year", "Exp Date Month", "Price Per 100K"],
                                          ascending=[True, True, False])
        sorted_data = sorted_data.groupby('Exp Date Month Year').head(n)
        sorted_data = sorted_data[["Issuer Name", "Common Ticket", "Remaining Life (months)", "Previous Closing Price",
                                   "Cash per Share in Trust", "Exp Date", "Ann. YTM - Last Reported", "IPO Size ($m)",
                                   "Price Per 100K", "Exp Date Month Year"]]

        month_year_cutoff_obj = datetime.datetime.strptime(month_year_cutoff, "%m-%Y")
        sorted_data["Exp Date Month Year"] = pd.to_datetime(sorted_data["Exp Date Month Year"], format="%m-%Y")
        sorted_data = sorted_data[sorted_data["Exp Date Month Year"] > month_year_cutoff_obj]
        sorted_data = sorted_data.set_index(sorted_data["Exp Date Month Year"])
        sorted_data.index.name = "Index Date"
        self.find_weighted_ipo(sorted_data)
        self.find_shares_and_price(sorted_data)
        return sorted_data

    @staticmethod
    def find_weighted_ipo(dataset):
        dataset["Weighted IPO"] = dataset["IPO Size ($m)"] \
                                  / dataset.groupby("Exp Date Month Year")["IPO Size ($m)"].sum()

    @staticmethod
    def find_shares_and_price(dataset):
        dataset["Number of Shares Weighted"] = (NDiffs.initial_investment * dataset["Weighted IPO"]) \
                                                / dataset["Previous Closing Price"]
        dataset["Price Per Initial Weighted"] = dataset["Price Per 100K"] * dataset["Weighted IPO"]

    def only_keep_top_n_spacs(self, dataset, n):
        rows_to_keep = [0] * n
        greatest_prices = [-99] * n
        greatest_price_rows = []
        for count, row in enumerate(dataset.iterrows(), start=1):
            current_price = float(row[1]["Price Per Initial Weighted"])
            if current_price >= greatest_prices[0]:
                greatest_prices[0] = float(current_price)
                rows_to_keep[0] = row[1].values
                greatest_prices, rows_to_keep = zip(*sorted(zip(greatest_prices, rows_to_keep)))
                greatest_prices = list(greatest_prices)
                rows_to_keep = list(rows_to_keep)

            if count % n == 0:
                greatest_price_rows.append(copy.deepcopy(rows_to_keep))

        # TODO note below
        '''
        using the dataset.index will only work if there's exactly n spacs per group, otherwise this will fail
        with not enough rows found, should fix this
        '''
        df_highest_prices_repeated = pd.DataFrame(np.row_stack(greatest_price_rows))
        df_highest_prices_repeated.columns = ["Issuer Name", "Common Ticket", "Remaining Life (months)",
                                              "Previous Closing Price", "Cash per Share in Trust", "Exp Date",
                                              "Ann. YTM - Last Reported", "IPO Size ($m)", "Price Per 100K",
                                              "Exp Date Month Year", "Weighted IPO", "Number of Shares Weighted",
                                              "Price Per Initial Investment Weighted"]

        df_reset_index = self.validate_and_reset_index(df_highest_prices_repeated)
        return df_reset_index

    def validate_and_reset_index(self, dataset):
        is_valid = self.validate_length_of_index(dataset, dataset)
        if is_valid:
            dataset.index = dataset["Exp Date Month Year"]
        else:
            self.validate_correct_number_of_fields_per_group()
            dataset.index = dataset["Exp Date Month Year"].iloc[0:len(dataset)]
        dataset.index.name = "Date Index"
        return dataset

    @staticmethod
    def validate_correct_number_of_fields_per_group(dataset):
        print(dataset.groupby(["Exp Date Month Year"]).size())

    @staticmethod
    def validate_length_of_index(dataset, new_dataset):
        len_of_index_dataset = len(dataset.index)
        len_of_index_new_dataset = len(new_dataset.index)
        return len_of_index_dataset == len_of_index_new_dataset

    @staticmethod
    def generate_monthly_date_range(dataset_date_column, end_date=""):
        if end_date != "":
            date_range = pd.date_range(start=dataset_date_column.iloc[0],
                                       end=end_date, freq="MS")
        else:
            date_range = pd.date_range(start=dataset_date_column.iloc[0],
                                       end=dataset_date_column.iloc[-1], freq="MS")
        return date_range

    @staticmethod
    def forward_fill_missing_data(dataset):
        dataset.ffill(inplace=True)
        return dataset

    @staticmethod
    def write_data(dataset, file_output_path):
        dataset.to_csv(file_output_path)


num_spacs = 2
ndiffs = NDiffs()
returned_spac_data = ndiffs.read_and_process_in_data("".join([ndiffs.file_path,
                                                     "2022_03_16 - SPAC reported yields.xlsx"]))
ndiffs.write_data(returned_spac_data, "".join([ndiffs.file_path, "full_out.csv"]))

ranked_data = ndiffs.rank_by_diff_and_month(returned_spac_data, num_spacs, month_year_cutoff="05-2022")
highest_prices = ndiffs.only_keep_top_n_spacs(ranked_data, num_spacs)
ndiffs.validate_correct_number_of_fields_per_group(ranked_data)

ndiffs.write_data(ranked_data, "".join([ndiffs.file_path, "full_out.csv"]))
ndiffs.write_data(highest_prices, "".join([ndiffs.file_path, "out_highest_prices.csv"]))
