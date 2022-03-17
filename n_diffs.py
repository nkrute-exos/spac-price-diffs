"""Code to take an excel sheet of spac cash per share in trust compared to the previous closing price
   and group by the month and year. Then only show the highest n number of price diffs"""
import numpy as np
import pandas as pd
import datetime
pd.options.mode.chained_assignment = None


class NDiffs():
    headers = ["Issuer Name", "Common Ticket", "Remaining Life (months)", "Previous Closing Price",
               "Cash per Share in Trust", "Exp Date", "Ann. YTM - Last Reported", "IPO Size ($m)"]

    def read_in_data(self, file_name):
        spac_data = pd.read_excel(file_name)
        spac_data = spac_data.iloc[6:, 1:]
        spac_data.columns = NDiffs.headers
        spac_data = spac_data.reset_index(drop=True)
        spac_data["Exp Date"] = pd.to_datetime(spac_data["Exp Date"])
        spac_data["Price Diff"] = spac_data["Cash per Share in Trust"] - spac_data["Previous Closing Price"]
        spac_data["Exp Date Month Year"] = spac_data["Exp Date"].dt.strftime('%m-%Y')
        spac_data["Exp Date Year"] = spac_data["Exp Date"].dt.strftime('%Y')
        spac_data["Exp Date Month"] = spac_data["Exp Date"].dt.strftime('%m')
        return spac_data

    def rank_by_diff_and_month(self, dataset, n, month_year_cutoff="1-1970"):
        sorted_data = dataset.sort_values(by=["Exp Date Year", "Exp Date Month", "Price Diff"],
                                          ascending=[True, True, False])
        sorted_data = sorted_data.groupby('Exp Date Month Year').head(n)
        sorted_data = sorted_data[["Issuer Name", "Common Ticket", "Remaining Life (months)", "Previous Closing Price",
                                   "Cash per Share in Trust", "Exp Date", "Ann. YTM - Last Reported", "IPO Size ($m)",
                                   "Price Diff", "Exp Date Month Year"]]

        month_year_cutoff_obj = datetime.datetime.strptime(month_year_cutoff, "%m-%Y")
        sorted_data["Exp Date Month Year"] = pd.to_datetime(sorted_data["Exp Date Month Year"], format="%m-%Y")
        sorted_data = sorted_data[sorted_data["Exp Date Month Year"] > month_year_cutoff_obj]
        sorted_data = sorted_data.set_index(sorted_data["Exp Date Month Year"])
        return sorted_data


    def add_column_with_best_value(self, dataset):
        greatest_diff = -99
        last_highest_row = []
        greatest_diff_rows = []
        for row in dataset.iterrows():
            latest_price_diff = row[1]["Price Diff"]
            if latest_price_diff > greatest_diff:
                greatest_diff = latest_price_diff
                last_highest_row = row[1].values
            greatest_diff_rows.append(last_highest_row)
        df_highest_diff_repeated = pd.DataFrame(np.row_stack(greatest_diff_rows), index=dataset.index)
        idx = self.generate_monthly_date_range(dataset["Exp Date Month Year"])
        df_highest_diff_repeated = df_highest_diff_repeated.reindex(idx)
        self.forward_fill_missing_data(df_highest_diff_repeated)
        return df_highest_diff_repeated

    def generate_monthly_date_range(self, dataset_date_column):
        date_range = pd.date_range(start=dataset_date_column.iloc[0],
                                   end=dataset_date_column.iloc[-1], freq="MS")
        return date_range

    def forward_fill_missing_data(self, dataset):
        dataset.ffill(inplace=True)
        return dataset

    def write_data(self, dataset, file_output_path):
        dataset.to_csv(file_output_path)


ndiffs = NDiffs()
returned_spac_data = ndiffs.read_in_data("/Users/nicholaskrute/Documents/SPAC_Price_by_diffs/2022_03_16 - SPAC reported yields.xlsx")
ranked_data = ndiffs.rank_by_diff_and_month(returned_spac_data, 1, month_year_cutoff="05-2022")
highest_diffs = ndiffs.add_column_with_best_value(ranked_data)
ndiffs.write_data(highest_diffs, "/Users/nicholaskrute/Documents/SPAC_Price_by_diffs/out_highest_diff.csv")
