from n_diffs import NDiffs
from outsideLiquidationDate import CDMData
import pandas as pd

columns_to_keep = ["Issuer Name", "Common Ticker", "Previous Closing Price",
                   "Cash per Share in Trust", "Predicted Cash in Trust", "Redeem Date",
                   "Profit Per 100K", "PnL", "Shares to Cover Evenly Split", "Avg Vol"]

ndiffs = NDiffs()
cdm_data_obj = CDMData()
look_back = 5
num_spacs = 5
volume_filter = 0

# CDM VERSION
amount_to_cover = cdm_data_obj.get_draw_schedule(board="".join([ndiffs.file_path, "results/",
                                              "A Note Securitization SPACS - Hanover copy.xlsx"]))
returned_spac_data = cdm_data_obj.get_spacs_from_dashboard(board="".join([ndiffs.file_path, "in_data/",
                                                                          "SPAC Dashboard.xlsx"]),
                                                           start_date="4/1/2022",
                                                           look_back=look_back)
returned_spac_data = ndiffs.read_and_process_in_data_cdm(returned_spac_data)
ndiffs.write_data(returned_spac_data, "".join([ndiffs.file_path, "out_data/",
                                               "returned_spac_data.csv"]))

returned_spac_data = returned_spac_data.loc[
    returned_spac_data["Avg Vol"] > volume_filter].reset_index(drop=True)
full_out = ndiffs.rank_by_diff_and_month(returned_spac_data, num_spacs)
highest_prices = ndiffs.keep_top_n_spacs_via_lookback(full_out,
                                                      start_date="6/1/2022",
                                                      n=num_spacs)

highest_prices = ndiffs.calc_number_of_shares_to_buy(highest_prices,
                                                     amount_to_cover,
                                                     num_spacs)
date_check = ndiffs.validate_no_out_of_range_dates(
    highest_prices[columns_to_keep])
final_date = list(amount_to_cover)[-1]
if date_check:
    print("ERROR IN DATES, PLEASE VALIDATE THE FIELDS")
else:
    extended_data = ndiffs.extend_dataset(highest_prices[columns_to_keep],
                                          pd.to_datetime("5/1/2025"),
                                          num_spacs)
    ndiffs.make_ending_dates_all_zero(extended_data,
                                      pd.to_datetime(final_date))
    ndiffs.write_data(full_out, "".join([ndiffs.file_path, "out_data/",
                                         "full_out.csv"]))
    ndiffs.write_data(extended_data, "".join([ndiffs.file_path, "out_data/",
                                              "ranked_data.csv"]))
