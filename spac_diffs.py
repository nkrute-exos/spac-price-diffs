from n_diffs import NDiffs
from outsideLiquidationDate import CDMData
import pandas as pd
import datetime

amount_to_cover = {"9/1/22": 3448148.15, "10/1/22": 4187037.04, "11/1/22": 4187037.04, "12/1/22": 4187037.04,
                   "1/1/23": 4187037.04, "2/1/23": 4187037.04, "3/1/23": 4187037.04, "4/1/23": 4187037.04,
                   "5/1/23": 4187037.04, "6/1/23": 4187037.04, "7/1/23": 4187037.04, "8/1/23": 4187037.04,
                   "9/1/23": 4187037.04, "10/1/23": 4187037.04, "11/1/23": 4187037.04, "12/1/23": 4187037.04,
                   "1/1/24": 4187037.04, "2/1/24": 4187037.04, "3/1/24": 4187037.04, "4/1/24": 4187037.04,
                   "5/1/24": 4187037.04, "6/1/24": 4187037.04, "7/1/24": 4187037.04, "8/1/24": 4187037.04}
columns_to_keep = ["Issuer Name", "Common Ticker", "Previous Closing Price", "Cash per Share in Trust",
                   "Redeem Date", "Profit Per 100K", "Number of Shares", "PnL", "Shares to Cover Evenly Split"]


VERSION = "CDM"
num_spacs = 1
cdm_data_obj = CDMData()
ndiffs = NDiffs()

if VERSION == "CDM":
    print("Running CDM version")
    ### CDM VERSION
    returned_spac_data = cdm_data_obj.make_cdm_file_from_cantor_file(price_data="".join([ndiffs.file_path, "in_data/",
                                                                                         "SPAC Dashboard.xlsx"]),
                                                                     trade_date="5/1/2022")
    returned_spac_data = ndiffs.read_and_process_in_data_cdm(returned_spac_data)
    full_out = ndiffs.rank_by_diff_and_month(returned_spac_data, num_spacs, month_year_cutoff="05-2022")
    highest_prices = ndiffs.only_keep_top_n_spacs(full_out, num_spacs)
    highest_prices = ndiffs.calc_number_of_shares_to_buy(highest_prices, amount_to_cover, num_spacs)
    date_check = ndiffs.validate_no_out_of_range_dates(highest_prices[columns_to_keep])
    final_date = list(amount_to_cover)[-1]
    if date_check:
        print("ERROR IN DATES, PLEASE VALIDATE THE FIELDS")
    else:
        extended_data = ndiffs.extend_dataset(highest_prices[columns_to_keep], pd.to_datetime("5/1/2025"), num_spacs)
        ndiffs.make_ending_dates_all_zero(extended_data, pd.to_datetime(final_date))
        ndiffs.write_data(full_out, "".join([ndiffs.file_path, "out_data/", "full_out.csv"]))
        ndiffs.write_data(extended_data, "".join([ndiffs.file_path, "out_data/", "ranked_data.csv"]))

if VERSION == "CANTOR":
    print("Running CANTOR version")
    ### CANTOR VERSION
    returned_spac_data = ndiffs.read_and_process_in_data_cantor("".join([ndiffs.file_path,
                                                                  "in_data/", "2022_04_05_SPAC_reported_yields.xlsx"]))
    full_out = ndiffs.rank_by_diff_and_month(returned_spac_data, num_spacs, month_year_cutoff="05-2022")
    highest_prices = ndiffs.only_keep_top_n_spacs(full_out, num_spacs)
    highest_prices = ndiffs.calc_number_of_shares_to_buy(highest_prices, amount_to_cover, num_spacs)
    date_check = ndiffs.validate_no_out_of_range_dates(highest_prices[columns_to_keep])
    final_date = list(amount_to_cover)[-1]
    if date_check:
        print("ERROR IN DATES, PLEASE VALIDATE THE FIELDS")
    else:
        extended_data = ndiffs.extend_dataset(highest_prices[columns_to_keep], pd.to_datetime("5/1/2025"), num_spacs)
        cdm_dataset = cdm_data_obj.get_expected_prices_at_liquidation(extended_data)
        ndiffs.make_ending_dates_all_zero(extended_data, pd.to_datetime(final_date))
        ndiffs.write_data(full_out, "".join([ndiffs.file_path, "out_data/", "full_out.csv"]))
        ndiffs.write_data(extended_data, "".join([ndiffs.file_path, "out_data/", "ranked_data.csv"]))
