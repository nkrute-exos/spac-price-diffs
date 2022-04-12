import pandas as pd
import numpy as np
import parameters.parameters as params
import headers


class ProjectLoan:
    columns = ["Draw", "Modeled Draw", "Bal", "Principal", "Prin WAL", "Interest", "CF"]

    @staticmethod
    def project_loan():
        headers_column = headers.SharedColumns.create_dataframe()
        initial_draw = params.initial_draw_percent * (params.project_cost-params.equity_in) + params.equity_in
        initial_draw_col = pd.DataFrame(0, index=range(0, len(headers_column)), columns=["Draw"])
        initial_draw_col.loc[0, "Draw"] = initial_draw
        print(initial_draw_col)


print(ProjectLoan.project_loan())
