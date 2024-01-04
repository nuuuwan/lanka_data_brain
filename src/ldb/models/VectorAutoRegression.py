import datetime
from functools import cached_property

import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.tsa.api import VAR
from utils import SECONDS_IN, Log

from ldb.core import DataSource

log = Log('VectorAutoRegression')


class VectorAutoRegression:
    def __init__(self, *ds_list: list[DataSource]):
        self.ds_list = ds_list

    @cached_property
    def first_ds(self):
        return self.ds_list[0]

    @cached_property
    def df_key_col(self):
        return self.first_ds.df_key_col

    @cached_property
    def index_list(self):
        return list(self.df_merged.index)

    @cached_property
    def last_ut(self) -> int:
        return int(self.index_list[-1].timestamp())

    @cached_property
    def avg_gap(self) -> int:
        key_list = self.index_list
        n = len(key_list)
        gap_list = []
        for i in range(n - 1):
            k1 = key_list[i].timestamp()
            k2 = key_list[i + 1].timestamp()
            dk = k2 - k1
            gap_list.append(dk)
        avg_gap = sum(gap_list) / len(gap_list)
        avg_gap = round(avg_gap / SECONDS_IN.DAY, 0) * SECONDS_IN.DAY
        return avg_gap

    @cached_property
    def df_merged(self) -> pd.DataFrame:
        df_merged = self.first_ds.df
        df_key_col = self.df_key_col
        columns = [df_key_col, self.first_ds.df_val_col]
        for ds in self.ds_list[1:]:
            assert ds.df_key_col == df_key_col
            df_merged = df_merged.merge(ds.df, on=df_key_col, how='outer')
            columns.append(ds.df_val_col)
        df_merged.columns = columns
        df_merged = df_merged.interpolate(method='ffill')
        df_merged = df_merged.interpolate(method='bfill')

        df_merged = df_merged.set_index(self.df_key_col)
        df_merged = df_merged.sort_index()
        return df_merged

    def get_forecast_df(self, maxlags: int, steps: int):
        df = self.df_merged.copy()

        model = VAR(df)
        results = model.fit(
            maxlags=maxlags, ic='fpe', trend='n', verbose=True
        )

        forecast = results.forecast(df.values[-results.k_ar:], steps=steps)

        df = self.df_merged.copy()
        for i, row in enumerate(list(forecast)):
            ut = self.last_ut + (i + 1) * self.avg_gap
            t = datetime.datetime.fromtimestamp(ut)

            new_row = {}
            for i_ds, ds in enumerate(self.ds_list):
                new_row[ds.df_val_col] = row[i_ds]
            df.loc[t] = new_row

        df = df.sort_index()

        return df

    def plot(self, maxlags: int, steps: int, display: int):
        first_ds = self.first_ds
        df = self.get_forecast_df(maxlags=maxlags, steps=steps)
        log.debug(str(df))
        df = df[-display:]
        color = [
            'blue' if i < display - steps else 'lightblue'
            for i in range(display)
        ]

        plt.bar(
            df.index,
            df[first_ds.df_val_col],
            width=24,
            color=color,
        )
        plt.title(first_ds.short_name)
        plt.xlabel("Time")
        plt.grid()
        plt.gcf().set_size_inches(8, 4.5)
        plt.show()
