import datetime
import os
from functools import cached_property

import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.tsa.api import VAR
from utils import SECONDS_IN, Log, hashx
import json

from ldb.core import DataSource

log = Log('VectorAutoRegression')


class VectorAutoRegression:
    def __init__(self, *ds_list: list[DataSource]):
        self.ds_list = ds_list
        log.info(f'Created VectorAutoRegression with {ds_list} data sources.')

    @cached_property
    def first_ds(self):
        return self.ds_list[0]

    @cached_property
    def base_name(self) -> str:
        h = hashx.md5(json.dumps([str(ds) for ds in self.ds_list]))
        return self.first_ds.df_val_col + '-' + h[:8]

   

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
        df_diffed = df.diff().dropna()


        model = VAR(df_diffed)
        model_result = model.fit(maxlags=maxlags, ic='aic', trend='n')

        forecast_diffed = model_result.forecast(
            df_diffed.values[-model_result.k_ar:], steps=steps
        )
        m = len(self.ds_list)
        forecast = df.iloc[-1].values.reshape(1, m) + forecast_diffed.cumsum(axis=1)

        df_forecast = self.df_merged.copy()
        for i, row in enumerate(list(forecast)):
            ut = self.last_ut + (i + 1) * self.avg_gap
            t = datetime.datetime.fromtimestamp(ut)

            new_row = {}
            for i_ds, ds in enumerate(self.ds_list):
                new_row[ds.df_val_col] = row[i_ds]
            df_forecast.loc[t] = new_row

        df_forecast = df_forecast.sort_index()
        df_forecast.to_csv(os.path.join('data', 'forecasts', f'{self.base_name}.csv'))
        log.info(f'Saved forecast to {self.base_name}.csv.')
        return df_forecast

    def plot(self, df: pd.DataFrame, steps: int, ds: DataSource):
        display = steps * 10
        df = df[-display:]

        color = [
            'blue' if i < display - steps else 'lightblue'
            for i in range(display)
        ]

        plt.bar(
            df.index,
            df[ds.df_val_col],
            width=24,
            color=color,
        )
        plt.title(ds.short_name)
        plt.xlabel("Time")
        plt.grid()
        plt.gcf().set_size_inches(12, 4.5)
        
        image_path = os.path.join('data', 'charts', f'{self.base_name}.png')
        plt.savefig(image_path)
        plt.close()
        
        log.info(f'Saved chart to {image_path}.')
        
    @staticmethod
    def forecast(ds, maxlags: int, steps: int, min_abs_corr: float):
        n = 100
        if maxlags:
            n = max(n, maxlags * 2 + steps)
        corr_ds_list = ds.list_correlated(
            n=n, min_abs_corr=min_abs_corr, ignore_exact=True
        )
        if len(corr_ds_list) <= 1:
            log.warning(f'No correlated data sources found for {ds}.')
            return
        
        var = VectorAutoRegression(ds, *[info['ds'] for info in corr_ds_list])
        df_forecast = var.get_forecast_df(maxlags=maxlags, steps=steps)
        var.plot(df_forecast, steps, ds)
