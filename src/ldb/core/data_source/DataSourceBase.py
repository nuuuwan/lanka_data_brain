
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property

import pandas as pd
from utils import Log

log = Log('DataSourceBase')


def parse_date(x):
    if len(x) == 4:
        return datetime.strptime(
            x,
            '%Y')
    if len(x) == 7:
        return datetime.strptime(
            x,
            '%Y-%m')
    if len(x) == 10:
        return datetime.strptime(
            x,
            '%Y-%m-%d')
    raise ValueError(f'Invalid date format: {x}')


@dataclass
class DataSourceBase:
    source_id: str
    # category: str
    sub_category: str
    # scale: str
    # unit: str
    frequency_name: str
    # i_subject: int
    # footnotes: dict
    # summary_statistics: dict
    cleaned_data: dict
    # raw_data: dict

    @staticmethod
    def from_dict(data: dict):
        return DataSourceBase(
            source_id=data['source_id'],
            # category=data['category'],
            sub_category=data['sub_category'],
            # scale=data['scale'],
            # unit=data['unit'],
            frequency_name=data['frequency_name'],
            # i_subject=(int)(data['i_subject']),
            # footnotes=data['footnotes'],
            # summary_statistics=data['summary_statistics'],
            cleaned_data=data['cleaned_data'],
            # raw_data=data['raw_data'],
        )

    @property
    def short_name(self) -> str:
        return ' '.join(
            [
                self.sub_category,
                '-',
                self.frequency_name,
            ]
        )

    @cached_property
    def df_val_col(self) -> str:
        return self.short_name.replace(' ', '-').lower().replace('---', '-')

    @cached_property
    def df_key_col(self) -> str:
        return 't'

    @cached_property
    def __len__(self) -> int:
        return len(self.cleaned_data)

    def __str__(self) -> str:
        return f'DataSource("{self.short_name}", n={len(self.cleaned_data):,})'

    @property
    def df(self) -> pd.DataFrame:
        d_list = [
            {
                self.df_key_col: parse_date(k),
                self.df_val_col: float(v),
            }
            for k, v in self.cleaned_data.items()
        ]
        df = pd.DataFrame(
            d_list,
            columns=[
                self.df_key_col,
                self.df_val_col])
        return df
