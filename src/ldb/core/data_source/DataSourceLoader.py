import os
import tempfile
from functools import cache

from utils import Git, JSONFile, Log

from ldb.core.data_source.DataSourceBase import DataSourceBase

log = Log('DataSource')


class DataSourceLoader(DataSourceBase):
    DIR_DATA = os.path.join(
        tempfile.gettempdir(),
        'lanka_data_timeseries.data')
    GITHUB_USERNAME = 'nuuuwan'
    GITHUB_REPO_DATA = 'lanka_data_timeseries'
    GITHUB_REPO_DATA_URL = (
        'https://github.com/' + f'{GITHUB_USERNAME}/{GITHUB_REPO_DATA}.git'
    )

    @staticmethod
    def download_data():
        if os.path.exists(DataSourceLoader.DIR_DATA):
            log.info(f'Data already exists at {DataSourceLoader.DIR_DATA}')
            return
        log.info(
            'Downloading data'
            + f' from {DataSourceLoader.GITHUB_REPO_DATA_URL}'
            + f' to {DataSourceLoader.DIR_DATA}'
        )
        git = Git(DataSourceLoader.GITHUB_REPO_DATA_URL)
        git.clone(DataSourceLoader.DIR_DATA)
        git.checkout('data')

    @classmethod
    def from_file(cls, file_path: str):
        return cls.from_dict(JSONFile(file_path).read())

    @staticmethod
    @cache
    def list_all() -> list:
        DataSourceLoader.download_data()
        dir_sources = os.path.join(
            DataSourceLoader.DIR_DATA, 'sources')
        ds_list = []
        for source_id in os.listdir(dir_sources):
            dir_source = os.path.join(dir_sources, source_id)
            for file_name in os.listdir(dir_source):
                if file_name.startswith(
                        source_id) and file_name.endswith('.json'):
                    file_path = os.path.join(dir_source, file_name)
                    ds = DataSourceLoader.from_file(
                        file_path
                    )
                    ds_list.append(ds)

        log.info(f'Found {len(ds_list):,} data sources.')
        return ds_list

    @staticmethod
    @cache
    def list_large(n: int) -> list:
        ds_list = DataSourceLoader.list_all()
        ds_list_large = [ds for ds in ds_list if len(ds) > n]
        log.info(f'Found {len(ds_list_large):,} data sources, where n >= {n}.')
        return ds_list_large

    @staticmethod
    @cache
    def list_from_search(search_key: str) -> list:
        ds_list = DataSourceLoader.list_all()
        return [
            ds for ds in ds_list
            if search_key.lower() in ds.short_name.lower()
        ]

    def list_correlated(self, n: int, min_abs_corr: float,
                        ignore_exact: bool) -> list:
        ds_list = DataSourceLoader.list_large(n)
        info_list = []
        for ds in ds_list:
            if ds.short_name == self.short_name:
                continue
            corr = self.get_correlation_coefficient(ds)
            if corr > 0.9999:
                if ignore_exact:
                    continue

            if abs(corr) >= min_abs_corr:
                log.debug(f'{corr:.2f}:' + '\t' + str(ds))
                info_list.append(dict(ds=ds, corr=corr))
        info_list.sort(key=lambda x: abs(x['corr']), reverse=True)
        log.info(
            f'Found {len(info_list):,} data sources,'
            + f' where |corr| >= {min_abs_corr}.')
        return info_list
