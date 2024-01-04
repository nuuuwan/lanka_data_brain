import os
import tempfile

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

    @staticmethod
    def from_file(file_path: str):
        return DataSourceLoader.from_dict(JSONFile(file_path).read())

    @staticmethod
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

        log.info(f'Found {len(ds_list):,} data sources')
        return ds_list
