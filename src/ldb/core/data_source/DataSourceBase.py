

from dataclasses import dataclass


@dataclass
class DataSourceBase:
    source_id: str
    category: str
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
            category=data['category'],
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
                self.category,
                self.sub_category,
                self.frequency_name,
            ]
        )

    def __str__(self) -> str:
        return f'DataSource({self.short_name})'
