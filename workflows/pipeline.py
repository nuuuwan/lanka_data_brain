from ldb import DataSource, VectorAutoRegression


def main():
    ds_list = DataSource.list_all()
    ds_tourist = DataSource.list_from_search(
        'Total Tourist Arrivals - Monthly',
    )[0]
    ds_remit = DataSource.list_from_search(
        'Secondary income - Workers\' Remittances',
    )[0]
    ds_gdp = DataSource.list_from_search(
        'GDP per capita (current US$)',
    )[0]

    var = VectorAutoRegression(
        ds_tourist, ds_remit, ds_gdp, ds_list[0], ds_list[1000]
    )
    var.plot(maxlags=36, steps=12, display=24)


if __name__ == '__main__':
    main()
