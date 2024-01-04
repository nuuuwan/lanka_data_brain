from ldb import DataSource, VectorAutoRegression


def main():
   for ds in DataSource.list_large(n=200):
        VectorAutoRegression.forecast(
            ds, maxlags=12, steps=12, min_abs_corr=0.9
        )


if __name__ == '__main__':
    main()
