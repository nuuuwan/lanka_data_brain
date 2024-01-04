from ldb import DataSource, VectorAutoRegression
from utils import Log 

log = Log('pipeline')

def main():
   for ds in DataSource.list_large(n=200):
        try:
            VectorAutoRegression.forecast(
                ds, maxlags=12, steps=12, min_abs_corr=0.9
            )
        except Exception as e:
            log.error(f'Failed to forecast {ds}: {e}')


if __name__ == '__main__':
    main()
