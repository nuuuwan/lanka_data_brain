from ldb import DataSource


def main():
    ds_list = DataSource.list_all()
    print(ds_list[0])

if __name__ == '__main__':
    main()
