from ldb import DataSource, Projector


def main():
    ds_list = DataSource.list_all()
    ds = ds_list[2328]
    print(ds)
    prj = Projector(ds, m_training_window=36, n_min_train=120)
    print(prj)
    print(prj.plot(n_future=12))


if __name__ == '__main__':
    main()
