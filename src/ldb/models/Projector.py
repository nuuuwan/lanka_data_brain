from datetime import datetime
from functools import cached_property
from utils import TIME_FORMAT_DATE, Log, Time
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import numpy as np
from ldb.core import DataSource

log = Log('Projector')

class Projector:
    def __init__(
        self,
        data_source: DataSource,
        m_training_window: int,
        n_min_train: int,
    ):
        self.data_source = data_source
        self.m_training_window = m_training_window
        self.n_min_train = n_min_train

    def __str__(self) -> str:
        d = dict(
            data_source=self.data_source.short_name,
            m_training_window=self.m_training_window,
            n_min_train=self.n_min_train,
            n=self.n,
            is_projectable=self.is_projectable,
        )
        return f'Projector({str(d)})'

    @cached_property
    def cleaned_data(self) -> dict:
        return self.data_source.cleaned_data

    @cached_property
    def data_points(self) -> list[tuple]:
        return list(self.cleaned_data.items())

    @cached_property
    def n(self) -> int:
        return len(self.cleaned_data.values())

    @cached_property
    def is_projectable(self):
        return self.n > self.n_min_train + self.m_training_window
    

    @cached_property
    def train_data(self):
        _, v = zip(*self.data_points)
        X = []
        Y = []
        for i in range(self.n - self.m_training_window):
            X.append(list(v[i: i + self.m_training_window]))
            Y.append([v[i + self.m_training_window]])
        return X, Y
    
    def train(self):
        X, Y = self.train_data
        model = LinearRegression()
        model.fit(X, Y)
        m, c = model.coef_[0], model.intercept_
        log.debug(f'train: {m=}, {c=}')
        return m, c
    
    def project(self, n_future: int):
        data_points = self.data_points
        d, v = zip(*data_points)

        uts = []
        for di in d:
            ut = TIME_FORMAT_DATE.parse(di).ut
            uts.append(ut)
        gaps = []
        for i in range(len(uts) - 1):
            gaps.append(uts[i + 1] - uts[i])
        avg_gap = sum(gaps) / len(gaps)
        log.debug(f'{avg_gap=}')

        X, Y = self.train_data
        m, c = self.train()
        
        last_x_ut = TIME_FORMAT_DATE.parse(data_points[-1][0]).ut
        log.debug(f'{last_x_ut=}')

        for i in range(n_future):
            x = X[-1]
            y = list(np.dot(m, x) + c)

            X.append(x[1:] + [y[0]])
            Y.append([y])
            d = TIME_FORMAT_DATE.stringify(Time(last_x_ut + (i + 1) * avg_gap))
            data_points.append((d, y[0]))
        return data_points
    
    def plot(self, n_future: int):
        MAX_PLOT_VALUES = n_future + 12
        projected_data_points = self.project(n_future)[-MAX_PLOT_VALUES:]
        color = [('blue' if i < MAX_PLOT_VALUES - n_future else 'lightblue' ) for i in range(MAX_PLOT_VALUES)]
        print(projected_data_points)
        x, y = zip(*projected_data_points)
        x = [datetime.strptime(x, '%Y-%m-%d') for x in x]
        plt.bar(x, y,color=color,width=24)
        plt.show()
