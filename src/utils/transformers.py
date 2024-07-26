import pandas as pd
import yaml
import os
from datetime import datetime

class CriaFeatures:
    def __init__(self, configfile : str):
        self.configfile = configfile

    def fit(self, X : pd.DataFrame = None, y : pd.Series = None):
        self.variaveis_utilizadas = yaml.safe_load(open(os.path.join('src', 'data', 'config', self.configfile), 'r'))['variaveis_utilizadas']
        self.map_horario = yaml.safe_load(open(os.path.join('src', 'data', 'config', 'map_horario.yaml'), 'r'))
        return self
    
    def transform(self, X : pd.DataFrame):
        X = X[self.variaveis_utilizadas].reset_index(drop=True)
        X_tmp = X.reset_index(drop=True)

        X_tmp['valor_parcela'] = X_tmp['valor_transacao'] / X_tmp['quantidade_parcelas']

        X_tmp['periodo_do_dia'] = X_tmp['data_e_hora'].str[0:2].map(self.map_horario)

        colunas_criadas = [coluna for coluna in X_tmp.columns if coluna not in X.columns]
        X_tmp = X.merge(X_tmp[colunas_criadas], right_index=True, left_index=True)
        return X_tmp


