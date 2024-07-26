import sys
import pickle
import os

sys.path.append('src/')

from utils.latency.utils.transformers_latency import CriaFeatures


def main():

    cria_features = CriaFeatures('data_config.yaml')

    cria_features.fit()

    pickle.dump(cria_features, open(os.path.join('models', 'encoders', 'cria_features.pkl'), 'wb'))


if __name__ == '__main__':

    main()

