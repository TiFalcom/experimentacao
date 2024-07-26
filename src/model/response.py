import sys
import pickle
import os
import pandas as pd
from datetime import datetime
import click
import multiprocessing

sys.path.append('src/utils/latency/')

@click.command()
@click.option('--qty_workers', default=1, type=int, help='Numero de processos simultaneos.')
@click.option('--qty_requests', default=500, type=int, help='Quantidade de requests por processo.')
@click.option('--transformers_path', default='src/utils/transformers.py', type=str, help='Nome do arquivo transformers que deve ser considerado.')
@click.option('--class_name', default='CriaFeatures', type=str, help='Nome da classe que realiza a transformação de variáveis.')
@click.option('--method_name', default='transform', type=str, help='Nome do método que realiza a transformação de variáveis.')
def main(qty_workers, qty_requests, transformers_path, class_name, method_name):
    """Avalia tempo de resposta por etapa do criador de features
    
    """
    qty_timestamps = write_transformers_latency(transformers_path, class_name, method_name)

    time_names = str(['time_' + str(i) for i in range(1, qty_timestamps+1)]).replace('[', '').replace(']', '').replace(' ', '').replace("'", '').replace(',', ';')
    print('worker', 'time_init', time_names, 'time_end', sep=';')

    if qty_workers > multiprocessing.cpu_count():
        raise

    payload = pd.read_csv(os.path.join('data', 'payload.csv'))

    procs = []
    for worker_id in range(qty_workers):
        proc = multiprocessing.Process(target=worker, args=(worker_id, qty_requests, payload,))
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()


def worker(worker_id, qty_requests, payload):
    """ Função que é executada em multiplos processos
    """
    cria_features = pickle.load(open(os.path.join('models', 'encoders', 'cria_features.pkl'), 'rb'))

    for _ in range(qty_requests):
        timestamps = []

        timestamps.append(datetime.now())
        _, timestamps_cria_features = cria_features.transform_latency(payload)
        timestamps_cria_features.append(datetime.now())
        timestamps += timestamps_cria_features

        timestamps_str = str([ts.strftime('%Y-%m-%d %H:%M:%S.%f') for ts in timestamps]).replace('[', '').replace(']', '').replace(' ', '').replace("'", '').replace(',', ';')

        print(worker_id, timestamps_str, sep=';')

def write_transformers_latency(transformers_path, class_name, method_name):
    import importlib.util
    import inspect

    module_name = os.path.basename(transformers_path).split('.')[0]
    spec = importlib.util.spec_from_file_location(module_name, transformers_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    method_content = inspect.getsource(getattr(module, class_name).__dict__[method_name])
    
    lines = method_content.split('\n')
    new_method_content = []
    qty_timestamps = 0

    for line in lines:
        if line.strip():
            if line.strip().startswith(('return')):
                new_method_content.append(line.replace('\n', '').rstrip() + ', timestamps\n')
            else:
                new_method_content.append(line)
            if line.strip().startswith(('def ')):
                new_method_content.append('        timestamps = []')
            if not line.strip().startswith(('class ', 'return', 'import', 'from')):
                new_method_content.append('        ' + 'timestamps.append(datetime.now())\n')
                qty_timestamps += 1

    new_method_content = '\n'.join(new_method_content).replace(method_name, method_name + '_latency')

    with open(transformers_path, 'r') as file:
        new_file_content = file.read().replace(method_content, new_method_content)

    with open(os.path.join('src', 'utils', 'latency', 'utils', 'transformers.py'), 'w') as file:
        file.write(new_file_content)

    return qty_timestamps

def include_latency():
    pass

if __name__ == '__main__':

    main()