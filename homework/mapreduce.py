'''Map/Reduce implementation'''

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path

def _load_input(input_directory):
    files = glob.glob(f'{input_directory}/*')
    with fileinput.input(files = files) as f:
        sequence = [ (fileinput.filename(), line) for line in f ]
    return sequence


def _shuffle_and_sort(sequence):
    sequence.sort(key = lambda x: x[0])
    return sequence

def _create_ouptput_directory(output_directory):
    if os.path.exists(output_directory):
        for file in glob.glob(f'{output_directory}/*'):
            os.remove(file)
        os.rmdir(output_directory)
    os.makedirs(output_directory)

def _save_output(output_directory, sequence):
    with open(f'{output_directory}/part-00000', 'w', encoding = 'utf-8') as f:
        f.writelines([ f'{key}\t{value}\n' for key, value in sequence ])

def _create_marker(output_directory):
    with open(f'{output_directory}/_SUCCESS', 'w', encoding = 'utf-8') as f:
        f.write('')

def run_mapreduce_job(mapper, reducer, input_directory, output_directory):
    seq = _load_input(input_directory)
    seq = mapper(seq)
    seq = _shuffle_and_sort(seq)
    seq = reducer(seq)

    _create_ouptput_directory(output_directory)
    _save_output(output_directory, seq)
    _create_marker(output_directory)