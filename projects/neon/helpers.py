import csv, argparse
import pandas as pd
from zipfile import ZipFile
from . import walk_files, INTENSITY_VALUE_FRAME, INTENSITY_FILE


def generate_phen_descriptions(input_dir):
    found_values = set()

    for file in walk_files(input_dir):
        csv_file = None

        with ZipFile(file) as zip_file:

            for filename in zip_file.namelist():
                if filename.endswith('phe_statusintensity.csv'):
                    if csv_file:
                        raise RuntimeError('multiple csv files found in zip_file {}'.format(zip_file.filename))
                    csv_file = zip_file.open(filename)

        if not csv_file:
            raise RuntimeError('didnt file csv file in zip_file {}'.format(zip_file))

        data = pd.read_csv(csv_file, header=0, chunksize=1000000, skipinitialspace=True,
                           usecols=['phenophaseName'])

        for chunk in data:
            found_values.update(chunk.phenophaseName.unique().tolist())

    with open('phenphase_names.csv', 'w') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(['phenophaseName'])

        for value in found_values:
            writer.writerow([value])


def generate_intensity_values(input_dir):
    if INTENSITY_VALUE_FRAME is None:
        intensity_frame = pd.DataFrame([], columns=['value', 'lower_count', 'upper_count', 'lower_percent',
                                                    'upper_percent'])
    else:
        intensity_frame = INTENSITY_VALUE_FRAME

    found_values = set()

    for file in walk_files(input_dir):
        csv_file = None

        with ZipFile(file) as zip_file:

            for filename in zip_file.namelist():
                if filename.endswith('phe_statusintensity.csv'):
                    if csv_file:
                        raise RuntimeError('multiple csv files found in zip_file {}'.format(zip_file.filename))
                    csv_file = zip_file.open(filename)

        if not csv_file:
            raise RuntimeError('didnt file csv file in zip_file {}'.format(zip_file))

        data = pd.read_csv(csv_file, header=0, chunksize=1000000, skipinitialspace=True,
                           usecols=['phenophaseIntensity'])

        for chunk in data:
            found_values.update(chunk.phenophaseIntensity.unique().tolist())

    found_values.difference_update(intensity_frame.index.tolist())

    intensity_frame = intensity_frame.append(
        pd.DataFrame.from_items([('value', list(found_values))])
    )

    intensity_frame.drop_duplicates('value', inplace=True)
    intensity_frame.set_index('value', inplace=True)

    intensity_frame.to_csv(INTENSITY_FILE)


INTENSITY = 0
PHENO_DESC = 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NEON Parser')
    parser.add_argument('input_dir', help='the input directory')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--intensity', dest='action', action='store_const',
                       const=INTENSITY,
                       help='generate a intensity_values.csv file with all phenophaseIntensity values found in the data')
    group.add_argument('--phenophase', dest='action', action='store_const',
                       const=PHENO_DESC,
                       help='generate a phenophase_names.csv file with all PhenophaseName values found in the data')

    args = parser.parse_args()
    input_dir = args.input_dir.strip()

    if args.action == INTENSITY:
        generate_intensity_values(input_dir)
    elif args.action == PHENO_DESC:
        generate_phen_descriptions(input_dir)
