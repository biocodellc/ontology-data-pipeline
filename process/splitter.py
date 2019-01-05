# -*- coding: utf-8 -*-
import csv
import os


def split_file(input_data, output_dir, column, chunk_size):
    with open(input_data) as input_file:
        reader = csv.reader(input_file)

        header = next(reader)
        split_index = [c.lower() for c in header].index(column.lower())

        csv_writers = {}

        for row in reader:
            k = row[split_index]

            if k not in csv_writers:
                writer = csv.writer(open(os.path.join(output_dir, '{}_1.csv'.format(k)), mode='w', newline=''))
                writer.writerow(header)
                csv_writers[k] = {
                    'writer': writer,
                    'count': 0
                }
            elif csv_writers[k]['count'] % (chunk_size - 1) == 0:
                file_num = (csv_writers[k]['count'] / (chunk_size - 1)) + 1
                csv_writers[k]['writer'] = csv.writer(
                    open(os.path.join(output_dir, '{}_{}.csv'.format(k, file_num)), mode='w', newline='')
                )

            csv_writers[k]['count'] += 1
            csv_writers[k]['writer'].writerow(row)

