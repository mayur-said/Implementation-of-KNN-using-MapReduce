# MinMaxMRJob.py

import re
from mrjob.job import MRJob

class MinMaxJob(MRJob):

    DELIMITER = ","
    REGEX = re.compile("^[+-]?\d+(\.\d+)?$")

    mins = []
    maxs = []


    def mapper(self, _, line):

        field_values = line.split(self.DELIMITER)[1:-1]
        col = 1
        for field_value in field_values:
            # only process a column if the contained value is numeric
            if bool(self.REGEX.match(field_value.strip())):
                field_value = float(field_value)
                yield(col,field_value)
            col += 1

    def combiner(self, col, values):
        values = list(values)
        minval = min(values)
        maxval = max(values)
        yield(col,[minval,maxval])

    def reducer(self, col, values):
        values = list(values)
        mins = [i[0] for i in values]
        maxs = [i[1] for i in values]
        yield(col,[min(mins),max(maxs)])

if __name__ == '__main__':
    MinMaxJob.run()