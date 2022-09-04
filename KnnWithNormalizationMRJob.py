from mrjob.job import MRJob
import csv
import numpy as np

class KnnWithNormalizationMRJob(MRJob):
    DELIMITER = ","
    minmaxdata = []

    
    def configure_args(self):
        super(KnnWithNormalize, self).configure_args()
        self.add_passthru_arg("--Max_K", type=int, help="Minimum value of K")
        self.add_file_arg("--test", help='Path of test dataset')
        self.add_file_arg("--minmax", type=str)

    
    def mapper_init(self):
        with open(self.options.minmax,"r") as f:
            for line in f:
                fieldvalues = line.split(self.DELIMITER)
                colnumber = int(fieldvalues[0])
                minimum = float(fieldvalues[1])
                maximum = float(fieldvalues[2])
                self.minmaxdata.append([colnumber,minimum,maximum])


    def mapper(self, _, row):
        minmaxdata = self.minmaxdata
        train_values = row.strip().split(self.DELIMITER)
        #Normalize Train data
        col = 0
        train_out_values = []
        for value in train_values:
            norm_values = [x for x in minmaxdata if x[0] == col]
            col += 1
            if norm_values:
                minimum = norm_values[0][1]
                maximum = norm_values[0][2]
                train_out_values.append((float(value) - minimum) / (minimum - maximum))
            else:
                train_out_values.append(value)
        
        train_nparray = np.array(train_out_values[1:-1], dtype=float)

        with open(self.options.test, 'r') as test_file:
            test_csv_reader = csv.reader(test_file)
            for test_values in test_csv_reader:
                #Normalize Test Data
                col = 0
                test_out_values = []
                for value in test_values:
                    norm_values = [x for x in minmaxdata if x[0] == col]
                    col += 1
                    if norm_values:
                        minimum = norm_values[0][1]
                        maximum = norm_values[0][2]
                        test_out_values.append((float(value) - minimum) / (minimum - maximum))
                    else:
                        test_out_values.append(value)
                line_values = np.array(test_out_values[1:-1], dtype=float)
                sum_vectors = np.sum(np.square(line_values - train_nparray))
                # distance = (sum([(x1-x2)**2 for x1,x2 in zip(line_values,row_array)]))**0.5
                dist = np.sqrt(sum_vectors)
                yield test_values[0], (dist, train_values[-1], test_values[-1])
    

    def combiner(self, key, values):
        pq = []
        for value in values:
            if len(pq) < self.options.Max_K:
                pq.append(value)
            else:
                max_value = max(pq, key=lambda x: x[0])
                if max_value[0] < value[0]:
                    pq.remove(max_value)
                    pq.append(value)
        for value in pq:
            yield key, value

    
    def reducer(self, key, values):
        pq = []
        for value in values:
            if len(pq) < self.options.Max_K:
                pq.append(value)
            else:
                max_value = max(pq, key=lambda x: x[0])
                if max_value[0] > value[0]:
                    pq.remove(max_value)
                    pq.append(value)
        for value in pq:
            yield key, value


if __name__ == '__main__':
    KnnWithNormalize.run()