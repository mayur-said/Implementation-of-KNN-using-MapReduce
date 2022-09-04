import sys
from KnnWithNormalizationMRJob import KnnWithNormalizationMRJob
import pandas as pd
from MinMaxMRJob import MinMaxJob

if __name__ == '__main__':
    print('Start')
    args = sys.argv[1:]
    print(args)

    
    minmaxargs = args[:2] + [args[-1]]
    # print(minmaxargs)
    minmaxjob = MinMaxJob(minmaxargs)

    with minmaxjob.make_runner() as runner:
        runner.run()
        with open("minmax.txt", "w") as f:
            for column, value in minmaxjob.parse_output(runner.cat_output()):
                f.write(",".join(str(val) for val in [column]+value)+"\n")


    # args = args + ['--minmax','minmax.txt']
    print(args)
    knnjob = KnnWithNormalizationMRJob(args)

    dict_ = {}

    with knnjob.make_runner() as runner:
        print('Runner Started')
        runner.run()
        print('Runner End')

        for key, value in knn_job.parse_output(runner.cat_output()):
            dict_.setdefault(key, []).append(value)
        
        print('For Loop End')
        # print(dict_)

        predicted_labels = {}
        actual_labels = []
        for key, value in dict_.items():
            lst_sorted = sorted(value, key=lambda x: x[0])
            # print(lst_sorted)
            lst_labels = [x[1].strip() for x in lst_sorted]
            actual_labels.append(value[0][2])
            for i in range(1, len(lst_labels)+1):
                max_ = max(set(lst_labels[:i]), key = lst_labels[:i].count)
                predicted_labels.setdefault(key, []).append(max_)
        # print(predicted_labels)
        # print(len(actual_labels))
        # print(dict_)

        predictions = pd.DataFrame(predicted_labels)
        print(predictions.shape)
        results = {}
        for i in range(0, predictions.shape[0]):
            acc = sum(predictions.iloc[i,:] == actual_labels)/len(actual_labels)
            results['K' + str(i + 1)] = acc
        print(results)
        
        