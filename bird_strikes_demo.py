from pyspark import *
import csv
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.linalg import SparseVector

def main():
    sc = SparkContext("local")

    simple_operations(sc)
    bird_strikes_exploration(sc)


def simple_operations(sc):

    rdd = sc.parallelize([1,2,3])
    print rdd.count() #3

    doubled = rdd.map(lambda x: x*2)
    print doubled.collect() # [2, 4, 6]

    divided = doubled.flatMap(lambda x: [x/2.0, x/2.0])
    print divided.collect() # [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]

    filtered = divided.filter(lambda x: x > 1)
    print filtered.collect() # [2.0, 2.0, 3.0, 3.0]
    return

def bird_strikes_exploration(sc):
    text = sc.textFile('bird_strikes.csv')
    header = text.first()

    [columns] = csv.reader([header])

    ### let's make dictionary rows out of this csv text file for easy field access
    rows = text.map(lambda line : make_dict(line, columns)).cache()
    print rows.count(), 'rows' # 99973

    star_print("first row in file, it's the header!")
    print rows.first()
    ### we've got the header as the first row! let's take that out

    rows = text.filter(lambda line: line != header)\
                .map(lambda line : make_dict(line, columns)).cache()
    print rows.count(), 'rows' # 99972

    star_print("first row now, that's better")
    print rows.first()

    ### oh no! some of our rows have weird lengths!
    lengths = rows.map(lambda row: len(row))
    print star_print("distinct row lengths")
    print lengths.distinct().collect()
    star_print("row length distribution")
    print lengths.map(lambda l: (l,1)).reduceByKey(lambda x,y: x+y).collect()

    ### let's just get rid of these malformed rows
    star_print("removing malformed rows")
    rows = rows.filter(lambda row: len(row) == len(columns)).cache()
    print rows.count(), 'rows' # 99262

    ### what does one of these fields look like?
    aircraftTypes = rows.map(lambda row: row['Aircraft: Type'])
    star_print('some values from the aircraft type field')
    print aircraftTypes.take(20)
    star_print('all distinct values from aircraft type')
    print aircraftTypes.distinct().collect()
    star_print('distribution of values')
    print aircraftTypes.map(lambda t: (t,1)).reduceByKey(lambda x,y: x+y).collect()

    ### let's identify high and low cardinality fields!
    star_print('cardinalities')
    lowCardinality, highCardinality = [], []
    for field in columns:
        fieldRdd = rows.map(lambda row: row[field])
        if fieldRdd.distinct().count() <= 20:
            lowCardinality.append(field)
        else:
            highCardinality.append(field)
    print 'low', lowCardinality
    print 'high', highCardinality


    ### let's pick a field to try to predict!
    birdHits = rows.map(lambda row: row['Wildlife: Number struck'])
    star_print("distribution of 'Wildlife: Number struck' field")
    print birdHits.map(lambda h: (h,1)).reduceByKey(lambda x,y: x+y).collect()

    ### binary classification labels:
    strikeMap = {'': 0.0, '1': 0.0, '2 to 10': 0.0, '11 to 100': 100.0, 'Over 100': 100.0}

    ### let's use these fields to predict:
    pFields = [f for f in lowCardinality if f != 'Wildlife: Number struck']

    ### we need to featurize our values
    features = rows.flatMap(lambda row: [(f, row[f]) for f in pFields])
    distinctFeatures = features.distinct().collect()
    featureMap = dict([(feature,i) for i,feature in enumerate(distinctFeatures)])
    star_print('our feature map, mapping (field, field_value) to integers')
    print featureMap

    ### we must turn our features into a matrix representation using a sparse vector and a label
    def makeLabeledPoint(row):
        numberStruck = row['Wildlife: Number struck']
        label = strikeMap[numberStruck]
        predictors = [(featureMap[(f, row[f])],1.0) for f in pFields]
        return LabeledPoint(label, SparseVector(len(featureMap), predictors))

    ### split our data set into 2, this is one way to do it but this is obnoxious
    trainingRows = rows.filter(lambda row: ''.join(row.values()).__hash__() %2)
    testRows = rows.filter(lambda row: not ''.join(row.values()).__hash__() %2)

    ### make labeled points out of each set of rows
    training = trainingRows.map(makeLabeledPoint)
    test = testRows.map(makeLabeledPoint)

    star_print('some feature vectors')
    for lp in training.take(10):
        print lp.features

    ### train a model with naive bayes on the training set
    model = NaiveBayes.train(training, 1.0)

    ### use the model to find predictions on test set
    prediction = test.map(lambda lp: model.predict(lp.features))

    ### combine predictions and existing test labels
    predictionAndLabel = prediction.zip(test.map(lambda lp: lp.label))

    ### check accuracy of predictions against test labels
    accuracy = 1.0 * predictionAndLabel.filter(lambda (p,l) : p == l).count() / test.count()
    star_print('accuracy')
    print accuracy

def star_print(s, stars=10):
    print '*' * stars, s, '*' * stars

def make_dict(line, columns):
    [row] = unicode_csv_reader([line])
    return dict(zip(columns, row))

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')


if __name__=='__main__':
    main()
