from pyspark import *
import csv

def main():
    sc = SparkContext("local")

    bus_stop_exploration(sc)


def bus_stop_exploration(sc):
    bus_stop_text = sc.textFile("bus_stops.csv")

    star_print("first line in the file, it's the header")
    header = bus_stop_text.first()
    print header

    star_print("first 5 lines in the file")
    for line in bus_stop_text.take(5):
      print line

    star_print("number of lines in the file")
    print bus_stop_text.count() #11639

    star_print("parse the header into columns")
    columns = csvLineToList(header)
    print columns

    star_print("parse the rows")
    rows = bus_stop_text.map(lambda line: makeRow(line, columns))
    for row in rows.take(3):
        print row

    star_print("we made a row from the header, we don't want that, take it out")
    rows = bus_stop_text.filter(lambda line: line != header) \
        .map(lambda line: makeRow(line, columns))
    for row in rows.take(3):
        print row

    def makeRowFunc(columns):
        return lambda line: makeRow(line,columns)

    ## same but with a function def
    rows = bus_stop_text.filter(lambda line: line != header) \
        .map(makeRowFunc(columns))


    star_print("let's grab the stop names out")
    stop_names = rows.map(lambda row: row.get('stop_name'))
    for stop_name in stop_names.take(5):
        print stop_name

    star_print("how many have an '&' in them?")
    with_and = stop_names.filter(lambda s: '&' in s)
    print with_and.count()

    star_print("can we split by & and find the streets?")
    streets = with_and.map(lambda s: s.split('&'))
    for street in streets.take(5):
        print street

    star_print("let's flatmap the intersections to get streets, and strip whitespace")
    streets = with_and.flatMap(lambda s: s.split('&')) \
        .map(lambda s: s.strip())
    for street in streets.take(5):
        print street

    star_print("how many streets did we parse? about double the number of intersections")
    print streets.count()

    star_print("let's make some stats now, first we transform streets into a tuple of the form (street, 1)")
    street_one = streets.map(lambda s: (s,1))
    print street_one.first()

    star_print("we use reduce by key to add up all the 1's with the same street")
    street_count = street_one.reduceByKey(lambda x,y: x+y)
    for item in street_count.take(5):
        print item

    star_print("now we reverse the tuples and sort by the count descending")
    count_street = street_count.map(lambda (street, count): (count, street))
    for item in count_street.sortByKey(False).take(10):
        print item

    star_print("let's parse out the latitudes and check their range and average")
    lats = rows.map(lambda row: float(row['stop_lat']))
    print 'max lat = ', lats.max()
    print 'min lat = ', lats.min()
    print 'avg lat = ', lats.mean()

    star_print("0.01 degrees seems a reasonable bin size for lat/lon")
    print '1/100 of the latitude range = ',(lats.max() - lats.min()) / 100

    star_print("let's round the lat/lons to precision 2 and make bins out of them")
    bins = rows.map(lambda row: makeLatLonBin(row['stop_lat'], row['stop_lon']))
    for bin in bins.take(5):
        print bin

    star_print("now let's see what the densest bins are")
    bin_count =  bins.map(lambda b: (b,1)).reduceByKey(lambda x,y: x+y)
    count_bin = bin_count.map(lambda (b,c): (c,b)).sortByKey(False)
    for item in count_bin.take(5):
        print item

    star_print("it's even easier if we use a function for common operations, same result")
    for item in sortedCount(bins, False).take(5):
        print item



def csvLineToList(line):
    [result] = csv.reader([line])
    return result


def makeRow(line, columns):
    row_values = csvLineToList(line)
    row = dict(zip(columns, row_values))
    return row

def makeLatLonBin(lat_str, lon_str, precision=2):
    lat = round(float(lat_str),precision)
    lon = round(float(lon_str),precision)
    return (lat, lon)


def keyCounts(rdd):
    return rdd.map(lambda v: (v,1)) \
        .reduceByKey(lambda x,y: x+y)

def sortedCount(rdd, ascending=True):
    key_counts = keyCounts(rdd)
    return key_counts.map(lambda (v,c) : (c,v)) \
        .sortByKey(ascending)

def star_print(s, stars=10):
    print '\n','*' * stars, s, '*' * stars



if __name__=='__main__':
    main()
