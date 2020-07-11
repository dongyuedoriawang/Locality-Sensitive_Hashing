from os import environ
from pyspark import SparkConf, SparkContext
import sys
import csv
import itertools
import time

conf = SparkConf().setAppName('SON')
sc = SparkContext(conf=conf)

starttime = time.time()

# Read input variables
threshold = int(sys.argv[1])
support = int(sys.argv[2])
input_path = sys.argv[3]
output_path = sys.argv[4]


#########################
# Read and Write CSV file
with open(input_path, 'r') as file_tafeng:
    my_reader = csv.reader(file_tafeng, delimiter=',')

    header = next(my_reader)

    lines = [['DATE-CUSTOMER_ID', 'PRODUCT_ID']]

    for row in my_reader:

        if row != header:

            listing = [row[0][:-4]+row[0][-2:]+'-'+str(int(row[1])), int(row[5])]
            lines.append(listing)

with open('Customer_product.csv', 'w') as writeFile:
    writer = csv.writer(writeFile)
    writer.writerows(lines)

#########################
# Read into RDD
rdd = sc.textFile('Customer_product.csv')
rawrdd = rdd.map(lambda x: x.split(","))

head = rawrdd.first()
tafengrdd = rawrdd.filter(lambda x: x != head)

# Check result
# print(tafengrdd.collect())

#########################
# Create a basket for each date-customer
# cust_basket = tafengrdd.map(lambda x: (x[0], [x[1]])).sortByKey().reduceByKey(lambda a, b: a + b).filter(lambda x: len(x[1])>threshold)
cust_basket = tafengrdd.map(lambda x: (x[0], {x[1]})).reduceByKey(lambda a, b: a | b).filter(lambda x: len(x[1]) > threshold)
# print(cust_basket.take(10))
#
chunks_rdd = cust_basket.map(lambda x: x[1])
# print(chunks_rdd.take(10))

# Get Number of Partitions
# num_partition = chunks_rdd.getNumPartitions()
# print(num_partition)

#
total_baskets_count = chunks_rdd.count()

# Load defined functions
def freq_single_generater(baskets, mid_support):
    ## Support may need to change
    # mid_support = input_support/num_partition

    singleton_counts = {}
    freq_list = []

    for each in baskets:
        for item in each:
            if item not in singleton_counts:
                singleton_counts[item] = 1
            else:
                if singleton_counts[item] < mid_support:
                    singleton_counts[item] += 1
                    if singleton_counts[item] >= mid_support:
                        freq_list.append(item)

    # for each in singleton_counts:
    #     if singleton_counts[each] >= mid_support:
    #         freq_list.append(each)

    return sorted(freq_list)


# Print out the result list of Freq Singletons
# print(freq_single_generater(user_basket.map(lambda x: x[1]).collect(), support))

################################################################
## Generate combinations starting with singletons for each chunk

def combo_generator(baskets, input_freqs, size, mid_support):

    # mid_support = input_support/num_partition
    if size == 2:
        pair_candidates = {}
        freq_cand = []

        pairs = itertools.combinations(input_freqs, 2)
        for pair in pairs:
            for basket in baskets:
                if set(pair).issubset(basket):
                    pair_candidates.setdefault(pair, 0)
                    if pair_candidates[pair] < mid_support:
                        pair_candidates[pair] += 1
                        if pair_candidates[pair] >= mid_support:
                            freq_cand.append(pair)

        # for pair in pair_candidates:
        #     if pair_candidates[pair] >= mid_support:
        #         freq_cand.append(pair)

    else:
        initial_can = []
        can_count = {}
        freq_cand = []
        combos = itertools.combinations(input_freqs, 2)
        for combo in combos:
            item_a = set(combo[0])
            item_b = set(combo[1])
            if len(item_a.intersection(item_b)) == size-2:
                can = tuple(sorted(item_a.union(item_b)))
                if can not in initial_can and set(itertools.combinations(can, size-1)).issubset(input_freqs):  ####
                    initial_can.append(can)

        for basket in baskets:                              ####
            for itemset in initial_can:
                can_count.setdefault(itemset, 0)         ####
                if can_count[itemset] < mid_support:
                    if set(itemset).issubset(basket):
                        can_count[itemset] += 1
                        if can_count[itemset] >= mid_support:
                            freq_cand.append(itemset)

    return sorted(freq_cand)


def chunk_iteration(baskets, support):

    baskets_list = []
    for basket in baskets:
        baskets_list.append(basket)

    num_baskets = len(baskets_list)

    mid_support = support*float(num_baskets)/total_baskets_count
    # mid_support = 5
    cand_list = []

    # generate singletons for each chunks
    past_freq = freq_single_generater(baskets_list, mid_support)

    cand_list.extend(tuple([item,]) for item in past_freq)

    # print(cand_list)

    size = 2

    # generate combos for each chunk
    while True:
        next_freq = combo_generator(baskets_list, past_freq, size, mid_support)

        if len(next_freq) == 0:
            break

        cand_list.extend(next_freq)
        size += 1
        past_freq = next_freq

    return cand_list


#####################
# Execution

# candidates_count = chunks_rdd.mapPartitions(lambda x: chunk_iteration(x, support)).flatMap(lambda x: x).\
#     map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)

candidates_count = chunks_rdd.mapPartitions(lambda x: chunk_iteration(x, support)).distinct().groupBy(len)
# candidates_count = chunks_rdd.mapPartitions(lambda x: chunk_iteration(x, support)).distinct()
# print(candidates_count.collect())
candidates_list = candidates_count.collectAsMap()

candidates = []
for key in candidates_list.keys():
    for item in candidates_list[key]:
        candidates.append(item)

sort_candidates = sorted(candidates, key=lambda i: (len(i), i))


# (key, ResultIterable[])
## Candidates_list is correct!!!
# for key in candidates_list.keys():
#     for item in candidates_list[key]:
#         print(item)

# print(candidates_count.take(10))
# print(candidates_list)

#####
frequents = []
freq_count = {}
for basket in chunks_rdd.collect():  ####
    for key in candidates_list.keys():
        for item in candidates_list[key]:
            freq_count.setdefault(item, 0)
            if freq_count[item] < support:
                if set(item).issubset(basket):
                    freq_count[item] += 1
                    if freq_count[item] >= support:
                        frequents.append(item)

sort_frequents = sorted(frequents, key=lambda i: (len(i), i))
# print(sort_frequents)


# Define a lines generator
def lines_generator(res):
    singleton = []
    combo_dict = {}
    len_count = 2
    singleton = []
    for item in res:
        if len(item) > len_count:
            len_count += 1
            combo_dict.setdefault(len_count, [])
            combo_dict[len_count].append(str(item))

        elif len(item) == 1:
            singleton.append(str(item).replace(",", ''))

        else:
            #         elif len(item) == len_count:
            combo_dict.setdefault(len_count, [])
            combo_dict[len_count].append(str(item))

    lines = []
    lines.append(",".join(singleton))
    for key in combo_dict:
        lines.append(",".join(combo_dict[key]))

    return lines

# with open('Dongyue_Wang_task2.txt', 'w') as outfile:
with open(output_path, 'w') as outfile:

    outfile.write("Candidates:\n")
    outfile.write('\n\n'.join(lines_generator(sort_candidates)))
    outfile.write("\n\nFrequent Itemsets:\n")
    outfile.write('\n\n'.join(lines_generator(sort_frequents)))


duration = time.time() - starttime
print("Duration:", duration)





















