import time
from pyspark import SparkContext
from itertools import combinations
import sys
import math
import os
import csv
from collections import Counter

time_begin = time.time()

def length(box):
    if type(box) is tuple:
        return len(box)
    else:
        return 1

def get_possible_size_two(freq_items):
	poss = list()
	for combination in combinations(freq_items, 2):
		combination = list(combination)
		combination.sort()
		poss.append(combination)
	return poss

def get_possible_items_size_n(freq_items, n):
	freq_items = list(freq_items)
	combinations1 = list()	
	for i in range(len(freq_items) - 1):
		for j in range( i + 1, len(freq_items)):
			first = freq_items[i]
			second = freq_items[j]
			if first[0:(n - 2)] == second[0:(n - 2)]:
                                l = list ( set(first) | set(second))
				combinations1.append(l)
			else:
				break
	return combinations1
			
def get_frequent_items_within_limit(baskets, candidates, support_limit):
	item_counts_n = {}
	for candidate in candidates:
		candidate = set(candidate)
		key = tuple(sorted(candidate))

		for basket in baskets:
			if candidate.issubset(basket):
				if key in item_counts_n:
				        item_counts_n[key] = item_counts_n[key] + 1
				else:
				        item_counts_n[key] = 1

	item_counts_n = Counter(item_counts_n)
	frequent_items = {a : item_counts_n[a] for a in item_counts_n if item_counts_n[a] >= support_limit }
	frequent_items = sorted(frequent_items)
        return frequent_items

def apriori(baskets, support, totalCount):
	baskets = list(baskets)
	support_limit = support*(float(len(baskets))/float(totalCount))
        result = list()
	items_and_count = Counter()
	for basket in baskets:
		items_and_count.update(basket)
        singles = {t : items_and_count[t] for t in items_and_count if items_and_count[t] >= support_limit } 
	freq_singles = sorted(singles)
	result.extend(freq_singles)

	n = 2
	freq_items = set(freq_singles)
        frequent_items1 = {a[0] for a in freq_items}
	while len(freq_items) != 0:
		if n == 2:
			candidateFrequentItems = get_possible_size_two(freq_items)
		else:
			candidateFrequentItems = get_possible_items_size_n(freq_items, n)

		newFrequentItems = get_frequent_items_within_limit(baskets, candidateFrequentItems, support_limit)
		result.extend(newFrequentItems)
		freq_items = list(set(newFrequentItems))
		freq_items.sort()
		n = n + 1
	return result


def count_candidates(baskets, candidates):
        all_baskets = list(baskets)
	item_counts = {}

	for candidate in candidates:
		key = candidate
                if type(candidate) is str:
                        candidate_string = candidate
                        candidate = set()
                        candidate.add(candidate_string)
                else:        
		        candidate = set(candidate)
		for basket in all_baskets:
			if candidate.issubset(basket):
				if key in item_counts:
					item_counts[key] = item_counts[key] + 1
				else:
					item_counts[key] = 1
	items = item_counts.items()
        return items

def main():
    input_file = sys.argv[1]
    support = int(sys.argv[2])


    spark_context = SparkContext("local[*]", "SON")
    rdd1 = spark_context.textFile(input_file, minPartitions = None, use_unicode = False)
    rdd1 = rdd1.mapPartitions(lambda x : csv.reader(x))
    all_baskets = rdd1.map(lambda x : ((x[0]), (x[1]))).groupByKey().mapValues(set)
    all_baskets = all_baskets.map(lambda x : x[1])
    all_count = all_baskets.count()
    first_map = all_baskets.mapPartitions(lambda baskets : apriori(baskets, support, all_count)).map(lambda x : (x,1))
    first_reduce = first_map.reduceByKey(lambda x,y: (1)).keys().collect()
    second_map = all_baskets.mapPartitions(lambda baskets : count_candidates(baskets, first_reduce))
    second_reduce = second_map.reduceByKey(lambda x,y: (x+y)) 
    item_sets = second_reduce.filter(lambda x: x[1] >= support)


    frequent_items = item_sets.keys().collect()
    frequent_items = sorted(frequent_items, key = lambda item: (length(item), item))
    full_file_name = os.path.basename(input_file)
    file_name = os.path.splitext(full_file_name)[0]
    output_file = "Devansh_Sharma_SON_" + file_name + "_" + str(support) + ".txt"
    out_ptr = open(output_file, "w")

    if len(frequent_items) != 0:
	len_item = length(frequent_items[0])
	out_ptr.write("(" + frequent_items[0] + ")")
	for i in range(1, len(frequent_items)):
		len_cur_item = length(frequent_items[i])
		if len_item == len_cur_item:
			out_ptr.write(", ")
		else:
			out_ptr.write("\n\n")

		if len_cur_item == 1:
			strVal = str("(" + frequent_items[i] + ")")
		else:
			strVal = str(frequent_items[i])
		out_ptr.write(strVal)
		len_item = len_cur_item

if __name__ == "__main__":
    main()
    time_end = time.time()
    print "Runs in : ", time_end - time_begin, " seconds"


