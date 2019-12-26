import json
import datetime

PRODUCT_VIEWED_EVENT = lambda: 'product_viewed'

ADDED_TO_CART_EVENT = lambda: 'product_added_to_cart'

ANONYMOUS_ID = lambda: 'anonymousId'

DATE_TIME_FORMAT = lambda: '%Y-%m-%dT%H:%M:%S.%fZ'

data_file_name = "data.txt"


def get_the_sequence(user_data_list, starting_index):
    ls = []
    for i in range(starting_index, len(user_data_list)):
        user_data = user_data_list[i]

        if "event" in user_data and user_data["event"] == PRODUCT_VIEWED_EVENT():
            product_id = user_data["properties"]["product_id"]
            product_type = user_data["properties"]["product_type"]
            ls.append(Product(product_id, product_type))
        else:
            if len(ls) == 0:
                return None, i
            return ls, i
    return ls, len(user_data_list)


def skip_the_sequence(user_data_list, starting_index):
    for i in range(starting_index, len(user_data_list)):
        if "event" in user_data_list[i] and user_data_list[i]["event"] == PRODUCT_VIEWED_EVENT():
            return i
    return len(user_data_list)


# from pyspark import SparkConf, SparkContext
# conf = SparkConf().setMaster("local").setAppName("first")
# sc = SparkContext(conf=conf)
class Product:

    def __init__(self, pid, ptype, cart_in=False):
        self.pid = pid
        self.ptype = ptype
        self.cart_in = cart_in

    def __eq__(self, other):
        return other.pid == self.pid

    def __hash__(self):
        return hash((self.pid, self.ptype))

    def __repr__(self):
        if self.cart_in:
            return str([self.pid, self.ptype, "in cart"])
        return str([self.pid, self.ptype])


# data = sc.textFile("data.txt")
#
# with open(data_file_name) as file:
#     dataList = json.load(file)


def read_data_file(file_name):
    data_list = []
    with open(file_name, 'r') as file:
        for line in file:
            data_list.append(json.loads(line))
    return data_list


data_list = read_data_file(data_file_name)
# print(dataList)

userGroupedData = dict()

for data in data_list:
    anonymous_id = data[ANONYMOUS_ID()]
    userGroupedData.setdefault(anonymous_id, []).append(data)

frequency_mapping = dict()

for anonymous_id, user_data_list in userGroupedData.items():
    user_data_list.sort(key=lambda obj: datetime.datetime.strptime(obj["receivedAt"], DATE_TIME_FORMAT()))

    current_ls = []
    counter = 0
    while counter < len(user_data_list):
        sequence, counter = get_the_sequence(user_data_list, counter)
        if sequence is not None:
            if counter < len(user_data_list) and "event" in user_data_list[counter] and user_data_list[counter][
                "event"] == ADDED_TO_CART_EVENT():
                user_data = user_data_list[counter]
                product_id = user_data["properties"]["product_id"]
                product_type = user_data["properties"]["product_type"]
                sequence.append(Product(product_id, product_type, True))
                counter += 1
            sequence = tuple(sequence)

            frequency_mapping[sequence] = frequency_mapping.get(sequence, 0) + 1
        counter = skip_the_sequence(user_data_list, counter)

write_file = open('result.txt', 'w')

for key, val in frequency_mapping.items():
    ls = []
    for i in range(len(key)):
        ls.append(str(key[i]))
        if i != len(key) - 1:
            ls.append('->')
    write_file.write(' '.join(ls) + ' : ' + str(val) + '\n')

write_file.close()
