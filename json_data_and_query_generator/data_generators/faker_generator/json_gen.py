import os
import ast
import collections.abc
from datetime import datetime
from xml.dom.minidom import Attr
import numpy as np
import random
import json
import faker as fakerModule
import faker.providers as FakeProviders

FakerInstanceForKeys = fakerModule.Faker()

# join two dictionaries :


class DeepFakerSchema(object):
    def __init__(self, faker=None, locale=None, providers=None, includes=None):
        self._faker = faker or fakerModule.Faker(
            locale=locale, providers=providers, includes=includes
        )

    def generate_fake(self, schema, iterations=1):
        result = [self._generate_one_fake(schema) for _ in range(iterations)]
        return result[0] if len(result) == 1 else result

    def _generate_one_fake(self, schema):
        """
        Recursively traverse schema dictionary and for each "leaf node", evaluate the fake
        value

        Implementation:
        For each key-value pair:
        1) If value is not an iterable (i.e. dict or list), evaluate the fake data (base case)
        2) If value is a dictionary, recurse
        3) If value is a list, iteratively recurse over each item
        """
        data = {}
        for k, v in schema.items():
            if isinstance(v, dict):
                data[k] = self._generate_one_fake(v)
            elif isinstance(v, list):
                data[k] = [self._generate_one_fake(item) for item in v]
            else:
                tokens = v.split("(")
                if len(tokens) > 1:
                    argument = tokens[1].split(")")[0]
                    data[k] = getattr(self._faker, tokens[0])(int(argument))
                else:
                    data[k] = getattr(self._faker, v)()
        return data


def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def populate_dict(path, existing_dict, valueType):
    if len(path) == 1:
        existing_dict[path[0]] = valueType
    else:
        head, tail = path[0], path[1:]
        existing_dict.setdefault(head, {})
        populate_dict(tail, existing_dict[head], valueType)


def add_field(path, valueType, schema):
    populate_dict(path, schema, valueType)


def add_level(p, schema):
    populate_dict(p, schema, {})


def add_level_and_field(p, valueType, schema):
    add_level(p, schema)
    p.append(FakerInstanceForKeys.word())
    add_field(p, valueType, schema)


def count_fields(schema):
    count = 0
    for k in schema.keys():
        count += 1
        if isinstance(schema[k], dict):
            count += count_fields(schema[k])
    return count


def get_list_of_levels(schema):
    levels = [[]]
    for k in schema.keys():
        if isinstance(schema[k], dict):
            subLevels = [[k] + x for x in get_list_of_levels(schema[k])]
            levels.extend(subLevels)
    return levels


def get_depth(schema):
    return max([len(x) for x in get_list_of_levels(schema)])


def get_longest_path(schema):
    pathList = get_list_of_levels(schema)
    return pathList[np.argmax([len(x) for x in pathList])]


class DataGenerator:
    def __init__(self, data_dir, schema_config_filepath="./schemaConfig.json"):
        """
        Args:
            config: config
            schema_config: schema config
        """

        self.schema = "NOT SET"
        self.data_dir = data_dir

        self.CONFIG_FILEPATH = schema_config_filepath

        with open(self.CONFIG_FILEPATH, "r") as inf:
            self.configDict = eval(inf.read())

        self.FORCED_PATHS = []
        self.NUM_FIELDS = 0
        self.LEN_FIELDS = 0
        self.NUM_LEVELS = 0
        self.NUM_SAMPLES = 0

        if "forcedPaths" in self.configDict.keys() and isinstance(
            self.configDict["forcedPaths"], list
        ):
            self.FORCED_PATHS = self.configDict["forcedPaths"]

        if "numFields" in self.configDict.keys():
            self.NUM_FIELDS = self.configDict["numFields"]

        if "lenFields" in self.configDict.keys():
            self.LEN_FIELDS = self.configDict["lenFields"]

        if "numLevels" in self.configDict.keys():
            self.NUM_LEVELS = self.configDict["numLevels"]

        if "numSamples" in self.configDict.keys():
            self.NUM_SAMPLES = self.configDict["numSamples"]

    ######################################################
    # GENERATE SCHEMA
    ######################################################

    def generate_schema(self):
        schema = {}
        for pathDict in self.FORCED_PATHS:
            path = pathDict["path"]
            valueType = pathDict["valueType"]
            if "operator" in pathDict.keys():
                op = pathDict["operator"]
                val = pathDict["value"]
                num = pathDict["num"]

            populate_dict(path, schema, valueType)

        # facilitate arrays

        def iter_paths(d):
            def iter1(d, path):
                paths = []
                for k, v in d.items():
                    if isinstance(v, dict):
                        paths += iter1(v, path + [k])
                    paths.append((path + [k], v))
                return paths

            return iter1(d, [])

        ARRAY_PATHS = []

        for x in iter_paths(schema):
            path = x[0]
            if "[" in path[-1]:
                ARRAY_PATHS.append(path)

        for path in ARRAY_PATHS:
            d = schema
            for key in path[:-1]:
                d = d[key]

            oldKey = path[-1]
            OLD_CONTENT = d[oldKey]

            new_key, num = oldKey.split("[")
            num = num.split("]")[0]
            LEN_ARR = int(num)

            d[new_key] = [OLD_CONTENT for i in range(LEN_ARR)]

            del d[oldKey]

        DUMMY_FIELD_TYPE = "text"

        while get_depth(schema) < self.NUM_LEVELS:
            p = get_longest_path(schema)
            p.append(FakerInstanceForKeys.word())
            valueType = DUMMY_FIELD_TYPE
            add_level_and_field(p, valueType, schema)

        levels = get_list_of_levels(schema)
        while count_fields(schema) < self.NUM_FIELDS:
            p = random.choice(levels)
            p = p + [FakerInstanceForKeys.word()]
            # print(p)
            add_field(p, DUMMY_FIELD_TYPE, schema)

        print("#" * 20)
        print("### Full Config ###")
        print("#" * 20)

        print(json.dumps(self.configDict, sort_keys=True, indent=4))

        print("#" * 20)
        print("### Full schema ###")
        print("#" * 20)

        print(json.dumps(schema, sort_keys=True, indent=4))

        # print("#"*20)
        # print("### number of fields:  %s ###" % count_fields(schema))
        print("#" * 20)
        print("list of levels")
        print(get_list_of_levels(schema))
        print("#" * 20)

        self.schema = schema

        return schema

    def actualGenerator(self, num_PROC, schema, outputPath):
        ######################################################
        # GENERATE DATASET
        ######################################################

        num_SAMPLES = int(int(self.NUM_SAMPLES) / int(num_PROC))

        FakerInstanceForValues = fakerModule.Faker()
        fakerModule.Faker.seed(datetime.now())

        faker = DeepFakerSchema(faker=FakerInstanceForValues)

        start = datetime.now()

        print("### generate sample of size: %s" % num_SAMPLES)

        print("### start faking schema at: %s" % start)
        data = faker.generate_fake(schema, iterations=num_SAMPLES)
        stop = datetime.now()
        print("### stop faking schema at: %s" % stop)
        print("took %s" % (stop - start))

        print("Write data to ", outputPath)
        with open(outputPath, "w") as file1:
            file1.writelines([json.dumps(x) + "\n" for x in data])

        # Dont create schema.txt file in the data directory as it conflicts with the
        # directory structure for benchmark (only json files in this directory)
        schema_txt_path = os.path.dirname(self.data_dir)
        with open(os.path.join(schema_txt_path, "schema.txt"), "w") as file1:
            file1.write(str(schema) + "\n")

    ######################################################
    # ADAPT TO REQUIRED QUERY RESULT
    ######################################################

    def dataGenerator_adapt(
        self, InFilePath="./genDatasets/data.txt", OutFilePath="./genDatasets/data.txt"
    ):
        with open(InFilePath) as f:
            jsons = f.readlines()

        for pathDict in self.FORCED_PATHS:
            path = pathDict["path"]
            if "operator" in pathDict.keys():
                op = pathDict["operator"]
                if not op == "eq":
                    print("Only supported operator is 'eq' until further development.")
                    raise ValueError(
                        "Only supported operator is 'eq' until further development."
                    )
                if not "value" in pathDict.keys():
                    raise ValueError("key 'value' is missing in path")
                val = pathDict["value"]
                if not "num" in pathDict.keys():
                    raise ValueError("key 'num' is missing in path")
                num = int(pathDict["num"])

                dictWithCorrectValue = {}
                populate_dict(path, dictWithCorrectValue, val)

                for i, line in enumerate(jsons[:num]):
                    data = ast.literal_eval(line)
                    update(data, dictWithCorrectValue)
                    jsons[i] = json.dumps(data) + "\n"

                random.shuffle(jsons)

        print("Output file is: " + OutFilePath)
        with open(OutFilePath, "w+") as outfile:
            outfile.write("".join(jsons))
