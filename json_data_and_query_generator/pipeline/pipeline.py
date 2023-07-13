"""
Driver script that runs the faker data generator and the query generator together
"""
import sys
import multiprocessing
import os
from datetime import datetime
import argparse
from json_data_and_query_generator.query_generator.query_generator import (
    SchemaBasedGenerator,
    StandaloneGenerator,
)
from json_data_and_query_generator.data_generators.faker_generator.json_gen import (
    DataGenerator,
)
import json
import tempfile
import shutil


def stopwatch(name, fct, argList):
    start = datetime.now()
    print("### start %s at: %s" % (name, start))

    if type(argList) == type(list()):
        data = fct(*argList)
    if type(argList) == type(dict()):
        data = fct(**argList)

    stop = datetime.now()
    print("### stop %s: %s" % (name, stop))
    print("took %s" % (stop - start))


def setup_output_dirs(workbook, output_dir, overwrite):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    workbook_dir = os.path.join(output_dir, workbook)
    queries_dir = os.path.join(workbook_dir, "queries")
    data_dir = os.path.join(workbook_dir, "data")
    if os.path.exists(workbook_dir) and overwrite:
        shutil.rmtree(workbook_dir)
    if os.path.exists(workbook_dir):
        raise Exception("Workbook " + workbook + " exsists!")
    if not os.path.exists(workbook_dir):
        os.makedirs(workbook_dir)
    if not os.path.exists(queries_dir):
        os.makedirs(queries_dir)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    config_template = {
        "run_insert_bench": True,
        "run_query_bench": True,
        "user_counts": [1, 5, 10],
        "batch_sizes": [1000, 10000],
        "index_cfg": {
            "collection1": [{"type": "int", "path": ["path", "to", "key1"]}],
            "collection2": [{"type": "string", "path": ["path", "to", "key2"]}],
        },
    }

    config_path = os.path.join(workbook_dir, "config.json")
    with open(config_path, "w", encoding="utf8") as config_file:
        json.dump(config_template, config_file, indent=4)

    return data_dir, queries_dir


def printSummary(args):
    max_length = (
        max(
            len(str(args.schema_config)),
            len(str(args.query_config)),
            len(str(args.output)),
        )
        + 4
    )
    print("_" * max_length + "_")
    print("|  SUMMARY ")
    print("_" * max_length + "_")
    print("|  schema used for data generator:")
    print("|   ", args.schema_config, "|")
    print("|  schema used for query generator: ")
    print("|   ", args.query_config, "|")
    print("|  files written to:")
    print("|   ", os.path.join(args.output, args.workbook), "|")
    print("|" + "_" * max_length + "_|")
    print()
    print()


def runDataGenerator(args, data_dir):
    DG = DataGenerator(data_dir, os.path.abspath(args.schema_config))
    schema = DG.generate_schema()

    temp_dir = os.path.join(data_dir, "temp")
    os.mkdir(temp_dir)
    temp_filename = "temp%s.json"
    merged_filename = "merged.json"
    final_filename = "{}.json".format(args.collection_name)
    temp_filepath = os.path.join(temp_dir, temp_filename % "*")
    merged_filepath = os.path.join(data_dir, merged_filename)
    final_filepath = os.path.join(data_dir, final_filename)

    jobs = []

    if int(args.num_proc) < 1 or int(args.num_proc) > 100:
        raise RuntimeError(
            "Num proc is {} and therefore exceeds valid value range".format(
                args.num_proc
            )
        )

    if int(args.num_proc) == 1:
        DG.actualGenerator(
            args.num_proc, schema, os.path.join(temp_dir, temp_filename % 0)
        )
    else:
        for i in range(int(args.num_proc)):
            p = multiprocessing.Process(
                target=DG.actualGenerator,
                args=(args.num_proc, schema, os.path.join(temp_dir, temp_filename % i)),
            )
            jobs.append(p)
            p.start()

        for job in jobs:
            job.join()

    # merge files
    # merge_command = "sed 1d %s > %s" % (temp_filepath, merged_filepath)
    def merge_files(file_out):
        with open(file_out, "w+") as fout:
            for i in range(int(args.num_proc)):
                with open(os.path.join(temp_dir, temp_filename % i)) as g_in:
                    fout.write(g_in.read())

    stopwatch(
        "merging parrallelly constructed files in '{}'".format(str(merge_files)),
        merge_files,
        [merged_filepath],
    )
    stopwatch(
        "adapting to forced values",
        DG.dataGenerator_adapt,
        {"InFilePath": merged_filepath, "OutFilePath": final_filepath},
    )

    shutil.rmtree(temp_dir)
    os.remove(merged_filepath)


def runQueryGenerator(args, queries_dir):
    with open(os.path.abspath(args.query_config), encoding="utf8") as query_cfg_file:
        with open(
            os.path.abspath(args.schema_config), encoding="utf8"
        ) as schema_cfg_file:
            query_cfg = json.load(query_cfg_file)
            schema_cfg = json.load(schema_cfg_file)
            if query_cfg["collection"] != args.collection_name:
                print(
                    "WARNING: Collection name from query config was overwritten. Use the --collection-name parameter!"
                )
                query_cfg["collection"] = args.collection_name
            schema_based_generator = SchemaBasedGenerator(query_cfg, schema_cfg)
            standalone_configs = schema_based_generator.run()
            for i, c in enumerate(standalone_configs):
                g = StandaloneGenerator(
                    c, queries_dir, "{}_{}".format(args.query_base_name, i)
                )
                g.run()


def getArgParser():
    default_out = os.path.join(tempfile.gettempdir(), "scenario")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        "-o",
        help='Path to the output directory for the generated query files. Defaults to "{}"'.format(
            str(default_out)
        ),
        default=default_out,
    )
    parser.add_argument(
        "--schema-config",
        help="Path to the schema config file. Defaults to example",
        default=None,
    )
    parser.add_argument(
        "--query-config",
        help="Path to the query generator config file. Defaults to example",
        default=None,
    )
    parser.add_argument(
        "--query-base-name",
        help='Base name of the generated queries. Defaults to "query"',
        default="query",
    )
    parser.add_argument(
        "--collection-name",
        help="Name of the collection to be created. Defaults to mycol",
        default="mycol",
    )
    parser.add_argument(
        "--num-proc",
        help="Number of processes for the data generation. Defaults to 1",
        default=1,
    )
    parser.add_argument(
        "--no-query", help="generate only data", default=False, action="store_true"
    )
    parser.add_argument("--workbook", "-w", help="workbook name", default="workbook1")
    parser.add_argument(
        "--overwrite",
        help="force overwrite of the data",
        default=False,
        action="store_true",
    )

    return parser


def parsArguments(arguments, parser):
    args = parser.parse_args(arguments)
    this_file_path = os.path.abspath(__file__)
    default_dir_configs = os.path.join(
        os.path.dirname(this_file_path), "..", "examples", "hello_data"
    )
    if args.schema_config is None:
        args.schema_config = os.path.join(
            default_dir_configs, "00_schema_config_example.json"
        )
    if args.query_config is None:
        args.query_config = os.path.join(
            default_dir_configs, "00_query_config_example.json"
        )
    return args


def main(arguments):
    parser = getArgParser()
    args = parsArguments(arguments, parser)

    data_dir, queries_dir = setup_output_dirs(
        args.workbook, os.path.abspath(args.output), args.overwrite
    )

    # ================= #
    # Data generator
    # ================= #
    runDataGenerator(args, data_dir)

    if args.no_query:
        printSummary(args)
        sys.exit(0)

    # ================= #
    # Query generator
    # ================= #
    runQueryGenerator(args, queries_dir)
    printSummary(args)


if __name__ == "__main__":
    main(sys.argv[1:])
