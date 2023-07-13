import argparse
import json
import itertools
import os
import re
import copy
import random
from jinjasql import JinjaSql

from json_data_and_query_generator.feasibility import feasibility_matrix as fsb

AGGREGATE_FUNCTIONS = ["COUNT", "SUM", "AVG", "MIN", "MAX"]
UNARY_FUNCTIONS = [
    "ABS",
    "ACOS",
    "ASIN",
    "ATAN",
    "COS",
    "LN",
    "SIN",
    "TAN",
    "LENGTH",
    "LOWER",
    "UPPER",
    "TO_BIGINT",
    "TO_DOUBLE",
    "TO_VARCHAR",
]

BINARY_FUNCTIONS_PREFIX = [
    "ATAN2",
    "MOD",
    "POWER",
    "ROUND",
    "CONCAT",
    "LOG",
]

BINARY_FUNCTIONS_INFIX = [
    "+",
    "-",
    "*",
    "/",
]

STANDARD_PLACEHOLDERS = {
    "AGGREGATE_FCT": AGGREGATE_FUNCTIONS,
    "UNARY_FCT": UNARY_FUNCTIONS,
    "BINARY_FCT_PREFIX": BINARY_FUNCTIONS_PREFIX,
    "BINARY_FCT_INFIX": BINARY_FUNCTIONS_INFIX,
}


class SchemaBasedGenerator:
    """
    Schema based (based on the data generator) query generation.
    Creates a standalone config from a schema config(used by the data generator) and it's own config
    """

    def __init__(self, config, schema_config):
        """
        Args:
            config: config
            schema_config: schema config (by data generator)
        """
        self._cfg = {
            "projection_pool": [
                x
                for x in schema_config["forcedPaths"]
                if self.config_to_matrix_type(x) != "array"
            ],
            "where_clause_pool": [
                x
                for x in schema_config["forcedPaths"]
                if "operator" in x and self.config_to_matrix_type(x) != "array"
            ],
        }
        self._cfg.update(config)
        self.insert_config_default_values()
        self.validate_config()
        self._feasibility_matrix = self.load_feasibility_matrix()

    def run(self):
        """
        Returns a list of configs for the standalone generator
        """
        standalone_configs = []

        for _ in range(self._cfg["number_of_different_queries"]):
            self._placeholder_count = 0  # Reset placeholder names
            placeholders = {}

            project = []

            (
                projection_clause,
                projection_placeholders,
            ) = self._generate_projection_clause(project)
            placeholders.update(projection_placeholders)

            template = 'SELECT {} FROM "{}"'.format(
                projection_clause, self._cfg["collection"]
            )

            if self._should_generate_where_clause():  # create where clause
                where_clause = self._generate_where_clause()
                if len(where_clause) > 0:
                    template += " WHERE {}".format(where_clause)

            if "limit" in self._cfg and self._cfg["limit"] is not None:
                template += " LIMIT {}".format(self._cfg["limit"])

            template += ";"

            standalone_cfg = {
                "template": template,
                "combinations": self._cfg["combinations_per_query"],
                "placeholders": placeholders,
            }
            standalone_configs.append(standalone_cfg)

        return standalone_configs

    def _should_generate_where_clause(self):
        forced_paths_in_where_clause = (
            "forced" in self._cfg["where_clause"]
            and len(self._cfg["where_clause"]["forced"]) > 0
        )
        max_random_in_where_clause = self._cfg["where_clause"]["random"][
            "number_total"
        ][1]

        if forced_paths_in_where_clause or max_random_in_where_clause > 0:
            return random.random() <= self._cfg["where_clause"]["probability"]
        else:
            return False

    def _generate_where_clause(self):
        """ """
        paths_pool = copy.deepcopy(self._cfg["where_clause_pool"])
        filters = []  # list of filter strings

        if (
            "forced" in self._cfg["where_clause"]
            and len(self._cfg["where_clause"]["forced"]) > 0
        ):
            forced_filters, paths_pool = self._generate_where_clause_forced_paths(
                paths_pool
            )
            filters += forced_filters

        random_filters, _ = self._generate_where_clause_random_paths(paths_pool)
        filters += random_filters
        operator = random.choice(self._cfg["where_clause"]["operators"])
        return " {} ".format(operator).join(filters)

    def _generate_where_clause_forced_paths(self, paths_pool):
        """ """
        filters = []

        for path_info in self._cfg["where_clause"]["forced"]:
            obj = self._get_forced_path_obj(path_info, paths_pool)
            if not "operator" in obj:
                raise RuntimeError(
                    "Specified where clause path is not viable as where clause: {}".format(
                        path_info
                    )
                )

            op = self.schema_cfg_op_to_str(obj["operator"])
            lhs = self._create_path_expr(self._cfg["collection"], obj["path"])
            rhs = obj["value"]
            paths_pool.remove(obj)
            format_str = (
                "{}{}'{}'" if self.config_to_matrix_type(obj) == "string" else "{}{}{}"
            )
            filters.append(format_str.format(lhs, op, rhs))

        return filters, paths_pool

    def _generate_where_clause_random_paths(self, paths_pool):
        """ """
        number_total = self.randint_from_range(
            self._cfg["where_clause"]["random"]["number_total"], len(paths_pool)
        )

        selected_fields = []
        for _ in range(number_total):
            obj = random.choice(paths_pool)
            paths_pool.remove(obj)
            op = self.schema_cfg_op_to_str(obj["operator"])
            lhs = self._create_path_expr(self._cfg["collection"], obj["path"])
            rhs = obj["value"]
            format_str = (
                "{}{}'{}'" if self.config_to_matrix_type(obj) == "string" else "{}{}{}"
            )
            selected_fields.append(format_str.format(lhs, op, rhs))

        return selected_fields, paths_pool

    def _generate_projection_clause(self, project):
        """ """
        placeholders = {}
        paths_pool = copy.deepcopy(self._cfg["projection_pool"])
        projections = project  # list of projection strings

        if (
            "forced" in self._cfg["projection"]
            and len(self._cfg["projection"]["forced"]) > 0
        ):
            (
                forced_projections,
                forced_placeholders,
                paths_pool,
            ) = self._generate_projection_clause_forced_paths(paths_pool)
            placeholders.update(forced_placeholders)
            projections.extend(forced_projections)

        if "random" in self._cfg["projection"]:
            (
                random_projections,
                random_placeholders,
                _,
            ) = self._generate_projection_clause_random_paths(paths_pool)
            placeholders.update(random_placeholders)
            projections.extend(random_projections)
        return ", ".join(projections), placeholders

    def _generate_projection_clause_random_paths(self, paths_pool):
        """ """
        placeholders = {}
        projections = []

        number_total = self.randint_from_range(
            self._cfg["projection"]["random"]["number_total"], len(paths_pool)
        )

        number_left = number_total
        # print('Number total: {}'.format(number_total))
        fct_types = ["UNARY", "BINARY", "AGGREGATE"]
        random.shuffle(fct_types)
        for fct_type in fct_types:
            # print(fct_type)
            if fct_type == "AGGREGATE":
                viable_paths = [
                    x for x in paths_pool if self.config_to_matrix_type(x) == "number"
                ]
                upper_bound = (
                    len(viable_paths)
                    if len(viable_paths) < number_left
                    else number_left
                )
                nr = self.randint_from_range(
                    self._cfg["projection"]["random"]["number_aggregate_fct"],
                    upper_bound,
                )
                number_left -= nr
                # print('Aggregate fct nr: {}'.format(nr))
                for _ in range(nr):
                    obj = random.choice(viable_paths)
                    path = self._create_path_expr(self._cfg["collection"], obj["path"])
                    p_name = self._get_next_placeholder_name()
                    s = "{{" + p_name + "}}" + "({})".format(path)
                    projections.append(s)
                    placeholders.update({p_name: AGGREGATE_FUNCTIONS})
                    paths_pool.remove(obj)
                    viable_paths.remove(obj)
            if fct_type == "UNARY":
                nr = self.randint_from_range(
                    self._cfg["projection"]["random"]["number_unary_fct"], number_left
                )
                number_left -= nr
                # print('Unary fct nr: {}'.format(nr))
                for _ in range(nr):
                    obj = random.choice(paths_pool)
                    paths_pool.remove(obj)
                    obj_type = self.config_to_matrix_type(obj)
                    feasible_fcts = self._find_feasible_unary_functions(obj_type)
                    assert len(feasible_fcts) > 0
                    path = self._create_path_expr(self._cfg["collection"], obj["path"])
                    p_name = self._get_next_placeholder_name()
                    s = "{{" + p_name + "}}" + "({})".format(path)
                    projections.append(s)
                    placeholders.update({p_name: feasible_fcts})
            if fct_type == "BINARY":
                nr = self.randint_from_range(
                    self._cfg["projection"]["random"]["number_binary_fct"],
                    number_left // 2,
                )
                # print('Binary fct nr: {}'.format(nr))
                for _ in range(nr):
                    viable = []
                    for lhs, rhs in itertools.product(paths_pool, paths_pool):
                        lhs_t = self.config_to_matrix_type(lhs)
                        rhs_t = self.config_to_matrix_type(rhs)
                        viable_prefix_nr = len(
                            self._find_feasible_binary_prefix_functions(lhs_t, rhs_t)
                        )
                        viable_infix_nr = len(
                            self._find_feasible_binary_infix_functions(lhs_t, rhs_t)
                        )
                        if viable_prefix_nr > 0 or viable_infix_nr > 0:
                            viable.append([lhs, rhs, viable_prefix_nr, viable_infix_nr])

                    if len(viable) == 0:
                        print(
                            "WARNING: No viable path combination for binary function found"
                        )
                        break
                    number_left -= 1

                    lhs_and_rhs = random.choice(viable)
                    lhs_obj = lhs_and_rhs[0]
                    rhs_obj = lhs_and_rhs[1]
                    if lhs_obj is rhs_obj:
                        paths_pool.remove(lhs_obj)
                    else:
                        paths_pool.remove(lhs_obj)
                        paths_pool.remove(rhs_obj)

                    lhs_type = self.config_to_matrix_type(lhs_obj)
                    rhs_type = self.config_to_matrix_type(rhs_obj)

                    use_infix = random.random() < float(
                        len(BINARY_FUNCTIONS_INFIX)
                    ) / len(BINARY_FUNCTIONS_INFIX) + len(BINARY_FUNCTIONS_PREFIX)
                    if use_infix:
                        feasible_fcts = self._find_feasible_binary_infix_functions(
                            lhs_type, rhs_type
                        )
                    else:
                        feasible_fcts = self._find_feasible_binary_prefix_functions(
                            lhs_type, rhs_type
                        )

                    path_lhs = self._create_path_expr(
                        self._cfg["collection"], lhs_obj["path"]
                    )
                    path_rhs = self._create_path_expr(
                        self._cfg["collection"], rhs_obj["path"]
                    )
                    p_name = self._get_next_placeholder_name()
                    if use_infix:
                        s = path_lhs + "{{" + p_name + "}}" + path_rhs

                    else:
                        s = path_lhs + "{{" + p_name + "}}" + path_rhs

                    projections.append(s)
                    placeholders.update({p_name: feasible_fcts})

        # Projection with no function applied
        for _ in range(number_left):
            obj = random.choice(paths_pool)
            paths_pool.remove(obj)
            path = self._create_path_expr(self._cfg["collection"], obj["path"])
            projections.append(path)

        return projections, placeholders, paths_pool

    def _generate_projection_clause_forced_paths(self, paths_pool):
        """ """
        placeholders = {}
        projections = []
        for forced_path_info in self._cfg["projection"]["forced"]:
            # each info object is one of 1: {path, fct}, 2: {path},3: {fct}
            if "path" in forced_path_info and "fct" in forced_path_info:
                self._handle_projection_clause_forced_path_option_1(
                    forced_path_info, paths_pool, placeholders, projections
                )

            elif "path" in forced_path_info:
                self._handle_projection_clause_forced_path_option_2(
                    forced_path_info, paths_pool, placeholders, projections
                )

            elif "fct" in forced_path_info:
                self._handle_projection_clause_forced_path_option_3(
                    forced_path_info, paths_pool, placeholders, projections
                )
            else:
                raise RuntimeError(
                    'Neither "fct" nor "path" in forced path in config : {}'.format(
                        forced_path_info
                    )
                )

        return projections, placeholders, paths_pool

    def _handle_projection_clause_forced_path_option_1(
        self, forced_path_info, paths_pool, placeholders, projections
    ):
        """
        Option 1: path and fct specified in config

        Arguments:
            forced_path_info: Object specified in config under 'projection'.'forced'
            paths_pool: Schema config 'forcedPaths' objects that are still left
            placeholders: Dictionary placeholder->[val1, val2, ...] for template instantiation
            projections: List of projections to include in projection clause
        """
        # Two paths specified: It is a binary function
        if type(forced_path_info["path"][0]) is list:
            if len(forced_path_info["path"][0]) > 2:
                raise RuntimeError(
                    "Forced path info has more than 2 paths specified: {}".format(
                        forced_path_info
                    )
                )
            lhs_obj = self._get_forced_path_obj(forced_path_info["path"][0], paths_pool)
            lhs_type = self.config_to_matrix_type(lhs_obj)
            lhs_path = self._create_path_expr(self._cfg["collection"], lhs_obj["path"])
            rhs_obj = self._get_forced_path_obj(forced_path_info["path"][1], paths_pool)
            rhs_type = self.config_to_matrix_type(rhs_obj)
            rhs_path = self._create_path_expr(self._cfg["collection"], rhs_obj["path"])

            # Single function specified: The function is fixed
            if type(forced_path_info["fct"]) is str:
                if not self._is_feasible_binary(
                    forced_path_info["fct"], lhs_type, rhs_type
                ):
                    raise RuntimeError(
                        "Forced path with this function not feasible {}".format(
                            forced_path_info
                        )
                    )

                if self._is_binary_fct_infix(forced_path_info["fct"]):
                    s = "{}{}{}".format(lhs_path, forced_path_info["fct"], rhs_path)
                elif self._is_binary_fct_prefix(forced_path_info["fct"]):
                    s = "{}({},{})".format(forced_path_info["fct"], lhs_path, rhs_path)
                else:
                    raise RuntimeError(
                        "Unknown function in forced path info: {}".format(
                            forced_path_info
                        )
                    )

                projections.append(s)

            # Multiple function specified: The function is not fixed but a placeholder
            elif type(forced_path_info["fct"]) is list:
                if not all(
                    self._is_feasible_binary(f, lhs_type, rhs_type)
                    for f in forced_path_info["fct"]
                ):
                    raise RuntimeError(
                        "Forced path with this functions not feasible {}".format(
                            forced_path_info
                        )
                    )

                if all([self._is_binary_fct_infix(f) for f in forced_path_info["fct"]]):
                    p_name = self._get_next_placeholder_name()
                    s = lhs_path + "{{" + p_name + "}}" + rhs_path
                    projections.append(s)
                    placeholders.update({p_name: forced_path_info["fct"]})

                elif all(
                    [self._is_binary_fct_prefix(f) for f in forced_path_info["fct"]]
                ):
                    p_name = self._get_next_placeholder_name()
                    s = "{{" + p_name + "}}" + "({},{})".format(lhs_path, rhs_path)
                    projections.append(s)
                    placeholders.update({p_name: forced_path_info["fct"]})
                else:
                    raise RuntimeError(
                        "Forced path functions must either all be infix or all prefix {}".format(
                            forced_path_info
                        )
                    )
            else:
                raise RuntimeError(
                    "Forced path function info has wrong type {}".format(
                        forced_path_info
                    )
                )

            paths_pool.remove(lhs_obj)
            paths_pool.remove(rhs_obj)

        # Single path specified: It is a unary function
        elif type(forced_path_info["path"][0]) is str:
            obj = self._get_forced_path_obj(forced_path_info["path"], paths_pool)
            obj_type = self.config_to_matrix_type(obj)
            obj_path = self._create_path_expr(self._cfg["collection"], obj["path"])

            # No function specified: Plain projection
            if forced_path_info["fct"] is None:
                projections.append(obj_path)

            # Single function specified: The function is fixed
            elif type(forced_path_info["fct"]) is str:
                if not self._is_feasible_unary(forced_path_info["fct"], obj_type):
                    raise RuntimeError(
                        "Forced path with this function not feasible {}".format(
                            forced_path_info
                        )
                    )
                s = "{}({})".format(forced_path_info["fct"], obj_path)
                projections.append(s)

            # Multiple functions specified: The function is a placeholder
            elif type(forced_path_info["fct"]) is list:
                if not all(
                    self._is_feasible_unary(f, obj_type)
                    for f in forced_path_info["fct"]
                ):
                    raise RuntimeError(
                        "Forced path with this function not feasible {}".format(
                            forced_path_info
                        )
                    )
                p_name = self._get_next_placeholder_name()
                s = "{{" + p_name + "}}" + "({})".format(obj_path)
                projections.append(s)
                placeholders.update({p_name: forced_path_info["fct"]})
            else:
                raise RuntimeError(
                    "Forced path fct info has wrong type {}".format(forced_path_info)
                )
            paths_pool.remove(obj)

        else:
            raise RuntimeError(
                "Forced path path info has wrong type {}".format(forced_path_info)
            )

    def _handle_projection_clause_forced_path_option_2(
        self, forced_path_info, paths_pool, placeholders, projections
    ):
        """
        Option 2: path specified but fct is a placeholder

        Arguments:
            forced_path_info: Object specified in config under 'projection'.'forced'
            paths_pool: Schema config 'forcedPaths' objects that are still left
            placeholders: Dictionary placeholder->[val1, val2, ...] for template instantiation
            projections: List of projections to include in projection clause
        """
        # Two paths specified: It is a binary function
        if type(forced_path_info["path"][0]) is list:
            lhs_obj = self._get_forced_path_obj(forced_path_info["path"][0], paths_pool)
            lhs_type = self.config_to_matrix_type(lhs_obj)
            lhs_path = self._create_path_expr(self._cfg["collection"], lhs_obj["path"])
            rhs_obj = self._get_forced_path_obj(forced_path_info["path"][1], paths_pool)
            rhs_type = self.config_to_matrix_type(rhs_obj)
            rhs_path = self._create_path_expr(self._cfg["collection"], rhs_obj["path"])

            feasible_prefix_fcts = self._find_feasible_binary_prefix_functions(
                lhs_type, rhs_type
            )
            feasible_infix_fcts = self._find_feasible_binary_infix_functions(
                lhs_type, rhs_type
            )
            if len(feasible_infix_fcts) == 0 and len(feasible_prefix_fcts) == 0:
                raise RuntimeError(
                    "No feasible function for forced path {}".format(forced_path_info)
                )
            use_infix = random.random() <= len(feasible_infix_fcts) / (
                len(feasible_infix_fcts) + len(feasible_prefix_fcts)
            )

            p_name = self._get_next_placeholder_name()
            if use_infix:
                s = lhs_path + "{{" + p_name + "}}" + rhs_path
                u = feasible_infix_fcts
            else:
                s = "{{" + p_name + "}}" + "({},{})".format(lhs_path, rhs_path)
                u = feasible_prefix_fcts
            projections.append(s)
            placeholders.update({p_name: u})
            paths_pool.remove(lhs_obj)
            paths_pool.remove(rhs_obj)

        # Single path specified: It's a unary function
        elif type(forced_path_info["path"][0]) is str:
            obj = self._get_forced_path_obj(forced_path_info["path"], paths_pool)
            obj_type = self.config_to_matrix_type(obj)
            obj_path = self._create_path_expr(self._cfg["collection"], obj["path"])
            # Find feasible functions
            feasible_fcts = self._find_feasible_unary_functions(obj_type)
            if len(feasible_fcts) == 0:
                raise RuntimeError(
                    "No feasible functions for forced path: {}".format(forced_path_info)
                )
            p_name = self._get_next_placeholder_name()
            s = "{{" + p_name + "}}" + "({})".format(obj_path)
            projections.append(s)
            placeholders.update({p_name: feasible_fcts})
            paths_pool.remove(obj)
        else:
            raise RuntimeError(
                "Unsupported type in path info: {}".format(forced_path_info)
            )

    def _handle_projection_clause_forced_path_option_3(
        self, forced_path_info, paths_pool, placeholders, projections
    ):
        """
        Option 3: fct is specified (except when multiple are specified) but path is a placeholder

        Arguments:
            forced_path_info: Object specified in config under 'projection'.'forced'
            paths_pool: Schema config 'forcedPaths' objects that are still left
            placeholders: Dictionary placeholder->[val1, val2, ...] for template instantiation
            projections: List of projections to include in projection clause
        """
        # Multiple functions specified: Functions are a placeholder, paths are a placeholder
        if type(forced_path_info["fct"]) is list:
            # Feasible paths are found by using the first function in the list.
            # The user is responsible that the same paths are feasible for the other
            # functions as well
            fct = forced_path_info["fct"][0]

            # Functions are unary
            if self._is_unary_fct(fct) or self._is_aggregate_fct(fct):
                feasible_paths = self._find_feasible_paths_for_unary_fct(
                    fct, paths_pool
                )
                p_fct = self._get_next_placeholder_name()
                p_path = self._get_next_placeholder_name()
                s = "{{" + p_fct + "}}({{" + p_path + "}})"
                projections.append(s)
                placeholders.update(
                    {p_fct: forced_path_info["fct"], p_path: feasible_paths}
                )

            # Functions are binary infix
            elif self._is_binary_fct_infix(fct):
                (
                    feasible_paths_lhs,
                    feasible_paths_rhs,
                ) = self._find_feasible_paths_for_binary_fct(fct, paths_pool)
                p_lhs = self._get_next_placeholder_name()
                p_fct = self._get_next_placeholder_name()
                p_rhs = self._get_next_placeholder_name()
                s = "{{" + p_lhs + "}}" + "{{" + p_fct + "}}" + "{{" + p_rhs + "}}"
                projections.append(s)
                placeholders.update(
                    {
                        p_lhs: feasible_paths_lhs,
                        p_fct: forced_path_info["fct"],
                        p_rhs: feasible_paths_rhs,
                    }
                )

            # Functions are binary prefix
            elif self._is_binary_fct_prefix(fct):
                (
                    feasible_paths_lhs,
                    feasible_paths_rhs,
                ) = self._find_feasible_paths_for_binary_fct(fct, paths_pool)
                p_lhs = self._get_next_placeholder_name()
                p_fct = self._get_next_placeholder_name()
                p_rhs = self._get_next_placeholder_name()
                s = "{{" + p_fct + "}}" + "({{" + p_lhs + "}}," + "{{" + p_rhs + "}})"
                projections.append(s)
                placeholders.update(
                    {
                        p_lhs: feasible_paths_lhs,
                        p_fct: forced_path_info["fct"],
                        p_rhs: feasible_paths_rhs,
                    }
                )
            else:
                raise RuntimeError(
                    "Unknown fct in fct info {}".format(forced_path_info)
                )

        # Single function specified: Function is fixed, paths are a placeholder
        elif type(forced_path_info["fct"]) is str:
            fct = forced_path_info["fct"]
            if self._is_unary_fct(fct) or self._is_aggregate_fct(fct):
                feasible_paths = self._find_feasible_paths_for_unary_fct(
                    fct, paths_pool
                )
                p_name = self._get_next_placeholder_name()
                s = fct + "({{" + p_name + "}})"
                projections.append(s)
                placeholders.update({p_name: feasible_paths})
            elif self._is_binary_fct_infix(fct):
                (
                    feasible_paths_lhs,
                    feasible_paths_rhs,
                ) = self._find_feasible_paths_for_binary_fct(fct, paths_pool)
                p_lhs = self._get_next_placeholder_name()
                p_rhs = self._get_next_placeholder_name()
                s = "{{" + p_lhs + "}}" + fct + "{{" + p_rhs + "}}"
                projections.append(s)
                placeholders.update(
                    {p_lhs: feasible_paths_lhs, p_rhs: feasible_paths_rhs}
                )
            elif self._is_binary_fct_prefix(fct):
                (
                    feasible_paths_lhs,
                    feasible_paths_rhs,
                ) = self._find_feasible_paths_for_binary_fct(fct, paths_pool)
                p_lhs = self._get_next_placeholder_name()
                p_rhs = self._get_next_placeholder_name()
                s = fct + "({{" + p_lhs + "}}," + "{{" + p_rhs + "}})"
                projections.append(s)
                placeholders.update(
                    {p_lhs: feasible_paths_lhs, p_rhs: feasible_paths_rhs}
                )
            else:
                raise RuntimeError(
                    "Unknown fct in fct info {}".format(forced_path_info)
                )
        else:
            raise RuntimeError(
                "Forced path fct value has invalid type {}".format(forced_path_info)
            )

    def _extend_substitute_list(self, array, desired_length):
        """
        Extend the array with items from the original array
        """
        original_length = len(array)
        while len(array) < desired_length:
            array.append(random.choice(array[:original_length]))

        return array

    def _find_feasible_paths_for_unary_fct(self, f, paths_pool):
        feasible_objs = [
            p
            for p in paths_pool
            if self._is_feasible_unary(f, self.config_to_matrix_type(p))
        ]
        feasible_paths = [
            self._create_path_expr(self._cfg["collection"], x["path"])
            for x in feasible_objs
        ]
        return feasible_paths

    def _find_feasible_paths_for_binary_fct(self, f, paths_pool):
        product = [x for x in itertools.product(paths_pool, paths_pool)]
        feasible_objs = [
            [x, y]
            for (x, y) in product
            if self._is_feasible_binary(
                f, self.config_to_matrix_type(x), self.config_to_matrix_type(y)
            )
        ]
        feasible_paths_lhs = [
            self._create_path_expr(self._cfg["collection"], x["path"])
            for (x, y) in feasible_objs
        ]
        feasible_paths_rhs = [
            self._create_path_expr(self._cfg["collection"], y["path"])
            for (x, y) in feasible_objs
        ]

        return feasible_paths_lhs, feasible_paths_rhs

    def _find_feasible_unary_functions(self, t):
        return [f for f in UNARY_FUNCTIONS if self._is_feasible_unary(f, t)]

    def _find_feasible_binary_infix_functions(self, lhs_t, rhs_t):
        return [
            f
            for f in BINARY_FUNCTIONS_INFIX
            if self._is_feasible_binary(f, lhs_t, rhs_t)
        ]

    def _find_feasible_binary_prefix_functions(self, lhs_t, rhs_t):
        return [
            f
            for f in BINARY_FUNCTIONS_PREFIX
            if self._is_feasible_binary(f, lhs_t, rhs_t)
        ]

    def _is_feasible_unary(self, unary_fct, t):
        return self._feasibility_matrix[unary_fct.lower()][t] == "SUCCESS"

    def _is_feasible_binary(self, binary_fct, lhs_t, rhs_t):
        return self._feasibility_matrix[binary_fct.lower()][lhs_t][rhs_t] == "SUCCESS"

    def _is_unary_fct(self, name):
        return name.upper() in UNARY_FUNCTIONS

    def _is_aggregate_fct(self, name):
        return name.upper() in AGGREGATE_FUNCTIONS

    def _is_binary_fct_infix(self, name):
        return name.upper() in BINARY_FUNCTIONS_INFIX

    def _is_binary_fct_prefix(self, name):
        return name.upper() in BINARY_FUNCTIONS_PREFIX

    def _get_next_placeholder_name(self):
        placeholder_name = "p{}".format(self._placeholder_count)
        self._placeholder_count += 1
        return placeholder_name

    def _get_forced_path_obj(self, path_list, pool):
        """
        Finds the object from the schema config describing a forced path by the list of it's path names
        """
        for x in pool:
            if x["path"] == path_list:
                return x
        raise RuntimeError(
            "Path list {} specified as forced path in config is not in schema config {}".format(
                path_list, pool
            )
        )

    def _create_path_expr(self, collection, path):
        """
        Creates a SQL path expression, eg "a"."b"."c"

        Args:
            collection: Name of the collection
            path: List of field names
        Returns:
            String path expression
        """
        s = '"{}"'.format(collection)
        for p in path:
            s += '."{}"'.format(self.remove_text(p, "[", "]"))
        return s

    def _get_array_nesting_depth(self, path_info):
        """
        Extract information about array from path info object
        """
        assert self.config_to_matrix_type(path_info) == "array"
        depth = 0
        for p in path_info["path"]:
            if "[" in p:
                depth += 1

        return depth

    def config_to_matrix_type(self, path_info):
        """
        Converts the path info object from the schema cfg forced path to
        to the string used in the feasibility matrix to represent a type
        """
        for p in path_info["path"]:
            if "[" in p:
                return "array"

        s = path_info["valueType"]
        if "random_number" in s:
            return "number"
        # elif 'word' in s or 'name' in s:
        else:
            return "string"
            # raise RuntimeError('Unknown value type: {}'.format(s))

    def schema_cfg_op_to_str(self, op):
        """
        Converts the name of the operators used in the schema config to the actual symbol
        """
        if op == "eq":
            return "="
        else:
            raise RuntimeError("Unknown operator {}".format(op))

    def randint_from_range(self, range, upper_bound=None):
        """
        Args:
            range: List of two elements. Min at [0] and max at [1]
            upper_bound: Upper bound for the maximum value
        Returns:
            If upper_bound is passed and less than range[1]:
                A random integer between range[0], upper_bound inclusive
            Else:
                A random int between range[0], range[1] inclusive
        """
        if upper_bound == 0 or range[1] == 0:
            return 0

        min = range[0]
        max = (
            upper_bound
            if upper_bound is not None and upper_bound < range[1]
            else range[1]
        )
        if max < min:
            min = 1
        return random.randint(min, max)

    def insert_config_default_values(self):
        if "random" in self._cfg["projection"]:
            cfg_rnd = self._cfg["projection"]["random"]
            if "number_unary_fct" not in cfg_rnd:
                cfg_rnd["number_unary_fct"] = [0, 0]
            if "number_binary_fct" not in cfg_rnd:
                cfg_rnd["number_binary_fct"] = [0, 0]
            if "number_aggregate_fct" not in cfg_rnd:
                cfg_rnd["number_aggregate_fct"] = [0, 0]

        if "where_clause" in self._cfg:
            cfg_where = self._cfg["where_clause"]
            if "operators" not in cfg_where:
                cfg_where["operators"] = ["AND", "OR"]
            if "probability" not in cfg_where:
                cfg_where["probability"] = 1
            if "random" not in cfg_where:
                cfg_where["random"] = {"number_total": [0, 0]}

        if "limit" not in self._cfg:
            self._cfg["limit"] = None

    def validate_config(self):
        assert type(self._cfg["collection"]) is str
        assert (
            type(self._cfg["number_of_different_queries"]) is int
            and self._cfg["number_of_different_queries"] > 0
        )
        assert (
            type(self._cfg["combinations_per_query"]) is int
            and self._cfg["combinations_per_query"] >= 1
        ) or (
            type(self._cfg["combinations_per_query"]) is str
            and self._cfg["combinations_per_query"] == "all"
        )
        assert self._has_forced_with_at_least_one_item_or_random(
            self._cfg["projection"]
        )

        if "forced" in self._cfg["projection"]:
            forced_cfg = self._cfg["projection"]["forced"]
            assert type(forced_cfg) is list
            for path_info in forced_cfg:
                if "fct" in path_info:
                    if type(path_info["fct"]) is str:
                        assert self._is_valid_fct(
                            path_info["fct"]
                        ), "Unknown function: {}".format(path_info)
                    if type(path_info["fct"]) is list:
                        assert all(
                            [self._is_valid_fct(f) for f in path_info["fct"]]
                        ), "Unknown function: {}".format(path_info)

        if "random" in self._cfg["projection"]:
            random_cfg = self._cfg["projection"]["random"]
            range_err_msg = '"projection"."random"."{}" must be a list of two integers with the first one not being greater than the second one'
            assert self._is_a_range(random_cfg["number_total"]), range_err_msg.format(
                "number_total"
            )
            assert self._is_a_range(
                random_cfg["number_unary_fct"]
            ), range_err_msg.format("number_unary_fct")
            assert self._is_a_range(
                random_cfg["number_binary_fct"]
            ), range_err_msg.format("number_binary_fct")
            assert self._is_a_range(
                random_cfg["number_aggregate_fct"]
            ), range_err_msg.format("number_aggregate_fct")

        if "where_clause" in self._cfg:
            assert "probability" in self._cfg["where_clause"]
            assert self._is_probability(self._cfg["where_clause"]["probability"])
            assert (
                "operators" in self._cfg["where_clause"]
                and len(self._cfg["where_clause"]["operators"]) >= 1
            )
            assert all(
                [op in ["AND", "OR"] for op in self._cfg["where_clause"]["operators"]]
            )
            assert self._has_forced_with_at_least_one_item_or_random(
                self._cfg["where_clause"]
            )
            if "random" in self._cfg["where_clause"]:
                assert self._is_a_range(
                    self._cfg["where_clause"]["random"]["number_total"]
                )

    def _is_probability(self, x):
        return x >= 0 and x <= 1

    def _has_forced_with_at_least_one_item_or_random(self, obj):
        return ("forced" in obj and len(obj["forced"]) > 0) or "random" in obj

    def _is_a_range(self, x):
        if type(x) is not list:
            return False
        if len(x) != 2:
            return False
        if type(x[0]) is not int or type(x[1]) is not int:
            return False
        if x[0] > x[1]:
            return False
        return True

    def _is_valid_fct(self, f):
        x = []
        x.extend(UNARY_FUNCTIONS)
        x.extend(BINARY_FUNCTIONS_INFIX)
        x.extend(BINARY_FUNCTIONS_PREFIX)
        x.extend(AGGREGATE_FUNCTIONS)
        return f in x

    def remove_text(self, s, open="(", close=")"):
        """
        Remove all text inside the open and close symbol from a string, eg.
        (:1)*(:2) -> *
        """
        ret = ""
        i = 0
        while i < len(s):
            if s[i] == open:
                while s[i] != close:
                    i += 1
                i += 1
                if not i < len(s):
                    break
            ret += s[i]
            i += 1
        return ret

    def load_feasibility_matrix(self):
        matrix = fsb.FEASIBILITY_MATRIX
        new_matrix = {}
        for k in matrix.keys():
            new_k = self.remove_text(k).lower()
            new_matrix[new_k] = matrix[k]
        return new_matrix


class StandaloneGenerator:
    """
    Takes a standalone config, instantiates the template and outputs the queries
    """

    def __init__(
        self, config, output_dir, query_base_name, do_print=True, do_print_only=False
    ):
        """
        Args:
            config: Standalone config
            output_dir: Abs path where to create the queries
            query_base_name: Base name for the generated queries, eg 'query' -> 'query_0.sql', 'query_1.sql', ...
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print('Created output directory "{}"'.format(output_dir))

        self._template, self._placeholder_dict, self._combinations = self._load_config(
            config
        )
        self._output_dir = output_dir
        self._query_base_name = query_base_name
        self._do_print = do_print or do_print_only
        self._do_print = False
        self._do_output = not do_print_only

    def _instantiate_and_output_queries(
        self, template, substitute_data, file_name_base
    ):
        for i, data in enumerate(substitute_data):
            output_file_name = file_name_base + "{}.sql".format(i)
            output_file_path = os.path.join(self._output_dir, output_file_name)
            jinja = JinjaSql()
            query, bind_params = jinja.prepare_query(template, data)
            query = query % tuple(bind_params)
            if self._do_print:
                print("{} :\n{}\n\n".format(output_file_name, query))
            if self._do_output:
                with open(output_file_path, "w", encoding="utf8") as output_file:
                    output_file.write(query)
                    output_file.flush()

    def run(self):
        (
            self._template,
            placeholders,
            placeholders_original,
        ) = self._make_placeholders_unique(self._template, self._placeholder_dict)

        if self._combinations == "all":
            data = self._create_cartesian_product(
                placeholders, placeholders_original, self._placeholder_dict
            )
        else:
            data = self._create_n_random_combinations(
                placeholders,
                placeholders_original,
                self._placeholder_dict,
                int(self._combinations),
            )

        self._instantiate_and_output_queries(
            self._template, data, self._query_base_name
        )

    def _load_config(self, config):
        if "template" not in config:
            raise RuntimeError('Key "template" is missing in standalone config')
        if "combinations" not in config:
            raise RuntimeError('Key "combinations" is missing in standalone config')
        placeholder_dict = copy.deepcopy(STANDARD_PLACEHOLDERS)
        if "placeholders" in config:
            placeholder_dict.update(config["placeholders"])
        return config["template"], placeholder_dict, config["combinations"]

    def _create_cartesian_product(
        self, placeholders, placeholders_original, placeholder_dict
    ):
        """
        Creates all possible combinations for a given template
        """
        lists = [placeholder_dict[x] for x in placeholders_original]
        cartesian_product = [x for x in itertools.product(*lists)]
        return [
            {p: v for (p, v) in zip(placeholders, combination)}
            for combination in cartesian_product
        ]

    def _create_n_random_combinations(
        self, placeholders, placeholders_original, placeholder_dict, N
    ):
        """
        Create N random combinations for a given template
        """
        all_combinations = self._create_cartesian_product(
            placeholders, placeholders_original, placeholder_dict
        )
        return (
            random.sample(all_combinations, N)
            if N < len(all_combinations)
            else all_combinations
        )

    def _make_placeholders_unique(self, template, placeholder_dict):
        """
        Make the used placeholders distinct so each can be substituted on it's own
        Args:
            template: The query template
        Returns:
            A tuple containing: template, placeholders, placeholders_original

        Given a template: SELECT {{AGGREGATE_FCT}}(x1), {{AGGREGATE_FCT}}(x2) FROM y
        Creates a template: SELECT {{AGGREGATE_FCT_0}}(x1), {{AGGREGATE_FCT_1}}(x2) FROM y
        Otherwise jinja will replace both placeholders with the same value, not allowing distinct combinations

        Returns a modified template string, a list containing updated placeholder names, a list containing the original placeholder names, eg
        SELECT {{AGGREGATE_FCT_0}}(x1),
        {{AGGREGATE_FCT_1}}(x2) FROM y, ['AGGREGATE_FCT_0', 'AGGREGATE_FCT_1'],
        ['AGGREGATE_FCT', 'AGGREGATE_FCT']
        """

        def nth_repl(s, sub, repl, n):
            """
            https://stackoverflow.com/questions/35091557/replace-nth-occurrence-of-substring-in-string
            """
            find = s.find(sub)
            # If find is not -1 we have found at least one match for the substring
            i = find != -1
            # loop util we find the nth or we find no match
            while find != -1 and i != n:
                # find + 1 means we start searching from after the last match
                find = s.find(sub, find + 1)
                i += 1
            # If i is equal to n we found nth match so replace
            if i == n:
                return s[:find] + repl + s[find + len(sub) :]
            return s

        original_placeholders = re.findall(
            r"""(?<={{)(""" + "|".join(placeholder_dict.keys()) + r""")(?=}})""",
            template,
        )
        placeholder_counts = {p: 0 for p in list(set(original_placeholders))}
        new_placeholders = []

        for p in original_placeholders:
            repl = p + "_{}".format(placeholder_counts[p])
            template = nth_repl(template, p, repl, placeholder_counts[p] + 1)
            new_placeholders.append(repl)
            placeholder_counts[p] += 1

        return template, new_placeholders, original_placeholders


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="Path to the config file", required=True)
    parser.add_argument(
        "--output",
        "-o",
        help='Path to the output directory for the generated query files. Defaults to "./"',
        default="./",
    )
    parser.add_argument(
        "--mode", help="Generator mode", choices=["standalone", "schema"]
    )
    parser.add_argument(
        "--schema-config",
        help='Path to the schema config file. Only in mode "schema"',
        default=None,
    )
    parser.add_argument(
        "--query-base-name",
        help='Base name of the generated queries. Defaults to "query"',
        default="query",
    )
    parser.add_argument(
        "--print", help="Print generated queries to console", action="store_true"
    )
    parser.add_argument(
        "--print-only",
        help="Only print the queries. Do not create output files",
        action="store_true",
    )

    args = parser.parse_args()

    if args.print and args.print_only:
        raise RuntimeError("Cannot specify print and print only together")

    if args.schema_config is not None:
        args.schema_config = os.path.abspath(args.schema_config)

    if args.mode == "standalone":
        with open(os.path.abspath(args.config), encoding="utf8") as cfg_file:
            sa_cfg = json.load(cfg_file)
            g = StandaloneGenerator(
                sa_cfg,
                os.path.abspath(args.output),
                args.query_base_name,
                args.print,
                args.print_only,
            )
            g.run()
    else:
        with open(os.path.abspath(args.config), encoding="utf8") as cfg_file:
            with open(
                os.path.abspath(args.schema_config), encoding="utf8"
            ) as schema_cfg_file:
                cfg = json.load(cfg_file)
                schema_cfg = json.load(schema_cfg_file)
                x = SchemaBasedGenerator(cfg, schema_cfg)
                sa_cfgs = x.run()
                for i, c in enumerate(sa_cfgs):
                    g = StandaloneGenerator(
                        c,
                        os.path.abspath(args.output),
                        "{}_{}".format(args.query_base_name, i),
                        args.print,
                        args.print_only,
                    )
                    g.run()
