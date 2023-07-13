import json as jsonModule


# to be pasted as a test into testMisc.py
def testTypes(self):
    UNARY_FUNCTIONS = [
        "ABS(:1)",
        "ACOS(:1)",
        "ASIN(:1)",
        "ATAN(:1)",
        "COS(:1)",
        "LN(:1)",
        "SIN(:1)",
        "TAN(:1)",
        "LENGTH(:1)",
        "LOWER(:1)",
        "UPPER(:1)",
        "TO_BIGINT(:1)",
        "TO_DOUBLE(:1)",
        "TO_VARCHAR(:1)",
    ]

    BINARY_FUNCTIONS = [
        "ATAN2(:1, :2)",
        "MOD(:1, :2)",
        "POWER(:1, :2)",
        "ROUND(:1, :2)",
        "CONCAT(:1, :2)",
        "(:1)+(:2)",
        "(:1)-(:2)",
        "(:1)*(:2)",
        "(:1)/(:2)",
        "LOG(:1, :2)",
    ]

    unaryFunctions = UNARY_FUNCTIONS
    binaryFunctions = BINARY_FUNCTIONS
    jsonTypes = ["object", "number", "array", "null", "boolean", "string"]
    keyDict = {
        "number": {"key": "key05", "value": 0.1},
        "string": {"key": "key04", "value": """\"a string\""""},
        "null": {"key": "key03", "value": "null"},
        "boolean": {"key": "key06", "value": "true"},
        "array": {"key": "key02", "value": "[1,2,3,4]"},
        "object": {"key": "key12", "value": '{ "key1" : [1,2,3,4]}'},
    }
    resultsDict = {}
    for fct in unaryFunctions:
        resultsDict[fct] = {}
    for fct in binaryFunctions:
        resultsDict[fct] = {}
        for jsonType in jsonTypes:
            resultsDict[fct][jsonType] = {}
    jsons = [
        """{ "key12" : { "key1" : [1,2,3,4]}}""",
        """{ "key02" : [1,2,3,4] }""",
        """{ "key03" : null}""",
        """{ "key04" : "a string"}""",
        """{ "key05" : 0.1}""",
        """{ "key06" : true}""",
    ]
    cursor = self.conn.cursor()
    name = self.getName()
    self.dropCollection(name, cursor)
    self.createCollection(name, cursor)
    for json in jsons:
        try:
            cursor.execute("""insert into %s values('%s')""" % (name, json))
            self.conn.commit()
        except:
            raise (ValueError("insert failed : " + json))

    for jsonType in jsonTypes:
        print()
        failedFunctions = []
        for fctname in unaryFunctions:
            print("######### %s , %s #########" % (fct, jsonType))
            key1 = '"' + keyDict[jsonType]["key"] + '"'
            val1 = keyDict[jsonType]["value"]
            fct = fctname.replace(":1", "%s")
            try:
                cursor.execute(
                    """select * from %s WHERE %s=%s"""
                    % (name, fct % (key1), fct % (val1))
                )
                statement = """select * from %s WHERE %s=%s""" % (
                    name,
                    fct % (key1),
                    fct % (val1),
                )
                print(statement)
                res = cursor.fetchall()
                print("######### result of select *: #########")
                res.sort()
                for r in res:
                    print(r)
                print("#########")
                result = "SUCCESS"
            except Exception as e:
                print(e)
                failedFunctions.append([fctname, jsonType])
                #                    msg = e.message
                #                    msg.replace("(257, \'sql syntax error:", "")
                #                    msg.replace(
                #                        "(129, 'transaction rolled back by an internal error:", "")
                result = "FAIL"

            resultsDict[fctname][jsonType] = result

    for jsonType1 in jsonTypes:
        for jsonType2 in jsonTypes:
            for fctname in binaryFunctions:
                print("######### %s #########" % fctname)
                key1 = '"' + keyDict[jsonType1]["key"] + '"'
                key2 = '"' + keyDict[jsonType2]["key"] + '"'
                val1 = keyDict[jsonType1]["value"]
                val2 = keyDict[jsonType2]["value"]
                fct = fctname.replace(":1", "%s").replace(":2", "%s")

                try:
                    cursor.execute(
                        """select * from %s WHERE %s=%s"""
                        % (name, fct % (key1, key2), fct % (val1, val2))
                    )
                    res = cursor.fetchall()
                    print("######### result of select *: #########")
                    res.sort()
                    for r in res:
                        print(r)
                    print("#########")
                    result = "SUCCESS"
                except Exception as e:
                    #                        msg = e.message
                    #                        msg.replace("(257, \'sql syntax error:", "")
                    #                        msg.replace(
                    #                            "(129, 'transaction rolled back by an internal error:", "")
                    #                        print msg
                    failedFunctions.append([fctname, jsonType1, jsonType2])
                    result = "FAIL"

                resultsDict[fctname][jsonType1][jsonType2] = result

    print("########## resultDict   ###########")
    print(resultsDict)
    print("########## /resultDict   ###########")
    with open("/data/feasibilityMatrix.json", "w+") as fp:
        jsonModule.dump(resultsDict, fp, sort_keys=True, indent=4)
    print("######### FAILED FCTS : #########")
    for r in failedFunctions:
        print(r)
    print("#########")

    with open("/data/failedFunctions.json", "w+") as fp:
        jsonModule.dump({"failed": failedFunctions}, fp, sort_keys=True, indent=4)
    self.dropCollection(name, cursor)
