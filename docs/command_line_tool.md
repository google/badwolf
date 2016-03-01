# Command line tool: bw

`bw` is the main command line tool used to access various functionality.
The `bw` command line tool is built via the `tools/vcli/bw` package. You
can build the tool by just typing

```
$ go test ./... && go build ./tools/vcli/bw/...
```

Is the test pass successfully you will get the `bw` tool ready to go.

## Usage

Once built, you will be able to access the commands available by typing:

```
$ bw help
```

This command will list of available options already. Also, you can always type

```
$ bw help COMMAND
```

To list the help related to the provided command. There are a set of flags
available for the `bw` tool. To list them just type:

```
$ bw -h
```

Keep in mind that all flags should be listed before you enter the command
you want to run.

## Command: Version

The version command prints the version of BadWolf being used. Below you can
find and example of the command output.

```
$ bw version
badwolf vCli (0.2.2-dev)
```

## Command: Run

The `run` command allows you to run all the BQL statements contained in a
given file. All lines in the file starting with # will be treated as comments
and will be discarded. An example of a file containing a set of executable
statements can be found at
[examples/bql/example_0.bql](../examples/bql/example_0.bql).
Below you can find the output of using the `run` command against the previously
mentioned.

```
$ bw run examples/bql/example_0.bql
Processing file examples/bql/example_0.bql

Processing statement (1/5):
CREATE GRAPH ?family;

Result:
OK

Processing statement (2/5):
INSERT DATA INTO ?family { /u<joe> "parent_of"@[] /u<mary> . /u<joe> "parent_of"@[] /u<peter> . /u<peter> "parent_of"@[] /u<john> . /u<peter> "parent_of"@[] /u<eve> };

Result:
OK

Processing statement (3/5):
SELECT ?name FROM ?family WHERE { /u<joe> "parent_of"@[] ?offspring ID ?name };

Result:
?name
mary
peter

OK

Processing statement (4/5):
SELECT ?grandchildren_name FROM ?family WHERE { /u<joe> "parent_of"@[] ?offspring . ?offspring "parent_of"@[] ?grandchildren ID ?grandchildren_name };

Result:
?grandchildren_name
john
eve

OK

Processing statement (5/5):
DROP GRAPH ?family;

Result:
OK
```

## Command: Assert

The `assert` command allows you to run all the stories contained in a given
folder. Stories are serialized as JSON files. Each story contains:

1. One or more sources. A source is a graph defined by the triples it contain.
2. One or more assertions. An assertion is a BQL query and the expected outcome.

An example of a simple story with only one assertion could be:

```
  "Name": "Family tree",
  "Sources": [
    {
      "ID": "?family",
      "Facts": [
        "/u<joe> \"parent_of\"@[] /u<mary>",
        "/u<joe> \"parent_of\"@[] /u<peter>",
        "/u<peter> \"parent_of\"@[] /u<john>",
        "/u<peter> \"parent_of\"@[] /u<eve>"
      ]
    }
  ],
  "Assertions": [
    {
      "Requires": "finding all Joe's offspring name",
      "Statement": "
      SELECT ?name
      FROM ?family
      WHERE {
        /u<joe> \"parent_of\"@[] ?offspring ID ?name
      }
      ORDER BY ?name;",
      "WillFail": false,
      "MustReturn": [
        {
          "?name": "mary"
        },
        {
          "?name": "peter"
        }
      ]
    }
  ]
}

```

The `assert` command will collect all story files and run every one of them
collecting the outcome and evaluating of each the assertion. Stories are heavily
used to validate BQL semantic behavior. All compliance stories can be found at
[examples/compliance](../examples/compiance). Compliance stories guarantee that
all backend storage drivers that implement `storage.Store` and `storage.Graph`
provide the exact same behavior. If a driver does not pass the compliance tests
in the aforementioned folder, it will be an indication of a serious bug and
should not be used since may lead to wrong results.

Below you can find the output of using the `assert` command against the
compliance folder that guarantees BQL returns the expected results.

```
$ bw assert examples/compliance
-------------------------------------------------------------
Processing folder "examples/compliance"...
	Processing file "cmpl_bql_example_1.json"... done.
	Processing file "cmpl_bql_example_3.json"... done.
	Processing file "cmpl_bql_minimal.json"... done.
	Processing file "cmpl_bql_example_0.json"... done.
	Processing file "cmpl_bql_example_2.json"... done.
	Processing file "cmpl_bql_graph_clauses.json"... done.
-------------------------------------------------------------
Evaluating 6 stories... done.
-------------------------------------------------------------
(1/6) Story "Family graph data example 1"...
	requires finding how many female grandchildren does Joe have [Assertion=TRUE]
	requires finding all male grandchildren does Joe have [Assertion=TRUE]
	requires finding the gender distribution of Joe's mammal grandchildren in our family graph? [Assertion=TRUE]
	requires finding how many offsprings Joe's has [Assertion=TRUE]
	requires finding all Joe's offspring names [Assertion=TRUE]
	requires finding all different mammal we know in the family graph [Assertion=TRUE]
	requires finding all genders of the members in the family graph [Assertion=TRUE]
	requires finding all the gender distribution in our family graph [Assertion=TRUE]

(2/6) Story "Family and car graph data example 4"...
	requires finding if any of Joe's grandchildren have the same name of his parent [Assertion=TRUE]
	requires finding who are Joe's grandchildren that do *not* have the same name of his parent [Assertion=TRUE]

(3/6) Story "A simple object manipulation"...
	requires retrieving the type [Assertion=TRUE]
	requires retrieving the id [Assertion=TRUE]
	requires retrieving the object [Assertion=TRUE]

(4/6) Story "Family graph data example 0"...
	requires finding all Joe's offspring name [Assertion=TRUE]
	requires finding all Joe's grandchildren [Assertion=TRUE]

(5/6) Story "Family and car graph data example 2"...
	requires finding how many grandchildren does Joe have [Assertion=TRUE]
	requires finding the different brands of car manufactures do we know [Assertion=TRUE]
	requires finding what cars does Joe's grandchildren drive in descending order? [Assertion=TRUE]
	requires finding any unique owner and manufacture, list the manufacture in descending order, and for each manufacture order the owners in ascending order [Assertion=TRUE]
	requires finding the manufactures in descending order by number of owners [Assertion=TRUE]

(6/6) Story "Family graph clauses traversal"...
	requires Joe has two children despite redundant clause [Assertion=TRUE]
	requires Joe's grandchildren combinations to be returned only if both do not have the same name [Assertion=TRUE]
	requires counting XX two as the number of grand children Joe has [Assertion=TRUE]
	requires Joe has two kids and one is Mary for sure [Assertion=TRUE]
	requires Joe has no two children and bot are children of Mary [Assertion=TRUE]
	requires returning the first Joe's grandchildren of the possible combinations [Assertion=TRUE]
	requires listing all the possible four combination of Joe's children and grandchildren only if the children has kids [Assertion=TRUE]
	requires Joe has at least two children and their names are Eve and John [Assertion=TRUE]
	requires Joe has at least two children and their names are Eve and John and both share the same parent [Assertion=TRUE]
	requires Joe's grandchildren should product four combinations [Assertion=TRUE]
	requires Joe's grandchildren combinations to be returned only if both have the same name [Assertion=TRUE]
	requires listing Joe as the only grandparent [Assertion=TRUE]
	requires no Joe's children gets returned if he does not have one called Zoe [Assertion=TRUE]
	requires Joe has at least two children and their names are Eve and John and both are children of Peter [Assertion=TRUE]
	requires Joe to have two grandchildren [Assertion=TRUE]
	requires four possible combinations of Joe's children [Assertion=TRUE]
	requires Joe's grandchildren combinations to be returned only if the first name is less than the second one [Assertion=TRUE]
	requires listing all the possible four combination of Joe's children and grandchildren [Assertion=TRUE]

-------------------------------------------------------------

done
```

If any of the assertions of a story fails, it will be proper indicated and the
obtained result table and the expected one will both be displayed.
