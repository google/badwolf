# Command line tool: bw

`bw` is the main command line tool used to access various functionality.
The `bw` command line tool is built via the `tools/vcli/bw` package. You
can build the tool by just typing

```
$ got test ./... && go build ./tools/vcli/bw/...
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

To list the help related to the provided command.

## Command: Version

The version command prints the version of BadWolf being used. Below you can
find and example of the command output.

```
$ bw version
badwolf vCli (alpha-0.1.dev)
```

## Command: Run

The run command allows you to run all the BQL statements contained in a
given file. All lines in the file starting with # will be treated as comments
and will be discarded. An example of a file containing a set of executable
statements can be found at
[examples/bql/example_0.bql](../examples/bql/example_0.bql).
Below you can find the output of using the `run` command against the previously mentioned.

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

The assert command will bulk-assert all the stories contained in the provided
stories folder. A compliance story is a JSON file that contains the data
to be used (one or more graphs), and the list of assertions (BQL query +
output table) to be validated against the provided data. An example of a file containing a simple story can be found at
[examples/compliance/cmpl_simple.json](../examples/compliance/cmpl_simple.json).
Below you can find the output of using the `assert` command against a folder
containing multiple stories.

```
$ bw assert examples/compliance
Processing folder "examples/compliance"...

-------------------------------------------------------------
Processing file "cmpl_bql_example_1.json"...

Family graph data requires finding all genders of the members in the family graph [TRUE]
Family graph data requires finding all the gender distribution in our family graph [TRUE]
Family graph data requires finding how many female grandchildren does Joe have [TRUE]
Family graph data requires finding all male grandchildren does Joe have [TRUE]
Family graph data requires finding the gender distribution of Joe's mammal grandchildren in our family graph? [TRUE]
Family graph data requires finding all Joe's offspring name [TRUE]
Family graph data requires finding all different mammal we know in the family graph [TRUE]
-------------------------------------------------------------
Processing file "cmpl_bql_example_3.json"...

Family and car graph data requires finding if any of Joe's grandchildren have the same name of his parent [TRUE]
Family and car graph data requires finding who are Joe's grandchildren that do not have the same name of his parent [TRUE]
-------------------------------------------------------------
Processing file "cmpl_bql_minimal.json"...

A simple object manipulation requires retrieving the type [TRUE]
A simple object manipulation requires retrieving the id [TRUE]
A simple object manipulation requires retrieving the object [TRUE]
-------------------------------------------------------------
Processing file "cmpl_bql_example_0.json"...

Family graph data requires finding all Joe's offspring name [TRUE]
Family graph data requires finding all Joe's grandchildren [TRUE]
-------------------------------------------------------------
Processing file "cmpl_bql_example_2.json"...

Family and car graph data requires finding how many grandchildren does Joe have [TRUE]
Family and car graph data requires finding the different brands of car manufactures do we know [TRUE]
Family and car graph data requires finding what cars does Joe's grandchildren drive in descending order? [TRUE]
Family and car graph data requires finding any unique owner and manufacture, list the manufacture in descending order, and for each manufacture order the owners in ascending order [TRUE]
Family and car graph data requires finding the manufactures in descending order by number of owners [TRUE]
-------------------------------------------------------------
Processing file "cmpl_bql_graph_clauses.json"...

Family traversal requires Joe has two children despite redundant clause [TRUE]
Family traversal requires four possible combinations of Joe's children [TRUE]
Family traversal requires Joe has at least two children and their names are Eve and John and both share the same parent [TRUE]
Family traversal requires Joe has at least two children and their names are Eve and John and both are children of Peter [TRUE]
Family traversal requires Joe has no two children and bot are children of Mary [TRUE]
Family traversal requires Joe to have two grandchildren [TRUE]
Family traversal requires Joe has two kids and one is Mary for sure [TRUE]
Family traversal requires no Joe's children gets returned if he does not have one called Zoe [TRUE]
Family traversal requires Joe has at least two children and their names are Eve and John [TRUE]
-------------------------------------------------------------

done
```

If any of the assertions of a story fails, it will be proper indicated and the
obtained result table and the expected one will both be displayed.
