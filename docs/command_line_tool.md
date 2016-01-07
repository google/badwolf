# Command line tool: bw

`bw` is the main command line tool used to access various functionality.
The `bw` command line tool is built via the `tools/vcli/bw` package.

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

The run command allows you to excecute all the BQL statements contained in a
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
SELECT ?grand_children_name FROM ?family WHERE { /u<joe> "parent_of"@[] ?offspring . ?offspring "parent_of"@[] ?grand_children ID ?grand_children_name };

Result:
?grand_children_name
john
eve

OK

Processing statement (5/5):
DROP GRAPH ?family;

Result:
OK

```
