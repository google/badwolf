# Command line tool: badwolf

`badwolf` is the main command line tool used to access various functionality.
The `badwolf` command line tool is built via the `tools/vcli/badwolf` package.

## Usage

Once built, you will be able to access the commands available by typing:

```
$ badwolf help
```

This command will list of available options already. Also, you can always type

```
$ badwolf help COMMAND
```

To list the help related to the provided command.

## Command: Version

The version command prints the version of BadWolf being used. Below you can
find and example of the command output.

```
$ badwolf version
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
$ badwolf run examples/bql/example_0.bql
Processing file examples/bql/example_0.bql

Processing statement (1/4):
CREATE GRAPH ?g;

Result:
OK

Processing statement (2/4):
INSERT DATA INTO ?g { /u<joe> "parent_of"@[] /u<mary> . /u<joe> "parent_of"@[] /u<peter> . /u<peter> "parent_of"@[] /u<john> . /u<peter> "parent_of"@[] /u<eve> };

Result:
OK

Processing statement (3/4):
SELECT ?offspring, ?grandChildren FROM ?g WHERE { /u<joe> "parent_of"@[] ?offspring . ?offspring "parent_of"@[] ?grandChildren };

Result:
?offspring	?grandChildren
/u<peter>	/u<john>
/u<peter>	/u<eve>

OK

Processing statement (4/4):
DROP GRAPH ?g;

Result:
OK

```
