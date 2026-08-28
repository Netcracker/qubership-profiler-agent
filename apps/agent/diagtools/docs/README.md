# Overview

The `diagtools` is a CLI tool and uses the Go standard library’s flag package to implement support for subcommands in a program.

Usage of applications with subcommands takes the following form.

command [global options] subcommand [subcommand options] [subcommand arguments]

- `global options` are all subcommands share these
- `subcommand options` are specific to a subcommand
- `subcommand arguments` are non-option arguments specific to a subcommand

Example:

```bash
diagtools --log.level=debug scan /tmp/diagnostic/*.hprof* ./core* ./hs_err*
```

If ticket#[33974](https://github.com/golang/go/issues/33974) (_"golang: make the internal lockedfile package public"_)
is resolved, files `/installer-cloud/main/diagtools/utils/filelock_*.go` are to be deleted
and library ones are to be used instead.

## How to test

1. run java process:

   ```bash
   java TestService.java 10 20 60
   ```

   It will run JVM and  wait for `10s`, `20s` and `60s`, and only after that will exit.

2. run commands manually:

   * `diagtool dump` - upload thread/top dumps for found Java application
   * `diagtool scan *.hprof` - upload generated heap dumps

3. to generate random heap dump:

   ```bash
   dd if=/dev/random of=./generated.hprof bs=100M count=1
   ```
