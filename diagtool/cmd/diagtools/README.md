
== How to test

1. run java process:

   ```bash
   java TestService.java 10 20 60
   ```

   It will run JVM and  wait for `10s`, `20s` and `60s`, and only after that will exit.

2. run commands manually:

   * `diagtool dump` - upload thread/top dumps for found Java application
   * `diagtool scan *.hprof` - upload generated heap dumps

4. to generate random heap dump:

   ```bash
   dd if=/dev/random of=./generated.hprof bs=100M count=1
   ```

