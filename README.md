# VariantSpark Readme

## Install

1. Download `variantspark.sh`, `example.conf` and `variantspark-1.0.jar`
4. Ensure `variantspark.sh` is executable `chmod +x variantspark.sh`

## Building From Source

If you have trouble running VariantSpark, you can build it yourself using Maven.

1. Check out the repo
2. `cd VariantSpark/variantspark`
3. `vi pom.xml` and ensure software versions match those on your cluster. 
4. `mvn package` to build.
5. If you built it locally, copy `target/VCF-clusterer-0.0.1-SNAPSHOT.jar` to your cluster.

## Submit a Job

Once installed, use the launcher script, `variantspark.sh` to submit a job to your cluster.
You need to specify a configuration file with `-c`. An example file is available as `example.conf`.
Submit a job using `./variantspark.sh -c example.conf`.

