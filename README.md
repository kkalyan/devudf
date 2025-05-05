# DevUDF - Collection of Apache Pig UDFs

This project provides a collection of User Defined Functions (UDFs) for Apache Pig that simplify common data processing tasks.

## Overview

DevUDF contains several UDFs that extend Apache Pig's functionality for specific use cases:

* Data sampling
* JSON size analysis
* IP range expansion
* Distributed lookup operations
* Base64 encoding

## Requirements

* Java 8+
* Apache Pig 0.14.0+
* Apache Hadoop 2.6.0+ (for distributed cache functionality)

## Installation

1. Clone this repository
2. Build using Maven:

```bash
mvn clean package
```

3. The packaged JAR will be available at `target/devudf-1.0.jar`

## Register the JAR in Pig

```pig
REGISTER /path/to/devudf-1.0.jar;
```

## Available UDFs

### LimitN

An Algebraic UDF that takes a BAG generated from `GROUP BY` and returns N records.

#### Usage

```pig
sample_data = FOREACH grouped GENERATE group AS key, LimitN(data.val1, 10);
```

The above example limits the results to 10 records. The default limit is 5 if no value is provided.

### SizeStats

Given a JSON input file and a specification to group fields, this UDF returns a tuple of sizes on disk.

#### Usage

```pig
size_data = FOREACH data GENERATE SizeStats(json_string, 'group1=field1,field2;group2=field3', 'include_field1,include_field2');
```

### ExpandIpRanges

Generates a bag of IPs from a start IP address to an end IP address.

#### Usage

```pig
ip_bag = FOREACH data GENERATE ExpandIpRanges(start_ip, end_ip);
```

### DistributedLookup

Implementation of VLOOKUP-like functionality using Hadoop's Distributed Cache. This UDF loads lookup data from a file and performs efficient lookups.

#### Usage

```pig
result = FOREACH data GENERATE DistributedLookup('/path/to/lookup.tsv', '\t', '0', '1', key);
```

Parameters:
- Path to the lookup file
- Field separator
- Index of the lookup key column (0-based)
- Index of the output column (0-based)
- Lookup key

### Base64UDF

Converts a byte array to a Base64 encoded string. Useful for storing sketches for loading into Druid.

#### Usage

```pig
base64_data = FOREACH data GENERATE Base64UDF(bytearray_field);
```

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 
