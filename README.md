# A collection of customized processors for Apache Nifi

## Installation

`mvn clean install` and copy the `nifi-dataminded-processors-nar-1.0-SNAPSHOT.nar` to the `lib` folder of your Nifi installation.

`mvn -DpushChanges=false -DlocalCheckout=true -DpreparationGoals=initialize release:prepare release:perform` to create a release

## Processors
### SimpleTriggeredProcessExecutor
Modified version of `ExecuteProcess`. This processor allows input and you can use expression language on the parameters. The attributes from the incoming FlowFile can be used as arguments of the external tool that you want to call

### ExecuteOracleSQL
Modified version of `ExecuteSQL`. This processor handles Oracle's `NUMBER` type. It will make an educated guess if the content of a `NUMBER` field is either an `Integer`, `Long`, `Float` or `Double` instead of converting it to `String`

### GenerateOracleTableFetch
Modified version of `GenerateTableFetch`. Will generate queries based on the given `SPLIT_COLUMN` rather than on `ROW_NUM`. This is the same approach that Apache Sqoop uses to generate it's queries. The number of partitions is also configurable.

Some attribute fields are added as well to provide more information of the table that you are reading. These are optional.

In end the processor will generate queries like this:
``` sql
SELECT * FROM MYTABLE WHERE ID BETWEEN 5295378 AND 10590758
```

