## Analytics Framework Usage Document

### Introduction

This document is intended to provide a detail information of - how to use the framework (within spark-shell) and various adapters, dispatchers and utilities built into the framework.

Before going into the document go through the framework design document located [here](https://github.com/ekstep/Common-Design/wiki/Analytics-Framework-Design)

### Nomenclature

**spark-home** - Spark home directory

**models** - Directory where the framework and models/data products are stored. Models are classes which contain the actual algorithm

**sc** - Short hand for spark context

***

### Core Package Structures

Following are the core analytics package structures that are needed to be imported to work with the framework.

```scala

/**
 * The following package is the core package for importing the framework code. This package includes the following:
 * 1. High level APIs - DataFetcher, DataFilter and OutputDispatcher
 * 2. Generic models used across the framework and algorithms - Event (for telemetry v1), TelemetryEventV2 (for telemetry v2), MeasuredEvent (for derived events), adapter models and config
 * 3. JobDriver - to execute a job when provided with a config and the data type
 */
import org.ekstep.analytics.framework._

/**
 * This package contains all the utility classes/functions such as:
 * 1. CommonUtil - All common utililities such as creating spark context, date utilities, event utilies, setting s3 and cassandra conf etc.
 * 2. JSONUtils - for json serialization and deserialization. The APIs are generalized to deserialized to any object
 * 3. S3Util - Utilities to operate on S3 data. Upload, fetch keys, query between a date range etc
 * 4. RestUtil - Generalized rest apis - get and post
 */
import org.ekstep.analytics.framework.util._

/**
 * This package contains all the adapters currently implemented in the framework.
 * 1. ContentAdapter - To fetch content data from the learning platform
 * 2. DomainAdapter - To fetch domain data from the learning platform
 * 3. ItemAdapter - To fetch item data from the learning platform
 * 4. UserAdapter - To fetch user profile information from the learner database (MySQL)
 */
import org.ekstep.analytics.framework.adapter._
```

Following are the package structures that are required to be imported to invoke existing algorithms/models


```scala

/**
 * This package contains all the models currently implemented. All the models are required to extend IBatchModel and implement the generic interface `def execute(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[String]`. Following are the models implemented
 * 1. LearnerSessionSummary - To compute the session summaries from raw telemetry of version v1
 * 2. LearnerSessionSummaryV2 - To compute the session summaries from raw telemetry of version v2
 * 3. LearnerActivitySummary - To compute the learner activity summary for the past one week
 * 4. LearnerProficiencySummary - To compute the learner proficiency per concept
 * 5. RecommendationEngine - To compute the learner concept relevance to be used in RE
 */
import org.ekstep.analytics.framework.model._

/**
 * This package contains all the database updaters currently implemented. Database updaters are also implemeted as models so should extend IBatchModel and implement the `execute()` function. Following are the updaters implemented
 * 1. UpdateLearnerActivity - To store the learner activity snapshot into learner db
 * 2. LearnerContentActivitySummary - To store the learner activity per content. Used for RE
 */
import org.ekstep.analytics.framework.updater._

/**
 * This package contains all the jobs - to either run the models or the updaters. Each DP will have a corresponding job associated with it.
 */
import org.ekstep.analytics.framework.job._
```

### Core Framework Concepts

#### Fetch Data/Input

Following are the supported fetch types in the framework abstracted by `DataFetcher`:

1. S3 - Fetch data from S3
2. Local - Fetch data from local file. Using the local file once can fetch data from hdfs too.

Following are the APIs available in the DataFetcher

```scala

// API to fetch data. Fetch the data as an RDD when an explicit type parameter is passed.
val rdd:RDD[T] = DataFetcher.fetchBatchData[T](search: Fetcher);
```

A `Fetcher` object should be passed to the DataFetcher along with the type the data should be serialized to. Following are the example structures of the `Fetcher` object.

```scala

// Example fetcher to fetch data from S3
val s3Fetcher = Fetcher("s3", Option(Query("<bucket>", "<prefix>", "<startDate>", "<endDate>", "<delta>")))

// S3 Fetcher JSON schema
{
	"type": "S3",
	"query": {
    	"bucket": "ekstep-telemetry",
     	"prefix": "telemetry.raw",
    	"startDate": "2015-09-01",
    	"endDate": "2015-09-03"
	}
}

// Example fetcher to fetch data from Local

val localFetcher = Fetcher("local", Option(Query(None, None, None, None, None, None, None, None, None, "/mnt/data/analytics/raw-telemetry-2016-01-01.log.gz")))

// Local Fetcher JSON schema
{
	"type": "local",
	"query": {
    	"file": "/mnt/data/analytics/raw-telemetry-2016-01-01.log.gz"
	}
}
```

#### Filter & Sort Data

Framework has inbuilt filters and sort utilities abtracted by the DataFilter object. Following are the APIs exposed by the DataFilter

```scala
/**
 * Filter and sort an RDD. This function does an 'and' sort on multiple filters
 */
def filterAndSort[T](events: RDD[T], filters: Option[Array[Filter]], sort: Option[Sort]): RDD[T]

/**
 * Filter a RDD on multiple filters
 */
def filter[T](events: RDD[T], filters: Array[Filter]): RDD[T]

/**
 * Filter a RDD on single filter
 */
def filter[T](events: RDD[T], filter: Filter): RDD[T]

/**
 * Filter a buffer of events.
 */
def filter[T](events: Buffer[T], filter: Filter): Buffer[T]

/**
 * Sort an RDD. The current sort supported is just plain String sort.
 */
def sortBy[T](events: RDD[T], sort: Sort): RDD[T]
```

Structure of the `Filter` and `Sort` objects are 

```scala
/*
 * Supported operators are:
 * 1. NE - Property not equals a value
 * 2. IN - Property in an array of values
 * 4. NIN - Property not in an array of values
 * 5. ISNULL - Property is null
 * 6. ISEMPTY - Property is empty
 * 7. ISNOTNULL - Property in not null
 * 8. ISNOTEMPTY - Property in not empty
 * 9. EQ - Property in equal to a value
 */
case class Filter(name: String, operator: String, value: Option[AnyRef] = None);

case class Sort(name: String, order: Option[String]);
```

Example Usage:

```scala
val rdd:RDD[Event] = DataFetcher.fetchBatchData[Event](Fetcher(...));

// Filter the rdd of events to only contain either OE_ASSESS or OE_INTERACT events only
val filterdRDD = DataFilter.filter[Event](rdd, Filter("eid", "IN", Option(Array("OE_ASSESS", "OE_INTERACT"))));
```

The `DataFilter` can do a nested filter too based on bean properties (silimar to json filter). For ex:

```scala
val filterdRDD = DataFilter.filter[Event](rdd, Filter("gdata.id", "EQ", Option("org.ekstep.delta")));
val filterdRDD = DataFilter.filter[Event](rdd, Filter("edata.eks.id", "ISNOTNULL", None));
```


#### Output Data

As discussed in the design document framework has support for the following output dispatchers:

1. ConsoleDispatcher - Dispatch output to the console. Basically used for debugging.
2. FileDispatcher - Dispatch output to a file.
3. KafkaDispatcher - Dispatch output to a kakfa topic
4. S3Dispatcher - Dispatch output to S3. Not recommended option. Can be used only for debugging purposes
5. ScriptDispatcher - Dispatch output to an external script (can be R, Python, Shell etc). Can be used for specific purposes like converting json output to a csv output (or) generating a static html report using R

Each and every output dispatcher should extend the `IDispatcher` and implement the following function

```scala
def dispatch(events: Array[String], config: Map[String, AnyRef]) : Array[String];
```

Output dispatchers are abstracted by the OutputDispatcher object.

APIs and example usage of OutputDispatcher

```scala
// APIs

// Dispatch to multiple outputs
def dispatch(outputs: Option[Array[Dispatcher]], events: RDD[String])

// Dispatch to single output
def dispatch(dispatcher: Dispatcher, events: RDD[String])

// Example Usage:
val rdd: RDD[String] = ....;
// File dispatcher
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "/mnt/data/analytics/test_ouput.log")), rdd);

// Dispatcher JSON schema

{
	"to": "", // file, s3, kafka, console, script
	"params": {
		// Dispatcher specific config parameters. Like topic and brokerList for kafka
	}
}

// Dispatcher Schema definition
case class Dispatcher(to: String, params: Map[String, AnyRef]);
```

#### Utility APIs

Following are the key utility APIs in CommonUtil

```scala

// Create and return the spark context
def getSparkContext(parallelization: Int, appName: String): SparkContext {}

// Set the s3 access key and secret fetched from environment variables
def setS3Conf(sc: SparkContext) {}

// Get dates between two dates
def getDatesBetween(fromDate: String, toDate: Option[String]): Array[String] {}

// Get the event (telemetry v1) timestamp in epoch format.
def getEventTS(event: Event): Long {}

// Get the event sync ts (telemetry v1) in epoch format.
def getEventSyncTS(event: Event): Long {}

// Get the time diff between two events
def getTimeDiff(start: Event, end: Event): Option[Double] {}

// Get the timestamp in epoch format from the datetime in string
def getTimestamp(timeInString: String): Long {}

// Round the decimal
def roundDouble(value: Double, precision: Int): Double {}

// Get the message id for an event. This is used as document id while indexing the data in ES
def getMessageId(eventId: String, userId: String, granularity: String, syncDate: Long): String {}

```

Following are the utility APIs in S3Util

```scala

// Upload a local file to S3 to the given bucket and key
def upload(bucketName: String, filePath: String, key: String) {}

// Get all keys in the bucket matching the prefix
def getAllKeys(bucketName: String, prefix: String): Array[String] = {}

// Search the bucket for all the keys matching the prefix and the date range. Delta is used in conjunction with toDate here. For ex: a toDate of 10-01-2016 and delta of 2 returns all keys matching the date range 08-01-2016 to 10-01-2016
def search(bucketName: String, prefix: String, fromDate: Option[String] = None, toDate: Option[String] = None, delta:Option[Int] = None): Buffer[String] = {}

```

Following are the utility APIs in RestUtil

```scala

// API to get data from the given URL. The expected response type is JSON and is serialized to the type passed
def get[T](apiURL: String) : T = {}

// API to post data to the given URL. The expected response type is JSON and is serialized to the type passed
def post[T](apiURL: String, body: String) : T = {}

// Example Usage
val url = Constants.getContentAPIUrl("org.ekstep.story.hi.elephant");
val response = RestUtil.get[Response](url); //Get content by content ID and parse it to response object

// Refer to the test case `TestRestUtil` for more examples
```

Following are the utility APIs in JSONUtils

```scala
// Serialize any object to JSON
def serialize(obj: AnyRef): String = {}

// Deserialize a JSON string to object of the Type T.
def deserialize[T: Manifest](value: String): T = {}

// Example Usage
val line = {"eid":"OE_START","ts":"2016-01-01T12:13:20+05:30","@timestamp":"2016-01-02T00:59:22.924Z","ver":"1.0","gdata":{"id":"org.ekstep.aser.lite","ver":"5.7"},"sid":"a6e4b3e2-5c40-4d5c-b2bd-44f1d5c7dd7f","uid":"2ac2ebf4-89bb-4d5d-badd-ba402ee70182","did":"828bd4d6c37c300473fb2c10c2d28868bb88fee6","edata":{"eks":{"loc":null,"mc":null,"mmc":null,"pass":null,"qid":null,"qtype":null,"qlevel":null,"score":0,"maxscore":0,"res":null,"exres":null,"length":null,"exlength":0.0,"atmpts":0,"failedatmpts":0,"category":null,"current":null,"max":null,"type":null,"extype":null,"id":null,"gid":null}}};
val event = JSONUtils.deserialize[Event](line);

```

#### Data Adapters

Following are the APIs of Content Adapter

```scala
// Get the game list - V1 api which is the same API used by genie services
def getGameList(): Array[Game] = {}

// Get all content list with status "Live"
def getAllContent(): Array[Content] = {}

// Get all items linked to a content.
def getContentItems(contentId: String, apiVersion: String = "v1") : Array[Item] = {}
```

Following are the APIs of Item Adapter

```scala
// Get item concept and max score given a content and item id. This is used by proficiency updater.
def getItemConceptMaxScore(contentId: String, itemId: String, apiVersion: String = "v1"): ItemConcept = {}
```

Following are the APIs of Domain Adapter

```scala
// Get the domain map from the LP. It contains all the "Live" concepts and relations.
def getDomainMap() : DomainMap = {}
```

Following are the APIs of User Adapter

```scala

// Get the user profile fields (handle, age, gender, language, standard etc) for a specific user
def getUserProfileMapping(userId: String): UserProfile = {}

// Get the language mapping in the system. id to code mapping.
def getLanguageMapping(): Map[Int, String] = {}

// Get the language mapping in the system for a given id.
def getLanguageMapping(langId: Int): String = {}
```

Go through the `Models` defined in the framework to get better understanding of the models passed by the adapters

#### Models

All models developed in the analytics should follow a common structure as explained below:

```scala
trait IBatchModelTemplate[T <: Input, A <: AlgoInput, B <: AlgoOutput, R <: Output] extends IBatchModel[T, R] {

    /**
     * Override and implement the data product execute method. In addition to controlling the execution this base class records all generated RDD's,
     * so that they can be cleaned up from memory when necessary. This invokes all the three stages defined for a data product in the following order
     * 1. preProcess
     * 2. algorithm
     * 3. postProcess
     */
    override def execute(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[R] = {
        ....
    }

    /**
     * Pre processing steps before running the algorithm. Few pre-process steps are
     * 1. Transforming input - Filter/Map etc.
     * 2. Join/fetch data from LP
     * 3. Join/Fetch data from Cassandra
     */
    def preProcess(events: RDD[T], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[A]

    /**
     * Method which runs the actual algorithm
     */
    def algorithm(events: RDD[A], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[B]

    /**
     * Post processing on the algorithm output. Some of the post processing steps are
     * 1. Saving data to Cassandra
     * 2. Converting to "MeasuredEvent" to be able to dispatch to Kafka or any output dispatcher
     * 3. Transform into a structure that can be input to another data product
     */
    def postProcess(events: RDD[B], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[R]
}

```

The purpose of this structure is to ensure:

1. Code readability and seperation of concerns.
2. Abstract the underlying processing of RDD's in terms of caching and clearing the cache.
3. Easy for data scientists to review the core algorithm

***

### Examples

#### Connect to spark-shell

```sh
# Connect to spark-shell with analytics framework and batch models included in classpath
<spark-home>/bin/spark-shell --jars <models-dir>/analytics-framework-0.5.jar,<models-dir>/batch-models-1.0.jar

# Connect to spark-shell with analytics framework and batch models included in classpath and providing custom cassandra host 
<spark-home>/bin/spark-shell --jars <models-dir>/analytics-framework-0.5.jar,<models-dir>/batch-models-1.0.jar --conf "spark.cassandra.connection.host=127.0.0.1"
```

#### Example scripts to read data

```scala

// Import required packages and set s3 credentials
import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
CommonUtil.setS3Conf(sc);

implicit val sparkContext = sc; // Re-assign the spark context as an implicit variable. Framework is designed to expect sc as implicit variable

// Read some data form S3
val queries = Option(Array(Query(Option("ekstep-dev-data-store"), Option("raw/"), Option("2016-01-01"), Option("2016-01-04"))));
val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
// Print the count on the console
rdd.count();

// Read data from local file
val queries = Option(Array(JSONUtils.deserialize[Query]("{\"file\":\"/mnt/data/analytics/akshara_session_summary.log\"}")))
val rdd = DataFetcher.fetchBatchData[MeasuredEvent](Fetcher("local", None, queries));
rdd.count();
```

#### Example scripts to filter data

```scala
// Get rdd as done in the first script
val filteredEvents = DataFilter.filter[Event](rdd, Filter("eid", "EQ", Option("OE_ASSESS")));
filteredEvents.count();

// Go through the test case TestDataFilter for more examples
```

#### Example scripts to dispatch output

```scala
// Get rdd as done in the first script
val rddAsString = rdd.map(JSONUtils.serialize(_));

/*
 * Dispatch output to file. Available params is "file" which is required
 */
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "/mnt/data/analytics/test_ouput.log")), rddAsString);

/*
 * Dispatch output to console. By default prints the total events size to console. Available params are:
 * 1. printEvent -> whether to print each event to console
 */
OutputDispatcher.dispatch(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])), rddAsString);

/*
 * Dispatch output to kafka. Available params are:
 * 1. brokerList -> Kafka broker list. Required parameter
 * 2. topic -> topic to dispatch the data. Required parameter
 */
OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> "localhost:9092", "topic" -> "telemetry.derived")), rddAsString);

/*
 * Dispatch output to s3. Available params are:
 * 1. bucket -> S3 bucket name. Required parameter
 * 2. key -> S3 key for the uploaded file. Required parameter
 * 3. zip -> true/false. Defaults to false. Whether to gzip the file or not.
 * 4. filePath -> Local file path to upload. Optional. Defaults to rdd if this is empty
 */
OutputDispatcher.dispatch(Dispatcher("s3", Map("bucket" -> "lpdev-ekstep", "key" -> "test-key", "zip" -> true.asInstanceOf[AnyRef])), rddAsString);

/*
 * Dispatch output to a script. Available params are:
 * 1. script -> script to invoke including the path. Required parameter
 */
OutputDispatcher.dispatch(Dispatcher("script", Map("script" -> "/mnt/data/analytics/generateReport.R")), rddAsString);
```

#### End to End sample script

Following is a sample script to fetch "takeoff" game data from production and compute average timespent per session

```scala
import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
CommonUtil.setS3Conf(sc);

implicit val sparkContext = sc;

// Read some data form S3
val queries = Option(Array(Query(Option("ekstep-telemetry"), Option("prod.telemetry.unique-"), Option("2016-01-15"), Option("2016-01-20"))));
val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
val oeEndEvents = DataFilter.filter(rdd, Filter("eid", "EQ", Option("OE_END")));
val takeOffSessions = DataFilter.filter(oeEndEvents, Filter("gdata.id", "EQ", Option("org.ekstep.delta")));
val timeSpent = takeOffSessions.map(e => CommonUtil.getTimeSpent(e.edata.eks.length).get);
timeSpent.mean(); // = 685.88
```

### Examples

> Compute per screen average time 

```scala
val queries = Option(Array(Query(Option("prod-data-store"), Option("ss/"), Option("2016-08-21"), Option("2016-09-22"))));
val rdd = DataFetcher.fetchBatchData[DerivedEvent](Fetcher("S3", None, queries));
val userContentEvents = DataFilter.filter(rdd, Filter("dimensions.gdata.id", "EQ", Option("do_30070866")));

val screenSummaries = userContentEvents.map { x =>
    x.edata.eks.screenSummary.map { x => x.asInstanceOf[Map[String, AnyRef]] };
}.reduce((a, b) => a ++ b);
val ssRDD = sc.makeRDD(screenSummaries.map(f => (f.get("id").get.asInstanceOf[String], f.get("timeSpent").get.asInstanceOf[Double])));
val result = ssRDD.groupBy(f => f._1).mapValues(f => {
    val values = f.map(f => f._2);
    (values.size, CommonUtil.roundDouble((values.sum / values.size), 2))
}).map(f => f._1 + "," + f._2._2 + "," + f._2._1);
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "screen_summaries.csv")), result);
```