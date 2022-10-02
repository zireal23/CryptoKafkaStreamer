package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaSchemapb "github.com/zirael23/CryptoStreams/kafkaSchema"
	"github.com/zirael23/CryptoStreams/lib"
	"github.com/zirael23/CryptoStreams/util"
	"google.golang.org/protobuf/proto"
)

// Configuration to initialize the kafka consumer
// TODO: shift these to environment variables

var (
    topic = []string{
        os.Getenv("TOPIC"),
    }
    MIN_COMMIT_COUNT = 100;
)


/*
* This is the entry point of the application.
* First the kafka consumer is initialized using the configuration provied above.
* Then the database connection is established.
* Finally the kafka consumer is connected to the database and messages are read, processed
* and sent to the database.
*/

func main() {
        fmt.Println("Hello");
     kafkaConsumer := initConsumer();
    log.Println("The kafka consumer was successfully initialised");

    dbResources, err := util.OpenDatabaseConnection();
    if err != nil {
        log.Println("No DB Connection");
    }
    log.Println("Successfully Connected to DB");
    lib.InitMap();
    consumeKafkaMessages(kafkaConsumer, dbResources);
    // Function called to cancel the contexts that were created to avoid memory leaks.
    util.CloseDatabaseConnection(dbResources);
}

/*
* The consumer uses the Confluent Kafka Library.
* A config is created that has necessary information and metadata to establish connection to the
* Kafka broker.
* The config takes in the kafka address to connect to the broker.
* The group Id is the id of the consumer group it needs to connect to.
* If there are no previously stored offsets it will fall back to `"auto.offset.reset"` which defaults   to    the `latest` message.
* Auto commit is set to false for various reasons related to fault tolerance.
* For more info on why not to auto-commit: https://newrelic.com/blog/best-practices/kafka-consumer-config-auto-commit-data-loss
*/

func initConsumer() *kafka.Consumer {
    //ConfigMap is a map containing standard librdkafka configuration properties as documented in: https://github.com/edenhill/librdkafka/tree/master/CONFIGURATION.md
    configMap := kafka.ConfigMap{
        "bootstrap.servers": os.Getenv("BOOSTRAP_SERVERS"),
        "sasl.mechanisms": "PLAIN",
        "security.protocol": "SASL_SSL",
        "sasl.username":os.Getenv("SASL_USERNAME"),
        "sasl.password": os.Getenv("SASL_PASSWORD"),
        "group.id":       "kafkaConsumer",
        "auto.offset.reset": "smallest",
        "enable.auto.commit": "false",
    }

    kafkaConsumer, err := kafka.NewConsumer(&configMap);

    if err != nil {
        log.Println("The consumer failed to initialise", err.Error());
    }
    return kafkaConsumer;
}


/*
* This is the main function that reads the messages from the kafka consumer and processes them.
* For reading messages, it needs to subscribe to a specific topic.
* The kafka consumer pulls messages in intervals of 100 ms and an event is generated every time a message is pulled and the event is type asserted
* If the event is a valid kafka message, the message is unmarshalled to the the message type and the required aggregate prices are calculated and the data is sent to the database.
* If the event type is a PartitionEOF that means the consumer has consumed all the messages in the partition and thus in this implementation, retries to read messages until new messages are received.
* If the event is an error then the consumer is shut down follwoing up by cleaning up the memory allocated.
* If the event is something other than these cases then the consumer ignores it.
* Also the messages are commited at an interval of 100 messages.
*/

func consumeKafkaMessages(kafkaConsumer *kafka.Consumer, dbResources util.DBResources) {
    err := kafkaConsumer.SubscribeTopics(topic,nil);
    	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal);
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM);
    go func(){
        <-sigchan;
        fmt.Println("Closing consumer");
        kafkaConsumer.Close();
        os.Exit(1);
    }();
    if err != nil{
        log.Println("Couldnt subscribe to kafka topic", err.Error());
    }

    numberOfMessagesRead := 0;
    run := true;

    for run {
        kafkaevent := kafkaConsumer.Poll(100);
        
        switch event := kafkaevent.(type){
        case *kafka.Message:
            numberOfMessagesRead += 1;
            if numberOfMessagesRead % 100 == 0{
                kafkaConsumer.Commit();
            }
            log.Println(string(event.Key));
            
            eventResponse := event.Value;
            unmarshalDataAndWriteToDB(eventResponse, dbResources);
            log.Println("The number of messages read is:", numberOfMessagesRead);
           
        case kafka.PartitionEOF:
            log.Printf("%% Reached %v\n", event);
        case kafka.Error:
            fmt.Fprintf(os.Stderr, "%% Error: %v\n",event);
            run = false;
            util.CloseDatabaseConnection(dbResources);
            kafkaConsumer.Close();
        default:
            log.Printf("Ignored %v\n", event);
        }

    }

}


func unmarshalDataAndWriteToDB(coinMessage []byte, dbResources util.DBResources){
    var coinDataResponse kafkaSchemapb.CoinData;
    proto.Unmarshal(coinMessage,&coinDataResponse);
    coinDataforDB := util.CoinPriceData{
        ID: coinDataResponse.GetId(),
        Name: coinDataResponse.GetName(),
        RealPrice: coinDataResponse.GetPrice(),
        //Converting the unix timestamp to a time.Time object
        Timestamp: time.Unix(coinDataResponse.GetTimestamp(),0),
        ArithmeticAggregatePrice: lib.CalulateCurrentArithmeticMean(coinDataResponse.GetPrice(),coinDataResponse.GetId()),
        GeometricAggregatePrice: lib.CalculateCurrentGeometricMean(coinDataResponse.GetPrice(),coinDataResponse.GetId()),
        HarmonicAggregatePrice: lib.CalculateCurrentHarmonicMean(coinDataResponse.GetPrice(),coinDataResponse.GetId()),
    }
    util.InsertCoinPricesToDB(dbResources,coinDataforDB);
}