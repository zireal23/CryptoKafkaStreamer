package util

import (
	"context"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)



type CoinPriceData struct{
	ID string
	Name string
	RealPrice float32
	ArithmeticAggregatePrice float64
	GeometricAggregatePrice float64
	HarmonicAggregatePrice float64
	Timestamp time.Time
}

type DBResources struct {
	client *mongo.Client
	ctx context.Context
	cancel context.CancelFunc
	selectedCollection *mongo.Collection
}

/*
* Uses the default mongoDB golang driver.
* Connects to the mongoDB instance running in docker using the connection string.
* Initialises the time series collection, and contexts.
* Context- context is a standard package of Golang that makes it easy to pass request-scoped values, cancelation signals, and deadlines across API boundaries to all the goroutines involved in handling a request.
* Returns the essential database structs and functions to be used to send/receive to/from the databse as well as the context cancel function to close the database connection at the end of the execution.
*/

func OpenDatabaseConnection() (DBResources, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb+srv://cryptoDB:sayan@cluster0.dve8ojg.mongodb.net/?retryWrites=true&w=majority"));
	if err != nil {
		log.Printf("Couldnt create mongoDB client due to: %v", err);
		return DBResources{}, err;
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second);

	err = client.Connect(ctx);

	var dbResources DBResources;
	
	if(err != nil){
		log.Println("Couldnt connect to mongodb instance");
		cancel();
		return dbResources, err;
	}
	
	//Creating and initialising the time series collection
	err = initTimeSeriesCollection(client);

	if err != nil {
		log.Println("Couldnt create the time series collection");
	}
	
	selectedCollection := client.Database("cryptoDB").Collection("cryptoPricesTimeSeries");

	log.Println("Successfully connected to mongo and initialised a time series collection!");

	//Required by other functions to send and recieve to/from the database
	dbResources = DBResources{
		client: client,
		ctx: ctx,
		cancel: cancel,
		selectedCollection: selectedCollection,

	}

	return dbResources, nil;
}



func initTimeSeriesCollection(client *mongo.Client) error {
	//creating the database if not present
	cryptoDataDB := client.Database("cryptoDB");

	// setting time series data options
	timeSeriesOptions := options.TimeSeries().SetTimeField("timestamp").SetGranularity("seconds");

	// setting mongodb collection options
	collectionOptions := options.CreateCollection().SetTimeSeriesOptions(timeSeriesOptions).SetExpireAfterSeconds(604800);

	//Creating the time series collection 
	err := cryptoDataDB.CreateCollection(context.TODO(),"cryptoPricesTimeSeries",collectionOptions);

	if(err != nil){
		return err;
	}
	return nil;
}



/*
* This function creates a BSON document to be insterd into the database.
* The BSON document contains all the coin data information to be stored inside the database.
*/

func InsertCoinPricesToDB(dbResources DBResources,coinData CoinPriceData){
	selectedCollection := dbResources.selectedCollection;

	insertCoinPriceQuery := bson.D{
		{Key: "ID", Value: coinData.ID},
		{Key: "Name", Value: transformStringForDatabase(coinData.Name)},
		{Key: "RealPrice", Value: coinData.RealPrice},
		//Converting the time.Time object to a primitive.DateTime object because thats the timestamp that is identified as a valid timestamp for the BSON format
		{Key: "timestamp", Value: primitive.NewDateTimeFromTime(coinData.Timestamp)},
		//TODO: Add GeometricAggregatePrice and HarmonicAggregatePrice later
		{Key: "ArithmeticAggregatePrice", Value: coinData.ArithmeticAggregatePrice},
		{Key: "GeometricAggregatePrice", Value: coinData.GeometricAggregatePrice},
		{Key: "HarmonicAggregatePrice", Value: coinData.HarmonicAggregatePrice},
		
	}
	//filter := bson.M{"ID": coinData.GetId()};

	_, err := selectedCollection.InsertOne(context.TODO(),insertCoinPriceQuery);

	if err != nil{
		log.Println("Couldnt insert data into DB", err.Error());
	}
	//log.Println(result);
}

//Function to query a specific coin price(not used, just in case)
func GetCoinPrices(coin string, dbResources DBResources){
	filter := bson.M{	
		"Name": coin,
	};
	var result bson.M;
	opts := options.FindOne();
	err := dbResources.selectedCollection.FindOne(dbResources.ctx,filter,opts).Decode(&result);
	if err != nil {
		log.Println("Couldnt find coin price");
	}
	queryResult, err := bson.Marshal(result);
	if err != nil {
		log.Println("Couldnt marhshal the query result");
	}
	var coinData CoinPriceData;
	err = bson.Unmarshal(queryResult,&coinData);
	if err != nil {
		log.Println("Couldnt unmarshal the query result into the struct");
	}
	log.Println(coinData.RealPrice, coinData.Name);
}


//Disconnect from the databse and close the created contexts
func CloseDatabaseConnection(dbResources DBResources){
	dbResources.client.Disconnect(dbResources.ctx);
	dbResources.cancel();
	log.Println("Successfully closed connection to mongoDB");
}

//Utility function
func transformStringForDatabase(coin string) string{
	return strings.ReplaceAll(coin," ","_");
}