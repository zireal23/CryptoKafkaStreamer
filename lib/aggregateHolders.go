package lib

import (
	"container/list"
	"log"
)

/**
* For every coin I am storing the prices of every 10 second interval for a 24 hour period.
* So there will be 8640 records for every coin.
* Now as soon as there are more than 8640 records, I remove the last record and store the most recent one.
* This way I can keep maintaining my rolling array in O(1) order and the averages as well.
* So I have a struct that will have an array for holding the prices, an integer to keep track of the number
* of the elements in the array and three floats to keep track of the current linear,
* geomteric and harmonic means.
**/

/**
* The cryptoPricesArray is a FIFO structure that supports pushing elements to the end of the list
* as well as removing the first element of the list that is also the oldest.
* The only overhead is allocating memory and deallocating memory during insertion and removal respectively.
**/

type RollingMeans struct {
	CryptoPricesArray *list.List
	LinearSumOfAllPrices float64
	LoagarithmicSumOfAllPrices float64
	ReciprocatedSumOfAllPrices float64
    NumberOfElements int32;
}
//Maximum number of prices to be held in the array for a sepcific currency
//The time window is 1 day and hence the number of 10 second intervals in a day is 8640
var LimitOfArrayElements int32 = 8640;


	/**
		The map stores the references to structs and not the struct itself
		So when we access a map value what we get is the copy of that struct 
		For reading purposes that is alright
		But for modifying the values, we cant modify the copy since the origin value is unmutated
		Whats best would be to have a map of struct pointers, that way we access the struct stored directly
	**/
var CryptoAggregatePricesHolder map[string]*RollingMeans;




/**
* TO initialize the map, we need to call the InitMap function
* The map is a global variable so we can access it from anywhere
* The map is a map of strings to RollingMeans structs
* The RollingMeans struct has a list of floats, three floats for the means and an integer for the number of elements.
* The make function of golang is used to initialize the map, it takes the type of the key and the type of the value.
* The map is a reference type so we dont need to return it.
**/

func InitMap(){
	CryptoAggregatePricesHolder = make(map[string]*RollingMeans);
}


/**
* This function is used to check if the map has been initialized or not
* If not then we initialize it
* If yes then we check if the map has the key for the cryptoSymbol
* If not then we initialize the map for that cryptoSymbol
* We initialise all the fields of the struct to 0.
**/

func CheckAndInitCurrencyMap(cryptoSymbol string) {
	if _, isPresent := CryptoAggregatePricesHolder[cryptoSymbol]; !isPresent{
		currencyAggregates := &RollingMeans{
			CryptoPricesArray: list.New(),
			LinearSumOfAllPrices: 0,
			LoagarithmicSumOfAllPrices: 0,
			ReciprocatedSumOfAllPrices: 0,
			NumberOfElements: 0,
		}
		CryptoAggregatePricesHolder[cryptoSymbol] = currencyAggregates;
}
}


/**
* This function is used to update the holding structs for a specific crypto symbol.
* The function takes the crypto symbol and the current price as parameters.
* The function first checks if the map has been initialized or not.
* If not then it initializes the map.
* Since the prices array is a collection of prices for a 24 hour period at 10 second intervals, we need to check if the array has reached its limit or not.
* If not then we just need to add the new price to the array and increment the number of elements.
* If yes then we need to remove the oldest price from the array and add the new price to the array.
* The function also inserts the new price to the array after doing a type assertion since the list container stores the elements as interface types.
**/


func UpdateCryptoStructs(cryptoSymbol string, currentPrice float32){
	CheckAndInitCurrencyMap(cryptoSymbol);

	if(CryptoAggregatePricesHolder[cryptoSymbol].NumberOfElements+1<=LimitOfArrayElements){
		CryptoAggregatePricesHolder[cryptoSymbol].NumberOfElements++;
	}else{
		/*
		* We are removing the oldest value
		* We hold the original array in a variable, remove the last element and assign it again to the struct
		*/
		log.Println("Reslicing the prices holder");
		oldestCurrencyPrice := CryptoAggregatePricesHolder[cryptoSymbol].CryptoPricesArray.Front();
		CryptoAggregatePricesHolder[cryptoSymbol].CryptoPricesArray.Remove(oldestCurrencyPrice);
	}
	CryptoAggregatePricesHolder[cryptoSymbol].CryptoPricesArray.PushBack(float32(currentPrice));

}