package lib

import "math"

/**
* This function calculates the current airthmetic mean of the prices of a particular crypto currency.
* The function takes the current price and the crypto symbol as parameters.
* The function first checks if the map has been initialized or not.
* If not then it initializes the map.
* The function then calculates the current arithmetic mean by adding the current price to the old sum of prices and dividing it by the number of elements.
* We also need to check if the number of elements is less than the limit or not.
* If not then we need to subtract the oldest price from the array from the current sum of prices.
* The function then updates the sum of prices in the map and returns the current arithmetic mean.
**/


func CalulateCurrentArithmeticMean(currentPrice float32, cryptoSymbol string) float64 {

	CheckAndInitCurrencyMap(cryptoSymbol);
		
	oldSumOfPrices := CryptoAggregatePricesHolder[cryptoSymbol].LinearSumOfAllPrices;
	numberOfElements := CryptoAggregatePricesHolder[cryptoSymbol].NumberOfElements;
	currentSumOfPrices := oldSumOfPrices + float64(currentPrice);
	
	//If the number of elements in the array is less than limit, then we just need to add the new price and increment the number of elements
	//If not then we need to add the new price and subtract the last price in the array as well
	if numberOfElements<LimitOfArrayElements {
		numberOfElements++;
	}else{
		//The value is type asserted
		currentSumOfPrices -= (CryptoAggregatePricesHolder[cryptoSymbol].CryptoPricesArray.Front().Value.(float64));
	}
	
	
	//TODO: Check for debugging errors later
	arithmeticMean := currentSumOfPrices / float64(numberOfElements);
	CryptoAggregatePricesHolder[cryptoSymbol].LinearSumOfAllPrices = currentSumOfPrices;
	//Insert the new prices to the price-holding array
	UpdateCryptoStructs(cryptoSymbol,currentPrice);
	
	return arithmeticMean;
}

/*
* Calculating the rolling geometric mean of so many numbers would have caused overflow.
* So we sum up the logarithmic values of all the prices, divide the sum by the number of elements and then take antilog of the resultant value and that will be our geomtric mean.
* Refer- https://www.geeksforgeeks.org/geometric-mean-two-methods/
*/

func CalculateCurrentGeometricMean(currentPrice float32, cryptoSymbol string) float64 {
	CheckAndInitCurrencyMap(cryptoSymbol);
	oldLogarithmicSumOfAllPrices := CryptoAggregatePricesHolder[cryptoSymbol].LoagarithmicSumOfAllPrices;
	numberOfElements := CryptoAggregatePricesHolder[cryptoSymbol].NumberOfElements;
	//math.Log() returns the natural logarithm value of the number passed in as an arg
	currentLogarithmicSumOfAllPrices := oldLogarithmicSumOfAllPrices + math.Log(float64(currentPrice));

	if numberOfElements<LimitOfArrayElements {
		numberOfElements++;
	}else{
		//Subtracting the log value of the price that is outside the window
		currentLogarithmicSumOfAllPrices -= math.Log(CryptoAggregatePricesHolder[cryptoSymbol].CryptoPricesArray.Front().Value.(float64));
	}
	//Converting the natural logarithm to floating numbers using the antilog (exponent with base e)
	geometricMean := math.Exp(currentLogarithmicSumOfAllPrices/float64(numberOfElements));
	CryptoAggregatePricesHolder[cryptoSymbol].LoagarithmicSumOfAllPrices = currentLogarithmicSumOfAllPrices;
	UpdateCryptoStructs(cryptoSymbol,currentPrice);

	return geometricMean;
}

/*
* Steps to calculate the harmonic mean are −
* Do the reciprocal of the elements
* Add all the reciprocated elements together
* Now divide the total number of elements in an array by the sum of reciprocated elements
*/

func CalculateCurrentHarmonicMean(currentPrice float32, cryptoSymbol string) float64 {
	CheckAndInitCurrencyMap(cryptoSymbol);
	oldReciprocatedSumOfAllPrices := CryptoAggregatePricesHolder[cryptoSymbol].ReciprocatedSumOfAllPrices;
	numberOfElements := CryptoAggregatePricesHolder[cryptoSymbol].NumberOfElements;
	currentReciprocatedSumOfAllPrices := oldReciprocatedSumOfAllPrices + float64(1/currentPrice);

	if numberOfElements<LimitOfArrayElements {
		numberOfElements++;
	}else{
		//Subtracting the inverse of the price that is outside the window
		currentReciprocatedSumOfAllPrices -= 1/(CryptoAggregatePricesHolder[cryptoSymbol].CryptoPricesArray.Front().Value.(float64));
	}

	harmonicMean := float64(numberOfElements)/currentReciprocatedSumOfAllPrices;
	CryptoAggregatePricesHolder[cryptoSymbol].ReciprocatedSumOfAllPrices = currentReciprocatedSumOfAllPrices;
	UpdateCryptoStructs(cryptoSymbol,currentPrice);

	return harmonicMean;

}