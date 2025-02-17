# trade-core
This service maintains the life cycle of the contest by Adjusting the stock prices (Demand &amp; Supply) according to market sentiments.

We generally process the transactions per player in league in batches (1 sec batches). This is mainly to treat at all these transactions coming in this timeframe with same player-price. 

And they we calculate netshares per player within this batch : 
if net >0 that mean if there are more buys than sells in that batch we do 

1. Give/Take assets at cur price -> here update the price with 0.1% of last one in players price list
2. Update the cur price for next batch

if net <=0 that mean there are more sells than buys in that batch we do 

1. Update the cur price  -> here update the price with 0.1% of last one in players price list
2. Give/Take assets at this cur price.





