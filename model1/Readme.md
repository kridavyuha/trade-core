Model 1:

Here we make buying selling friction less. We buy the stocks and market price and sell it at market price. The stock pool of a player can be assumed as infinite, but we increasine/decreases the price of a stock if the rate at which it is getting purchased (purchase_rate).

Factors that affect the market ...

*Demand :
   If we have Inf as the max capacity of each player i feel we are not talk upon supply terminology in our context, because it is always huge!

   So here i want to deal with 2 factors:

   a. Increasing Demand  -> Increase stock value
   b. Decreasing Demand  -> Decrease stock value.

   If a users Purchases x stocks then send { +x, timestamp } to queue
   If a user Sells x stocks then send { -x, timestamp } to queue

    Core computes events that have occured in the last t units of time and computes purchase_rate/minute. 
    Then relatively (as we wanted to give market simulation xperience) calculate the demand factor and sum it to the stock value.
   ** New Price= Current Price + k × Purchase Rate
   
    Lets say i want to update the demand factor in the contest for every 10 sec. A runner will run for every 10 sec and compute the demand factor.

*Performance : 
    These are the players performance that fluctuates the stocks. Buy the runs, wickets, catches they pick during the match.

