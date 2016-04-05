###Ghetto version control
# V0.1 : refactoring and z-score correction
# V0.2 : switching to rollSFM from dynlm
#       Abandon the flag3 for now
#       change channel definition to mean + thresh * ATR
#v0.3: fixed a mistake in the range, tweaked parameters
#       added an eyeb() function that takes a i number for easy ID (check confu_list, e.g.)
#       position the regression on the high-low instead of close
#       added a filter to ensure most slop coeficients are in the same direction
library(quantmod)
library(TTR)
library(moments)
#library(dynlm)
library(xts)
library(lubridate)

#output of getbar from Rlbpapi with bar=1mn
#attached for reference
df <- cl_trade
#resample so we dont f-up the final step of munging

full_ind <- seq(index(df)[1],last(index(df)),60)
dumdum<-xts(rep(1,length(full_ind)), order.by = full_ind)
df <- merge(dumdum,df)
df$value <- NULL
df$dumdum<-NULL
rm(dumdum,full_ind)

df$open <- na.locf(df$open)
df$high <- na.locf(df$high)
df$low <- na.locf(df$low)
df$close <- na.locf(df$close)
df$numEvents[is.na(df$numEvents)]<-0
df$volume[is.na(df$volume)]<-0

#first we'll calculate:
## returns
## SMA
## LMA
## RSI
## true range
## sdev
##skewness/kurtosis
## average trade size



#returns
df$ret <- ROC(df$close)

#SMA
n_sma<-10
n_lma <- 120
df$sma <- SMA(df$close,n_sma)
df$lma <- SMA(df$close,n_lma)


#RSI
df$rsi <- RSI(df$close, n=n_sma, maType="EMA")

#true range <--- question: ATR or TR ?? Iwent for ATR at the n_sma
df$atr <- ATR(df[,c("high","low","close")],n_sma, maType="EMA")[,1]


#stdev <---- the trailing window is abritrary. Thoughs ??
n_sdev<-30
df$sd <- rollapply(df$ret, n_sdev,sd)
df$mn<-rollmean(df$close, n_sdev)
df$sd_p<-rollapply(df$close, n_sdev,sd)

#skew (again arbitrarily selecting the window at 2h/120mn)
df$skew <- rollapply(df$ret,n_lma,skewness)

#kurtosis
df$kurt <- rollapply(df$ret,n_lma,kurtosis)

#average trade size
#df$t_s <- df$volume/df$numEvents <- this ficked the set
df$t_s <- df$numEvents
df$numEvents <- NULL

####
#Adding dummy variables
#####

#  Monday
# Delivery month Mar/Jun/Sep/Dec
# "busy"-> 9AM -2PM Chicago
# Monday
df$monday <- ifelse(weekdays(index(df))=="Monday", 1,0)

#main rolls
df$roll <- ifelse(lubridate::month(index(df)) %in% c(3,6,9,12),1,0)

#chicago busy time

df$busy <- ifelse(as.numeric(strftime(index(df),format="%H",tz="America/Chicago")) %in% 9:14,1,0)


###########
#now for the range thing
## slope
#slop<-function(x){as.numeric(dynlm(close ~ trend(close), data=x)$coefficients[2])}
#warnings issued for the bellow, and hist(df$r2) suggest weird stuff
#r2 <- function(x){as.numeric(summary(dynlm(close ~ trend(close), data=x))$r.squared)}

#df$trend<-rollapply(df$close, n_sma, slop ) #this is a RIGHT window. Should we take a mid point ??
#df$r2 <- rollapply(df$close, n_sma, r2 )

#decided to center the window. Using lag() and a 5mn window.Na.pad shouldn't be anywways
# Can't get this cr@p to work, window=R will have to do 
slp_wdw <- 5
df <- merge(df,rollSFM((df$high+df$close)/2,.index(df),slp_wdw))
#bellowto avoid changing code further
names(df)[names(df) == "r.squared"] <- "r2" 
names(df)[names(df) == "beta"] <- "trend" 
# I don't need the intercept
df$alpha <- NULL
## range. Logic to normalize max(high&low) by the close price is (very) debatable
# edit: the padding causes some null sd and fricks the vectors later
df$z_low <- (df$low-df$mn)/df$sd_p
df$z_high <- (df$high-df$mn)/df$sd_p

#cleanup (potential fuckery) EDIT2: fuckery at market close
df <- df[complete.cases(df)]


#####
## Now we loop/munge
## for now, every 5mn
## Sliding window of 10mn, munge the first 5mn into the flat file and flag the next one

# how many iterations
itra <- as.numeric(floor(difftime(last(index(df)),index(df)[1],units="mins")/5))

cols<-c(sprintf("volume%1d",seq(1:5)),
        sprintf("returns%1d",seq(1:5)), #misspelled? returnds
        sprintf("sma%1d",seq(1:5)),
        sprintf("lma%1d",seq(1:5)),
        sprintf("rsi%1d",seq(1:5)),
        sprintf("atr%1d",seq(1:5)),
        sprintf("skew%1d",seq(1:5)),
        sprintf("kurt%1d",seq(1:5)),
        sprintf("trades%1d",seq(1:5)),
        "monday",
        "roll",
        "chicago",
        "flag1",
        "flag2")


ml <-data.frame(cols)
#for eyeball investigation
confu_list <- c()
trend_list <- c()
nothing_list <-c()
range_list <- c()
for (i in 0:itra-1){
#for (i in 0:1000){
  #i=200    
  
  
  start <- index(df)[1]+i*60*5
  mid <-start+5*60
  end <- start+10*60
  tmp <- df[index(df) >=start & index(df) < mid]
  tmp2 <- df[index(df) >=mid & index(df) < end]
  #bellow is a catcher for some fuckery with market close (I think my complete.case wipes
  #them out for some reason. To be reviewed)
  if(dim(tmp)[1]==5 & dim(tmp2)[1]==5 & prod(tmp$sd_p) != 0 & prod(tmp2$sd_p) != 0 ){
    #there maybe some categorization issue to consider here (flags like monday, or lot size should be integers or factors but that won't show in csv)
    vctr <- c(as.vector(tmp$volume),
              as.vector(tmp$ret),
              as.vector(tmp$sma),
              as.vector(tmp$lma),
              as.vector(tmp$rsi),
              as.vector(tmp$atr),
              as.vector(tmp$skew),
              as.vector(tmp$kurt),
              as.vector(tmp$t_s),
              first(tmp$monday),
              first(tmp$roll),
              first(tmp$busy))
    
    #flag logic is, hum, fuzzy
    z_thresh <- 2
    r2_thresh <- 0.6
    slp_thresh <- 1e-04 #pseudoarbitrary, based on hist(df$trend) and 25% quantile
    #flag is set with (a) all r2 are above thresh and (b) slp is above thresh and (c) secondflag
    #(for now) is set if z scores are under2
    
    if(sum(tmp2$low > as.numeric(mean(tmp$close) - z_thresh * last(tmp$atr))) + 
       sum(tmp2$high < as.numeric(mean(tmp$close) + z_thresh * last(tmp$atr))) > 9){flag1 <- "range"}else{
          flag1 <- "norange"}
#applieda filter, and will focus on the last 3mn  of the period toavoid missing trends    
    if(sum(tail(tmp2$r2,3) > r2_thresh) > 2 & sum(abs(tail(tmp2$trend,3)) > slp_thresh) > 2 & sum(abs(sign(tail(tmp2$trend,3))))>2){flag2 <-"trend"}else{
          flag2 <- "notrend"}
    
    #flag ficks the type. Use "factor" ??
    #list confused and trends for investigation
    
    if(flag1 == "range" & flag2 == "trend"){confu_list <- c(confu_list, i)}
    if(flag1 == "norange" & flag2 == "trend"){trend_list <- c(trend_list, i)}
    if(flag1 == "norange" & flag2 == "notrend"){nothing_list <- c(nothing_list, i)}
    if(flag1 == "range" & flag2 == "notrend"){range_list <- c(range_list, i)}
    vctr<-c(vctr,flag1,flag2)
      ml<-data.frame(ml,vctr)

    }  
}

ml<-t.data.frame(ml) 

colnames(ml) <- ml[1,]  #make them real column names

ml <- ml[-1,] #remove 1st row that contained column names


#summary - refactored to print 1 line results
print(paste("trending:", sum(ml[,50] =="trend" & ml[,49] == "norange")))
print(paste("range:", sum(ml[,49] == "range" & ml[,50] == "notrend")))
print(paste("nothing:", sum(ml[,49] == "norange" & ml[,50] == "notrend")))
print(paste("confused:", sum(ml[,49] == "range" & ml[,50] == "trend")))


#percentage summarys
print(paste("percent_trending:", sum(ml[,50] =="trend")/nrow(ml)))
print(paste("percent_range:", sum(ml[,49] == "range")/nrow(ml)))
print(paste("percent_nothing:", sum(ml[,49] == "norange" & ml[,50] == "notrend")/nrow(ml)))
print(paste("percent_confused:", sum(ml[,49] == "range" & ml[,50] == "trend")/nrow(ml)))

#kill confused or nothing and write
ml <- as.data.frame(ml)
ml <- subset(ml, (flag1 == "norange" & flag2 == "trend")| (flag1 == "range" & flag2 == "notrend"))

write.csv(ml,file="~/mlset.csv")
#####
#For now, no normalization. Azure ML has some squashers, and some are specific by RNN. Question is whether the API data needs be squashed before or not

#eyeball using quantmod
eyeb<-function(x){
i=x
start <- index(df)[1]+i*60*5
mid <-start+5*60
end <- start+10*60
tmp <- df[index(df) >=start & index(df) < mid]
tmp2 <- df[index(df) >=mid & index(df) < end]
tmp3 <- df[index(df) >=start & index(df) < end]

mychartTheme <- chart_theme()
mychartTheme$rylab = T 
chart_Series(tmp3[,c("open","high","low","close")], theme=mychartTheme)

#add_TA(tmp2$mn+z_thresh*tmp2$sd_p,on=1, col=4)
#add_TA(tmp2$mn-z_thresh*tmp2$sd_p,on=1, col=4)

slp_av <- mean(tail(tmp2$trend,3))
ln_slp <- function(x){xts(coredata(first(x)+slp_av*as.numeric((index(x)-first(index(x))))),order.by=index(x))}
dummy <- (tmp2$high+tmp2$low)/2
add_TA(ln_slp(dummy),on=1, col=3)
ta_up <- xts(rep(mean(tmp$close)+z_thresh*last(tmp$atr),length(index(tmp2))),order.by = index(tmp2))
add_TA(ta_up, on=1, col=4)
ta_dn <- xts(rep(mean(tmp$close)-z_thresh*last(tmp$atr),length(index(tmp2))),order.by = index(tmp2))
add_TA(ta_dn, on=1, col=4)

}
#eye ball sample
sample(confu_list,10)
sample(range_list,10)
sample(trend_list,10)
sample(nothing_list,10)

