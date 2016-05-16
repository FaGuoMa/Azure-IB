# Map 1-based optional input ports to variables
dataset1 <- maml.mapInputPort(1) # class: data.frame
dataset2 <- maml.mapInputPort(2) # class: data.frame

# Contents of optional Zip port are in ./src/
# source("src/yourfile.R");
# load("src/yourData.rdata");

# Sample operation
data.set = rbind(dataset1, dataset2);

# You'll see this output in the R Device port.
# It'll have your stdout, stderr and PNG graphics device(s).
plot(data.set);

# Select data.frame to be sent to the output Dataset port
maml.mapOutputPort("data.set");

#fake initiation
dataset1 <-read.csv(file="/Users/maxime_back/Documents/avocado/rnn.csv",header = T)

#simple polling
#dataset1$trend_poll <- ifelse((dataset1$trend_NN == "trend") +(dataset1$trend_TCdeep == "trend") + (dataset1$trend_boostDT == "trend") > 
#                                (dataset1$trend_NN == "notrend") +(dataset1$trend_TCdeep == "notrend") + (dataset1$trend_boostDT == "notrend"), "trend", "notrend")

dataset1$range_poll_conf <- (dataset1$range_NN == dataset1$flag2)*dataset1$range_NNprob+
  (dataset1$range_TCdeep == dataset1$flag2)*dataset1$range_TCdeepprob+
  (dataset1$range_boostDT == dataset1$flag2)*dataset1$range_boostDTprob


#polling with thresh. ). 20% brings .5%/1% extra, so why not
thresh <- 0.2


dataset1$trend_poll <- ifelse((dataset1$trend_NN == "trend" & dataset1$trend_NNprob > thresh) 
                              +(dataset1$trend_TCdeep == "trend" & dataset1$trend_TCdeepprob > thresh) 
                              + (dataset1$trend_boostDT == "trend" & dataset1$trend_boostDTprob > thresh) > 
                                (dataset1$trend_NN == "notrend" & dataset1$trend_NNprob > thresh) 
                              +(dataset1$trend_TCdeep == "notrend" & dataset1$trend_TCdeepprob > thresh) 
                              + (dataset1$trend_boostDT == "notrend" & dataset1$trend_boostDTprob > thresh), "trend", "notrend")

####
#now for range
####

dataset1$trend_poll <- ifelse((dataset1$trend_NN == "trend") +(dataset1$trend_TCdeep == "trend") + (dataset1$trend_boostDT == "trend") > 
                                (dataset1$trend_NN == "notrend") +(dataset1$trend_TCdeep == "notrend") + (dataset1$trend_boostDT == "notrend"), "trend", "notrend")

dataset1$trend_poll_conf <- (dataset1$trend_NN == dataset1$flag2)*dataset1$trend_NNprob+
  (dataset1$trend_TCdeep == dataset1$flag2)*dataset1$trend_TCdeepprob+
  (dataset1$trend_boostDT == dataset1$flag2)*dataset1$trend_boostDTprob
  
  

#polling with thresh. ). 20% brings .5%/1% extra, so why not
thresh <- 0.2


dataset1$trend_poll <- ifelse((dataset1$trend_NN == "trend" & dataset1$trend_NNprob > thresh) 
                              +(dataset1$trend_TCdeep == "trend" & dataset1$trend_TCdeepprob > thresh) 
                              + (dataset1$trend_boostDT == "trend" & dataset1$trend_boostDTprob > thresh) > 
                                (dataset1$trend_NN == "notrend" & dataset1$trend_NNprob > thresh) 
                              +(dataset1$trend_TCdeep == "notrend" & dataset1$trend_TCdeepprob > thresh) 
                              + (dataset1$trend_boostDT == "notrend" & dataset1$trend_boostDTprob > thresh), "trend", "notrend")

dataset1$range_poll <- ifelse((dataset1$range_NN == "range") +(dataset1$range_TCdeep == "range") + (dataset1$range_boostDT == "range") > 
                                (dataset1$range_NN == "norange") +(dataset1$range_TCdeep == "norange") + (dataset1$range_boostDT == "norange"), "range", "norange")

dataset1$range_poll_conf <- (dataset1$range_NN == dataset1$flag2)*dataset1$range_NNprob+
  (dataset1$range_TCdeep == dataset1$flag2)*dataset1$range_TCdeepprob+
  (dataset1$range_boostDT == dataset1$flag2)*dataset1$range_boostDTprob


#polling with thresh. ). 20% brings .5%/1% extra, so why not
thresh <- 0.2


dataset1$range_poll <- ifelse((dataset1$range_NN == "range" & dataset1$range_NNprob > thresh) 
                              +(dataset1$range_TCdeep == "range" & dataset1$range_TCdeepprob > thresh) 
                              + (dataset1$range_boostDT == "range" & dataset1$range_boostDTprob > thresh) > 
                                (dataset1$range_NN == "norange" & dataset1$range_NNprob > thresh) 
                              +(dataset1$range_TCdeep == "norange" & dataset1$range_TCdeepprob > thresh) 
                              + (dataset1$range_boostDT == "norange" & dataset1$range_boostDTprob > thresh), "range", "norange")


############
#above is BS
#fake initiation
dataset1 <-read.csv(file="/Users/maxime_back/Documents/avocado/rnn.csv",header = T)



#simple polling as the threshold is not really helping
dataset1$trend_poll <- ifelse((dataset1$trend_NN == "trend") +(dataset1$trend_TCdeep == "trend") + (dataset1$trend_boostDT == "trend") > 
                                (dataset1$trend_NN == "notrend") +(dataset1$trend_TCdeep == "notrend") + (dataset1$trend_boostDT == "notrend"), "trend", "notrend")

#poll trend confindence (as in "sum of confidence if youwere right")
dataset1$trend_poll_conf <- (dataset1$trend_NN == dataset1$flag2)*dataset1$trend_NNprob+
  (dataset1$trend_TCdeep == dataset1$flag2)*dataset1$trend_TCdeepprob+
  (dataset1$trend_boostDT == dataset1$flag2)*dataset1$trend_boostDTprob
#simple polling as the threshold is not really helping
dataset1$range_poll <- ifelse((dataset1$range_NN == "range") +(dataset1$range_TCdeep == "range") + (dataset1$range_boostDT == "range") > 
                                (dataset1$range_NN == "norange") +(dataset1$range_TCdeep == "norange") + (dataset1$range_boostDT == "norange"), "range", "norange")
#poll trend confindence (as in "sum of confidence if youwere right")
dataset1$range_poll_conf <- (dataset1$range_NN == dataset1$flag1)*dataset1$range_NNprob+
  (dataset1$range_TCdeep == dataset1$flag1)*dataset1$range_TCdeepprob+
  (dataset1$range_boostDT == dataset1$flag1)*dataset1$range_boostDTprob

#combine outcome. Current results before combo: 1099 observations, range&trend: 102 notrend&norange: 26
dataset1$final <- ifelse(dataset1$trend_poll == "trend" & dataset1$range_poll == "norange", "trend",
                         ifelse(dataset1$trend_poll == "notrend" & dataset1$range_poll == "range", "range",
                                ifelse(dataset1$trend_poll == "trend" & dataset1$range_poll == "range", 
                                       ifelse(dataset1$trend_poll_conf>dataset1$range_poll_conf,"trend","range"),"nothing")))

data.set <- as.data.frame(dataset1$final)
