
library(tidyverse)
library(chron)
library(lubridate)
library(nycflights13)
data <- read.csv( "/Users/yuejin/Downloads/raw_travis211.csv", header=T)
nrow(data) #155268
View(data)
head(data)
str(data)
data$date_start <- mdy_hm(data$date_of_call_start_text)
data$year <- year(data$date_start)
data$week_number <- week(data$date_start)
data$week_sunday <- floor_date(data$date_start, "week")
data$hour <- hour(data$date_start)
data$is_weekend <- is.weekend(data$date_start)
data$is_weekend <- as.numeric(data$is_weekend)
data$call_time_block[data$hour<4] = '0-3'
data$call_time_block[data$hour<8 & data$hour >= 4] = '4-7'
data$call_time_block[data$hour<12 & data$hour >= 8] = '8-11'
data$call_time_block[data$hour<16 & data$hour >= 12] = '12-15'
data$call_time_block[data$hour<20 & data$hour >= 16] = '16-19'
data$call_time_block[data$hour>= 20] = '20-24'


