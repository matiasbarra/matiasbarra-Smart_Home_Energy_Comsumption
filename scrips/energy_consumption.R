####-----------------------[ Set the enviroment ]-----------------------####

pacman::p_load(RMySQL, dplyr, lubridate, tidyverse, ggplot2, plotly, DBI, 
               tidyverse, imputeTS, padr, chron, lattice, grid, forecast, stats, data.table)

## Create a database connection - SQL
con = dbConnect(MySQL(), user='deepAnalytics', password='Sqltask1234!',
                dbname='dataanalytics2018', 
                host='data-analytics-2018.cbrosir2cswx.us-east-1.rds.amazonaws.com')

## List the tables contained in the database 
dbListTables(con)

## Create a query requesting specific information from the original db 
query <- "
SELECT * FROM yr_2006
UNION ALL
SELECT * FROM yr_2007
UNION ALL
SELECT * FROM yr_2008
UNION ALL
SELECT * FROM yr_2009
UNION ALL
SELECT * FROM yr_2010
"

## df with the information in the query 
yrAll <- dbGetQuery(con, query)

## check data
str(yrAll)
head(yrAll)
tail(yrAll)

####-----------------------[ Data Pre-process ]-----------------------####

## scale variables
yrAll$Global_active_power <- round(yrAll$Global_active_power*1000/60, digits = 4)
yrAll$Global_reactive_power <- round(yrAll$Global_reactive_power*1000/60, digits = 4)
#total compsumption is Active power + Reactive power

## Combine Date and Time attribute values in a new attribute column
yrAll <-cbind(yrAll,paste(yrAll$Date,yrAll$Time), stringsAsFactors=FALSE)

## Give the new attribute in the 11th column a header name 
colnames(yrAll)[11] <-"DateTime"

## Move the DateTime attribute within the dataset
yrAll <- yrAll[,c(ncol(yrAll), 1:(ncol(yrAll)-1))]
head(yrAll)

## Convert DateTime from POSIXlt to POSIXct 
yrAll$DateTime <- as.POSIXct(yrAll$DateTime, "%Y/%m/%d %H:%M:%S")

## Add the time zone
attr(yrAll$DateTime, "tzone") <- "Europe/Paris"



####---------------------------- NA's -------------------------------####

sum(is.na(yrAll))

## calendar heat to see missing records 
source("http://blog.revolutionanalytics.com/downloads/calendarHeat.R")

calendarheat.NA <- calendarHeat(yrAll$Date, yrAll$Global_active_power)


## pad
pad.table <- pad(yrAll, interval = NULL, by = "DateTime", break_above = 3) #adding blank rows
sum(is.na(pad.table)) # check blank rows presence
pad.table$Date <- lubridate:: ymd(pad.table$Date)


## arrange df
pad.table <- arrange(pad.table, DateTime)

#### imputing values in missing values 
imputed_db <- na.interpolation(pad.table, option = "linear")
imputed_db$Date <- NULL
imputed_db$Time <- NULL
sum(is.na(imputed_db))


####-------------------------- Feature Engineering --------------------------------####

imputed_db$Submeterings <- imputed_db$Sub_metering_1+imputed_db$Sub_metering_2+imputed_db$Sub_metering_3
imputed_db$other_areas <- imputed_db$Global_active_power-imputed_db$Submeterings

imputed_db$year <- year(imputed_db$DateTime)
imputed_db$month <- month(imputed_db$DateTime)
imputed_db$week <- week(imputed_db$DateTime)
imputed_db$day <- day(imputed_db$DateTime)
imputed_db$hour <- hour(imputed_db$DateTime)
imputed_db$minute <- minute(imputed_db$DateTime)


# Rename and select variables 
imputed_db <- imputed_db %>% dplyr::select(-Voltage) %>% 
  dplyr::rename(ActiveEnergy = Global_active_power, ReactiveEnergy = Global_reactive_power, 
                Intensity = Global_intensity, Kitchen = Sub_metering_1, Laundry = Sub_metering_2, 
                W.A_HeatCold = Sub_metering_3)


# Season of the year
getSeason <- function(date) {
  WS <- as.Date("2008-12-21", format = "%Y-%m-%d") # Winter Solstice
  SE <- as.Date("2008-3-20",  format = "%Y-%m-%d") # Spring Equinox
  SS <- as.Date("2008-6-21",  format = "%Y-%m-%d") # Summer Solstice
  FE <- as.Date("2008-9-22",  format = "%Y-%m-%d") # Fall Equinox
  # Convert dates from any year to 2012 dates
  d <- as.Date(strftime(date, format="2008-%m-%d"))
  
  ifelse (d >= WS | d < SE, "Winter",
          ifelse (d >= SE & d < SS, "Spring",
                  ifelse (d >= SS & d < FE, "Summer", "Fall")))
}

#create a season column 
imputed_db$season <- getSeason(imputed_db$DateTime)


####-------------------------- Granularity --------------------------------####

#### seasonaly group ####
seasonaly <- imputed_db %>% group_by(season) %>%
  dplyr::summarize_at(vars(Kitchen,Laundry,W.A_HeatCold,
                           other_areas, ActiveEnergy, 
                           Submeterings, year, month, week, day), funs(sum))  #grouping by season



## seasonality
seasonality <- imputed_db %>% 
  group_by(date(DateTime)) %>% 
  summarise(mean = mean(ActiveEnergy)) %>%
  ggplot(aes(`date(DateTime)`, mean)) + 
  geom_line(color = "firebrick1") + 
  geom_smooth(se = F) + 
  labs(title = "Mean active energy consumed by day") + 
  ylab("Watt/h") + xlab("Time") + theme_light() 

ggplotly(seasonality) 
# It shows us a seasonality on the data, so the season has an influence in the energy consumption. 
# In winter there is a high consumption and in summer there is less consumption. 
# Important to consider that the season in our model

by_month <- imputed_db %>% 
  filter(year >= 2007) %>% 
  group_by(month) %>% 
  summarise(mean = mean(ActiveEnergy)) %>%
  ggplot(aes(x = month, mean)) + 
  geom_line(color = "firebrick1") + 
  geom_smooth(se = F) + 
  labs(title = "Mean active energy consumed by day") + 
  ylab("Watt/h") + xlab("Month") + theme_light() +
  scale_x_continuous()#ver como configurar esta funcion


#### other grouping ####

aggregated_df <- list()
plots.AE.sumsub <- list()


granularity <- c("year", "season", "month", "week", "day", "hour")

for(g in granularity){
  aggregated_df[[g]] <- imputed_db %>%
    group_by(DateTime=floor_date(DateTime, g)) %>%
    dplyr::summarize_at(vars(
      Kitchen,
      Laundry,
      W.A_HeatCold,
      ActiveEnergy, 
      other_areas,
      Submeterings),
      funs(sum))
  
  plots.AE.sumsub[[g]] <- ggplot(data = aggregated_df[[g]], aes(x = DateTime)) +
    geom_line(aes(y = Submeterings, color = "Submetering")) +
    geom_line(aes(y = ActiveEnergy, color = "Global Power")) +
    theme_minimal()+
    labs(title = paste("Global power vs submetering records", g),
         x = "Time",
         y = "Power")
}

plots.AE.sumsub[["year"]]
plots.AE.sumsub[["season"]]
plots.AE.sumsub[["month"]]
plots.AE.sumsub[["week"]]
plots.AE.sumsub[["day"]]
plots.AE.sumsub[["hour"]]


## Power Consumption per sub-meter monthly

PC_per_Submet_Montly <- plot_ly(aggregated_df[["month"]], 
        x = ~aggregated_df[["month"]]$DateTime, 
        y = ~aggregated_df[["month"]]$Kitchen, 
        name = 'Kitchen', type = 'scatter', mode = 'lines+markers') %>%
  add_trace(y = ~aggregated_df[["month"]]$Laundry,
            name = 'Laundry Room', mode = 'lines+markers') %>%
  add_trace(y = ~aggregated_df[["month"]]$W.A_HeatCold,
            name = 'Water Heater & AC', mode = 'lines+markers') %>%
  add_trace(y = ~aggregated_df[["month"]]$other_areas,
            name = 'Other Areas', mode = 'lines+markers') %>%
  layout(title = paste("Power Consumption per sub-meter monthly"),
         xaxis = list(title = "Time"),
         yaxis = list (title = "Power (watt-hours)"))

PC_per_Submet_Montly

####---------------------- vacation periods filter  ----------------------####
## 2007

vac_2007 <- filter(imputed_db, year== 2007, month == 8, day >= 1 & day <= 31)

plot_ly(vac_2007, x = ~vac_2007$DateTime, y = ~vac_2007$Kitchen, #plot week
        name = 'Kitchen', type = 'scatter', mode = 'lines') %>%
  add_trace(y = ~vac_2007$Laundry,
            name = 'Laundry Room', mode = 'lines') %>%
  add_trace(y = ~vac_2007$W.A_HeatCold,
            name = 'Water Heater & AC', mode = 'lines') %>%
  add_trace(y = ~vac_2007$other_areas,
            name = 'Other Areas', mode = 'lines') %>%
  layout(title = "Power Consumption per submeter in vacation 2007",
         xaxis = list(title = "Time"),
         yaxis = list (title = "Power (watt-hours)"))

## 2008

vac_2008 <- filter(imputed_db, year== 2008, month == 8, day >= 1 & day <= 31)

plot_ly(vac_2008, x = ~vac_2008$DateTime, y = ~vac_2008$Kitchen, #plot week
        name = 'Kitchen', type = 'scatter', mode = 'lines') %>%
  add_trace(y = ~vac_2008$Laundry,
            name = 'Laundry Room', mode = 'lines') %>%
  add_trace(y = ~vac_2008$W.A_HeatCold,
            name = 'Water Heater & AC', mode = 'lines') %>%
  add_trace(y = ~vac_2008$other_areas,
            name = 'Other Areas', mode = 'lines') %>%
  layout(title = "Power Consumption per submeter in vacation 2008",
         xaxis = list(title = "Time"),
         yaxis = list (title = "Power (watt-hours)"))



####-------------------[ Time series ]-------------------####


ts.year <- ts(aggregated_df[["year"]], start = c(2007,1), frequency = 1)
ts.month <- ts(aggregated_df[["month"]], start = c(2007,1), frequency = 12)
ts.week <- ts(aggregated_df[["week"]], start = c(2007,1), frequency = 52)
ts.day <- ts(aggregated_df[["day"]], start = c(2007,1), frequency = 365)
ts.hour <- ts(aggregated_df[["hour"]], start = c(2007,1), frequency = 8760)


## Create a time series por each variable in differents periods

vars <- list("Kitchen", "Laundry", "W.A_HeatCold", "ActiveEnergy", "other_areas")

ts.year.var <- list(vector(length = 5))
ts.month.var <- list(vector(length = 5))
ts.week.var <- list(vector(length = 5))
ts.day.var <- list(vector(length = 5))
ts.hour.var <- list(vector(length = 5))

for(i in vars){
  ts.year.var[[i]] <- ts.year[,i]
  ts.month.var[[i]] <- ts.month[,i]
  ts.week.var[[i]] <- ts.week[,i]
  ts.day.var[[i]] <- ts.day[,i]
  ts.hour.var[[i]] <- ts.hour[,i] 
}

## Decomposing time Series

decomp_ts.month.var <- list()
decomp_ts.week.var <- list()
decomp_ts.day.var <- list()

decomp.month.plot <- list()
decomp.week.plot <- list()
decomp.day.plot <- list()

## Decomposing by Loess

for (i in vars){
# By month
decomp_ts.month.var[[i]] <- ts.month.var[[i]] %>% stl(s.window = "periodic") 

decomp.month.plot[[i]] <- ts.month.var[[i]] %>% stl(s.window = "periodic") %>%
  autoplot() + xlab("Year") +
  ggtitle(paste("Monthly decomposition of", i))

# By week
decomp_ts.week.var[[i]] <- ts.week.var[[i]] %>% stl(s.window = "periodic") 

decomp.week.plot[[i]] <- ts.week.var[[i]] %>% stl(s.window = "periodic") %>% 
  autoplot() + xlab("Year") +
  ggtitle(paste("Weekly decomposition of", i)) 

# By day
decomp_ts.day.var[[i]] <- ts.day.var[[i]] %>% stl(s.window = "periodic") 

decomp.day.plot[[i]] <- ts.day.var[[i]] %>% stl(s.window = "periodic") %>% 
  autoplot() + xlab("Year") +
  ggtitle(paste("Daily decomposition of", i)) 
}

####------------------[ Forecasting Active Energy per Month ]---------------------####

## Data Partition

## create data partition from ts_month (2007-2010.11)
train.m <- window(ts.month[,"ActiveEnergy"], start = c(2007,1), end = c(2009,12))
test.m <- window(ts.month[,"ActiveEnergy"], start= c(2010,1))


## Model ARIMA

modArima.m <- auto.arima(train.m)
predArima.m <- forecast(modArima.m, h= 12)  # forecast 12 months ahead (start point train)
predArima.m
plot(predArima.m, col = "blue", fcol = "green")
accuracy(predArima.m, test.m)  #check metrics comparing prediction in test
summary(predArima.m)

plotpredArima.m <- autoplot(predArima.m, size = 3) + 
  ggtitle("Forecasting Arima by month")


## Model HoltWinters

modHoltW.m <- HoltWinters(train.m) 
predHoltW.m <- forecast(modHoltW.m, h= 12)
predHoltW.m
plot(predHoltW.m, col = "blue", fcol = "red")
accuracy(predHoltW.m, test.m)

##---------> Importante!!!  Arima bajar AICc, el ACF1 debe ser cercano a 0 

## Linear Model 

tslm.m <- tslm(ts.month[,4] ~ trend + season) #linear model in Active Energy [ ,4] of ts.month 
summary(tslm.m)
pred.tslm.m <- forecast(tslm.m, h=12)
plot(pred.tslm.m)


# Seasonal Naive 

modSnaive.m <- snaive(train.m)
predSnaive.m <- forecast(modSnaive.m, h = 12, level = c(80, 95))
plot(predSnaive.m)
accuracy(predSnaive.m, test.m)

## Comparisons

## comparing Arima to Real by month
compare.arima.real <- autoplot(ts.month[,"ActiveEnergy"], series = "real", size = 2) + 
  autolayer(predArima.m, series= "ARIMA prediction", PI= FALSE, size = 2) +
  ggtitle("Comparison ARIMA prediction vs Real")

## comparing Holt Winters to Real by month
compare.HoltW.real <- autoplot(ts.month[,"ActiveEnergy"], series = "real") + 
  autolayer(predHoltW.m, series= "Holt Winters prediction", PI= FALSE) +
  ggtitle("Comparison Holt Winters prediction vs Real")

####------------------[Forecasting Active Energy per Week]---------------------####

## Data Partition

## create data partition from ts_week (2007-2010.11)
train.w <- window(ts.week[,"ActiveEnergy"], start = c(2007,1), end = c(2009,12))
test.w <- window(ts.week[,"ActiveEnergy"], start= c(2010,1))

## Model ARIMA

modArima.w <- auto.arima(train.w)
predArima.w <- forecast(modArima.w, h= 60)  # forecast 60 weeks ahead (start point train)
predArima.w
plot(predArima.w, col = "blue", fcol = "green")
accuracy(predArima.w, test.w)  #check metrics comparing prediction in test
summary(predArima.w)

plotpredArima.w <- autoplot(predArima.w, size = 3) + 
  ggtitle("Forecasting Arima weekly")


## Model HoltWinters

modHoltW.w <- HoltWinters(train.w) 
predHoltW.w <- forecast(modHoltW.w, h= 60)
predHoltW.w
plot(predHoltW.w, col = "blue", fcol = "red")
accuracy(predHoltW.w, test.w)
summary(predHoltW.w)


## Linear Model 

tslm.w <- tslm(ts.week[ ,4] ~ trend + season)
summary(tslm.w)
pred.tslm.w <- forecast(tslm.w, h=60, level=c(80,90))
plot(pred.tslm.w)

# Seasonal Naive 

modSnaive.w <- snaive(train.w)
predSnaive.w <- forecast(modSnaive.w, h = 60, level = c(80, 95))
plot(predSnaive.w)


## comparing Arima to Real
autoplot(ts.week[,"ActiveEnergy"], series = "real") + 
  autolayer(predArima.w, series= "ARIMA prediction", PI= FALSE)

## comparing Holt Winters to Real
autoplot(ts.week[,"ActiveEnergy"], series = "real") + 
  autolayer(predHoltW.w, series= "Holt Winters prediction", PI= FALSE)


####Comparing ARIMA, Holt Winters and Seasonal Naive ####

#by month

modCompare.m <- autoplot(ts.month[,"ActiveEnergy"], series = "real", size = 1) + 
  autolayer(predArima.m, series= "ARIMA prediction", PI=FALSE, size = 2) +
  autolayer(predSnaive.m, series= "Seasonal Naive prediction", PI= FALSE, size = 2) +
  autolayer(predHoltW.m, series= "Holt Winters prediction", PI= FALSE, size = 2) +
  scale_linetype_manual(labels = c("real", "ARIMA prediction", "Seasonal Naive prediction", "Holt Winters prediction"),
                        values = c(1, 4, 4, 4)) +
  scale_size_manual(labels = c("real", "ARIMA prediction", "Seasonal Naive prediction", "Holt Winters prediction"),
                    values = c(1, 4, 4, 4)) +
  ggtitle("Compare Predictions ARIMA vs Holt Winters vs S Naive by month")

#by week

modCompare.w <- autoplot(ts.week[,"ActiveEnergy"], series = "real", size = 1) + 
  autolayer(predArima.w, series= "ARIMA prediction", PI=FALSE, size = 2) +
  autolayer(predSnaive.w, series= "Seasonal Naive prediction", PI= FALSE, size = 2) +
  autolayer(predHoltW.w, series= "Holt Winters prediction", PI= FALSE, size = 2) +
  scale_linetype_manual(labels = c("real", "ARIMA prediction", "Seasonal Naive prediction", "Holt Winters prediction"),
                        values = c(1, 4, 4, 4)) +
  scale_size_manual(labels = c("real", "ARIMA prediction", "Seasonal Naive prediction", "Holt Winters prediction"),
                    values = c(1, 4, 4, 4)) +
  ggtitle("Compare Predictions ARIMA vs Holt Winters vs S Naive by week")











