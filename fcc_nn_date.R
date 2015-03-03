library(ggplot2)
library(scales)
nn_dates <- read.csv("~/Documents/Regulations/regulations/nn_dates.csv", header=FALSE, stringsAsFactors=FALSE)
colnames(nn_dates) <- c("datestr", "class", "count")
nn_dates$date <- as.Date(nn_dates$datestr, "%Y-%m-%d")
# drop datestr
nn_dates <- nn_dates[,2:4]
# Show us where we don't have dates
nn_dates[is.na(nn_dates$date),]
# Missing 7954 dates total
sum(nn_dates[is.na(nn_dates$date),"count"])
# remove these; note that this is NOT removing "na" as a class
nn_dates <- nn_dates[!is.na(nn_dates$date),]
# remove clearly mistaken date 0111-02-04
nn_dates <- nn_dates[-1,]
# Also remove dates in 2015
nn_dates <- nn_dates[! nn_dates$date >= "2015-01-01",]
date_totals <- aggregate(nn_dates$count, by=list(nn_dates$date), FUN=sum)
colnames(date_totals) <- c("date", "count")
date_totals$class = "t"
# Let's provide an indicator of volume, but transformed and scaled from 0 to 1 so it can be plotted along with the proportions
date_totals$transformed <- scale(sqrt(date_totals$count), center=FALSE, scale=sqrt(max(date_totals$count)))
ggplot(nn_dates, aes(x=date, y=count, fill=class)) + 
  geom_bar(position="fill", stat="identity") + scale_fill_manual(values=c("#66c2a5", "#fc8d62", "#8da0cb", "#000000"), labels=c("Unclassifiable", "Oppose", "Support", "Volume")) + 
  geom_line(data=date_totals,  aes(x=date, y=transformed, fill="Square Root of Volume")) +
  scale_x_date(labels=date_format("%b-%d-%y")) +
  labs(x = "Date", y = "Proportion", fill="") + theme(legend.position="bottom")
ggsave(filename="~/Documents/Regulations/article/nn_dates.pdf", width=9, height=5)
