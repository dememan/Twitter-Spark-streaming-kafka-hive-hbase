#!/bin/sh

sudo service hbase-master restart
sudo service hbase-regionserver restart

clean=false

while getopts 'c' opt; do
	case $opt in
		c) clean=true;;
		*) echo "Error unknown arg!" exit 1
	esac
done

echo configuring hbase ...
if "$clean"; 
	then if [[ $(echo "exists 'tweets'" | hbase shell | grep 'does not exist') ]];
		then echo "create 'tweets', 'tweet-info', 'general-info'" | hbase shell;
	else
		echo "delete ... 'tweets' "
		echo "disable .... 'tweets'" | hbase shell
		echo "drop .... 'tweets'" | hbase shell
		echo "create 'tweets', 'tweet-info', 'general-info'" | hbase shell
	fi
else
	if [[ $(echo "exists 'tweets'" | hbase shell | grep 'does not exist') ]];
		then echo "create 'tweets', 'tweet-info', 'general-info'" | hbase shell;
	else 
		echo "'tweets' already exists"
	fi
fi

echo configuring hive ...
if "$clean";
	then if [[ $(echo "show tables like 'tweets'" | hive | grep 'tweets') ]];
		then echo "deleting old table"
		hive -e "drop table tweets"
		hive -f hive.hql;
	else
		hive -f hive.hql;
	fi
else
	hive -f hive.hql;
fi 






