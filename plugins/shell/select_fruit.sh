FRUIT=$1
if [ $FRUIT == APPLE ];then
	echo "You selected Apple"
elif [ $FRUIT == ORANGE ];then
	echo "You selected orange"
elif [ $FRUIT == GRAPE ];then
	echo "you selected grape"
else 
	echo "you selected other fruit"
fi
