valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --log-file="val.txt"  ./server -p 9090 -o ./Extras/logs.txt -l 4 -d Extras/table2.csv
