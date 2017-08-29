#!/bin/bash
Red='\033[0;31m'
Green='\033[0;32m'
NC='\033[0m' # No Color

counter=0;
ok=0

testeld(){
	inputVertex=$1
	inputEdge=$2
	query=$3
	expected=$4
	testName=$5
	((counter++))
	echo "------------------------"
	echo "TEST $testName"
	echo "Query: $query" 
	spark-submit --class Main --driver-memory 4g --executor-memory 4g  --master spark://sparkql-ubuntu:7077 --num-executors 1 /home/sparkuser/PSparkql/target/scala-2.10/sparkql_2.10-1.0.jar "$inputVertex" "$inputEdge" "$query" 1>result.log 2>error.log

	pid=$(echo $!)
	wait $pid
	echo "sleep 45"
	sleep 45
	echo "sleep END"
	count=""
	while [ -z "$count" ]; do
		count=$(grep "RESULT" result.log | cut -d" " -f2)
		echo "sleep 10"
		sleep 10
	done
	if [ $count -eq $expected ] 
	then
		printf "$testName: [${Green}OK${NC}] (expected: $expected)\n"
		((ok++))
	else 
		printf "$testName: [${Red}NOK${NC}]: got: $count instead of $expected\n"
	fi
	echo "========================"
}



inputVertex="/home/sparkuser/PSparkql/testelo/LUBM_1000.n3"
#inputVertex="/home/sparkuser/PSparkql/testelo/LUBM_TEST.n3"
#inputVertex="/home/sparkuser/PSparkql/testelo/LUBM_AP9.n3"
inputEdge="/home/sparkuser/PSparkql/testelo/LUBM_1000.n3"

#LUBM 1
query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . ?X <http://spark.elte.hu#takesCourse> <http://www.Department0.University1000.edu/GraduateCourse0>"
expected=3
#testeld "$inputVertex" "$inputEdge" "$query" "$expected" "LUBM 1"

#LUBM 2
query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#University> . ?Z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Department> . ?X <http://spark.elte.hu#memberOf> ?Z . ?Z <http://spark.elte.hu#subOrganizationOf> ?Y . ?X <http://spark.elte.hu#undergraduateDegreeFrom> ?Y "
expected=4
#testeld "$inputVertex" "$inputEdge" "$query" "$expected" "LUBM 2"

#LUBM 3
query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Publication> . ?X <http://spark.elte.hu#publicationAuthor> <http://www.Department0.University1000.edu/AssistantProfessor0>"
expected=8
#testeld "$inputVertex" "$inputEdge" "$query" "$expected" "LUBM 3"

#LUBM 4
query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Professor> . ?X <http://spark.elte.hu#worksFor> <http://www.Department0.University1000.edu> . ?X <http://spark.elte.hu#name> ?Y1 . ?X <http://spark.elte.hu#emailAddress> ?Y2 . ?X <http://spark.elte.hu#telephone> ?Y3"
expected=27
#testeld "$inputVertex" "$inputEdge" "$query" "$expected" "LUBM 4"

#LUBM 5
query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Person> . ?X <http://spark.elte.hu#memberOf> <http://www.Department0.University1000.edu>"
expected=373
#testeld "$inputVertex" "$inputEdge" "$query" "$expected" "LUBM 5"

#LUBM 6
query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student>"
expected=23734
testeld "$inputVertex" "$inputEdge" "$query" "$expected" "LUBM 6"

#LUBM 7
query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Course> . ?X <http://spark.elte.hu#takesCourse> ?Y . <http://www.Department0.University1000.edu/AssociateProfessor0> <http://spark.elte.hu#teacherOf> ?Y"
expected=52
testeld "$inputVertex" "$inputEdge" "$query" "$expected" "LUBM 7"

#LUBM 8
query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Department> . ?X <http://spark.elte.hu#memberOf> ?Y . ?Y <http://spark.elte.hu#subOrganizationOf> <http://www.University1000.edu> . ?X <http://spark.elte.hu#emailAddress> ?Z"
expected=12725
testeld "$inputVertex" "$inputEdge" "$query" "$expected" "LUBM 8"


#LUBM 9
query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Faculty> . ?Z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Course> . ?X <http://spark.elte.hu#advisor> ?Y . ?Y <http://spark.elte.hu#teacherOf> ?Z . ?X <http://spark.elte.hu#takesCourse> ?Z"
expected=310
testeld "$inputVertex" "$inputEdge" "$query" "$expected" "LUBM 9"


#LUBM 10
query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?X <http://spark.elte.hu#takesCourse> <http://www.Department0.University1000.edu/GraduateCourse0>"
expected=3
testeld "$inputVertex" "$inputEdge" "$query" "$expected" "LUBM 10"

echo "Statistics: $counter \ $ok "
