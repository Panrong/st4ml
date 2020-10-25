import json

config = {}
config["hadoopHome"] = "/usr/lib/hadoop-3.2.1"
config["master"] = "spark"
config["jarpackage"] = "../target/scala-2.12/map-matching_2.12-1.0.jar"
config["mmtrajfile"] = "/datasets/mm100000.csv"
config["mapfile"] = "../preprocessing/porto.csv"
config["sparkmaster"] = "spark://Master:7077"
config["executor-memory"] = "3500M"
config["total-executor-cores"] = "8"
config["executor-cores"] = "2"
config["numpartition"] = config["total-executor-cores"]
config["query"] = "../datasets/odqueries.txt"
config["resultsdir"] = "/datasets/tmpodres"
json.dump(config, open("odconfig-example.json", "w"))