# DistributedFileSystem
### Instructions/Notes
#### Download the project and using command line navigate to the folder of the project containing the 'build.gradle' file. Run the below command 
```
gradle build
```

#### Once the build completes, an executable JAR will be placed in the '\build\libs' folder by the name - 'FileSystem.jar'
#### Using command line, navigate to this folder and run the below commands
##### To start a Master (Only once)
```
java -jar FileSystem.jar Master
```

##### To start a Node (To add additional nodes)
```
java -jar FileSystem.jar Node
```

#### Once the nodes start, a 'LocalFiles' and 'DFSFileSystem' Folder is created. 
#### Once the DFS is running, you can execute the following commands to store/retrieve the files
##### Put
```
put localfilename fs513filename
```
```
E.g. put enwiki.xml testwiki.xml
```

##### Get
```
get fs513filename localfilename
```
```
E.g. get testwiki.xml enwiki.xml
```

##### Remove
```
remove fs513filename
```
```
E.g. remove testwiki.xml
````

##### ls 
To list all files
```
ls
```

##### Locate a file
To locate the nodes where a file is stored
```
locate fs513filename
```
```
E.g. Locate testwiki.xml
```

##### Files stored locally
```
lshere 
```

##### Print the Membership list on any node
```
ml 
```

