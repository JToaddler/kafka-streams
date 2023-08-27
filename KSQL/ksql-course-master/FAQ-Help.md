# Latest course notes

The ksqlDB platform and KSQL syntax is a rapidly developing project. New features and capabilities are frequently added to the ksqlDB project and the Confluent Platform

Make sure you are using the latest course notes. The course project files are updated more recently updated to work with the latest Confluent Platform. Sometimes the project "README" will have _newer_ content then the video (it takes longer to update a video then a project file). If you downloaded the project files a while ago, you may have the "old" readme.

Please follow the instructions at https://courses.datacumulus.com/downloads/kafka-ksql-na2.html to download the code with newest instructions.

# Error : "Error: Schema Registry failed to start"

This sometimes occurs - especially if you've recently restarted your machine. The easiest way to move forwards is to "reset" your environment and try restarting. This process will destroy any data you may have in existing topics

To reset the Confluent platform

```
confluent local destroy
```

And then start as normal

```
confluent local services ksql-server start
```

# Error : "Failed to start" - Windows 10 / WSL2

If you are using WIndows 10, and are having trouble starting ksqlDB, firstly try reset the Confluent platform (using `confluent local destroy` as described above)

If this fails to work, try a restart of WSL2 (Windows subsystem Linux) in Windows 10 by

- Right-click on the Windows 10 Start button
- Select the Windows PowerShell (Admin)
- Type the following into the PowerShell window and press the Enter

```
Get-Service LxssManager | Restart-Service
```

- And then start as normal 

```
confluent local services ksql-server start
```

# Checking the logs

You can review the CP logs for a given service with the confluent log command. For example, to see the Zookeeper logs

```
confluent local services zookeeper log -f
```


# Headless: KSQL is not configured to use a schema registry. 

The the lesson "Moving to Productions-Headless for KSQL" you may hit an error like this ...

```
ERROR Failed to start KSQL (io.confluent.ksql.rest.server.KsqlServerMain:68)
io.confluent.ksql.util.KsqlStatementException: Schema registry fetch for topic value request failed. Topic: riderequest-europe
Caused by: KSQL is not configured to use a schema registry. To enable it, please set ksql.schema.registry.url
```

This is due to a missing setting with the supplied `ksql-server.properties` file

Edit `/opt/confluent/etc/ksqldb/ksql-server.properties`; and uncomment the final line to enable KSQL's integration to the Confluent Schema Registry:
```
ksql.schema.registry.url=http://localhost:8081
```

# How do you display multiple split terminals at the same time?

Sometimes you want a more powerful command line terminal manager - especially if you wish to "split" the window

- On a Mac - look at iTerm 2
- For Windows 10 - consider Microsoft Windows Terminal or ConEmu