17/07/26 21:42:32 WARN HttpChannel: /jobs/
java.lang.NoSuchMethodError: javax.servlet.http.HttpServletRequest.isAsyncStarted()Z
	at org.spark_project.jetty.servlets.gzip.GzipHandler.handle(GzipHandler.java:484)
	at org.spark_project.jetty.server.handler.ContextHandlerCollection.handle(ContextHandlerCollection.java:215)
	at org.spark_project.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:97)
	at org.spark_project.jetty.server.Server.handle(Server.java:499)
	at org.spark_project.jetty.server.HttpChannel.handle(HttpChannel.java:311)
	at org.spark_project.jetty.server.HttpConnection.onFillable(HttpConnection.java:257)
	at org.spark_project.jetty.io.AbstractConnection$2.run(AbstractConnection.java:544)
	at org.spark_project.jetty.util.thread.QueuedThreadPool.runJob(QueuedThreadPool.java:635)



	http://blog.csdn.net/ainidong2005/article/details/53088957

	http://blog.csdn.net/qq_27093465/article/details/69226949