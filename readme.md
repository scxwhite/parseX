主要是一个sql解析的小工具
可以搭配[分布式任务调度系统Hera](https://github.com/scxwhite/hera)或者其它调度使用。解析hive、spark sql的输入、输出表。达到自动依赖任务的目的
直接使用[SqlParseUtil](https://github.com/scxwhite/parseX/blob/master/parsex-core/src/main/java/com/sucx/core/SqlParseUtil.java) 类中的静态方法调用
