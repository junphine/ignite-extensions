::
:: Grid mount command line loader.
::

@echo off

if "%OS%" == "Windows_NT" setlocal

::set JVM_OPTS=-agentlib:jdwp=transport=dt_socket,address=0.0.0.0:19007,server=y,suspend=n
set JAVA_HOME=D:\IDE\IDEA\jbr
::set PATH=C:\Program Files\Java\jdk-11\bin;%PATH%

::
:: Set router service environment.
::
set DEFAULT_CONFIG=-c config/core-site.ignite.xml -m /Users/igfs -r file:///tmp/
set MAIN_CLASS=hcfsfuse.fuse.HCFSFuse
SET work=%CD%

IF [%work:~-3%]==[bin] CD ..
::
:: Start router service.
::
call "bin\ignite.bat" %*
