file(REAL_PATH "${termium_java_DIR}/../../../java_home" RESOLVED_TERMIUM_JAVA_HOME)
set(JAVA_HOME "${RESOLVED_TERMIUM_JAVA_HOME}" CACHE INTERNAL "JAVA_HOME")

message(WARNING "Forcing JAVA_HOME to ${JAVA_HOME} for Termium Java Install")