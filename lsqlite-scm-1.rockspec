package = "lsqlite"
version = "scm-1"
source = {
	url = "git://github.com/katlogic/lsqlite.git";
	branch = "master";
}

description = {
	summary = "simple sqlite3 binding";
	detailed = "This library provides somewhat weird sqlite binding "..
		   " designed in a way to make caller code more compact.";
	homepage = "http://github.com/katlogic/lsqite";
	license = "MIT";
}

dependencies = {
	"lua >= 5.1"
}

build = {
	type = "builtin";
	modules = {
		lsqlite = {
			sources = { "lsqlite.c" };
			libraries = { "sqlite3" };
		}
	}
}
