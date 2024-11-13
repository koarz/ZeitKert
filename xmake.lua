add_rules("mode.debug", "mode.release")
add_rules("plugin.compile_commands.autoupdate", {outputdir = ".vscode"})
set_languages("c++23")
set_project("ZeitgeistDB")
set_version("0.1")
add_includedirs("src/include")
set_toolchains("clang")

add_requires("linenoise", "simdjson", "rapidjson", "gtest", "fmt")

target("ZeitgeistDB")
    set_kind("binary")
    add_files("src/**.cpp")
    add_packages("linenoise", "simdjson", "rapidjson", "fmt")

target("tests")
    set_kind("binary")
    add_files("src/**.cpp|main.cpp")
    add_files("tests/**.cpp")
    add_syslinks("asan")
    add_cxxflags("-fsanitize=address", "-fno-omit-frame-pointer")
    add_packages("linenoise", "simdjson", "rapidjson", "gtest", "fmt")

target("bpm-bench")
    set_kind("binary")
    add_files("benchmark/bpm-bench.cpp", "src/**.cpp|main.cpp")
    add_packages("linenoise", "simdjson", "rapidjson", "fmt")

set_default("ZeitgeistDB")