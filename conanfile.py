from conan import ConanFile
from conan.tools.cmake import CMake
from conan.tools.cmake.layout import cmake_layout

class anonymizer(ConanFile):
    settings = "os", "build_type", "compiler", "arch"
    generators = "CMakeToolchain", "CMakeDeps"

    def requirements(self):
        self.requires("librdkafka/2.8.0")
        self.requires("capnproto/1.1.0")
        self.requires("nlohmann_json/3.12.0")

    def layout(self):
        cmake_layout(self)

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()