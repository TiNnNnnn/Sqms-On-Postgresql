#include "gtest/gtest.h"
#include <filesystem>
#include <stdlib.h>
#include <sys/stat.h>
#include <type_traits>
#include <unistd.h>

class BaseTest : public ::testing::Test {
public:
    BaseTest() {
    }
    ~BaseTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
private:
    
};