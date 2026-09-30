#include <cstdlib>
#include <iostream>
#include <string_view>

#include "order_book_engine/version.hpp"

namespace {

int failures = 0;

void expect(bool ok, std::string_view what) {
  if (!ok) {
    ++failures;
    std::cerr << "断言失败: " << what << '\n';
  }
}

}  // namespace

int main() {
  expect(!order_book_engine::version().empty(), "version() 非空");
  expect(order_book_engine::version() == order_book_engine::kVersion, "version() 与 kVersion 一致");
  if (failures != 0) {
    std::cerr << failures << " 项断言失败\n";
    return 1;
  }
  std::cout << "全部断言通过\n";
  return 0;
}
