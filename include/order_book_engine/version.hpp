#pragma once

#include <string_view>

namespace order_book_engine {

/// 当前版本号，与 CMake 工程版本一致。
inline constexpr std::string_view kVersion = "0.1.0";

/// 返回当前版本号。
[[nodiscard]] std::string_view version() noexcept;

}  // namespace order_book_engine
