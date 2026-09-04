#ifndef WINDFLOW_TABLE_API_HPP
#define WINDFLOW_TABLE_API_HPP

// ============================================================================
// Standard Library Dependencies
// ============================================================================
#include <iostream>
#include <string>
#include <vector>
#include <cstdint>
#include <functional>
#include <limits>
#include <sstream>
#include <fstream>
#include <chrono>
#include <algorithm>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <cassert>

// ============================================================================
// WindFlow Core Runtime
// ============================================================================
#include <windflow.hpp>

// ============================================================================
// Tuple Tagging & Meta-Types
// ============================================================================
#include "tagged_tuple.hpp"

// ============================================================================
// Single-Stream Relational Operators (Map, Filter, Deduplication)
// ============================================================================
#include "select_builder.hpp"
#include "where_builder.hpp"
#include "distinct_builder.hpp"

// ============================================================================
// Aggregations (In-Stream Changelog & Tumbling/Sliding Windows)
// ============================================================================
#include "global_group_builder.hpp"
#include "windowed_group_builder.hpp"

// ============================================================================
// Multi-Stream Binary Operators (Joins & Set Operations)
// ============================================================================
#include "table_interval_join_builder.hpp"
#include "table_window_join_builder.hpp"
#include "intersect_builder.hpp"
#include "intersect_all_builder.hpp"

// ============================================================================
// Physical I/O & Connectors (CSV Parsers, Formatters, Watermarking)
// ============================================================================
#include "table_source_builder.hpp"
#include "table_sink_builder.hpp"

#endif // WINDFLOW_TABLE_API_HPP