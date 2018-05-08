//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scanner.cpp
//
// Identification: src/codegen/util/csv_scanner.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/csv_scanner.h"

#include <boost/filesystem.hpp>

#include "common/exception.h"
#include "executor/executor_context.h"
#include "type/abstract_pool.h"
#include "util/string_util.h"

namespace peloton {
namespace codegen {
namespace util {

CSVScanner::CSVScanner(peloton::type::AbstractPool &pool,
                       const std::string &file_path,
                       const codegen::type::Type *col_types, uint32_t num_cols,
                       CSVScanner::Callback func, void *opaque_state,
                       char delimiter, char quote, char escape)
    : memory_(pool),
      file_path_(file_path),
      file_(),
      buffer_(nullptr),
      buffer_begin_(0),
      buffer_end_(0),
      line_(nullptr),
      line_len_(0),
      line_maxlen_(0),
      delimiter_(delimiter),
      quote_(quote),
      escape_(escape),
      func_(func),
      opaque_state_(opaque_state) {
  // Make column array
  cols_ = static_cast<CSVScanner::Column *>(
      memory_.Allocate(sizeof(CSVScanner::Column) * num_cols));

  // Initialize the columns
  for (uint32_t i = 0; i < num_cols; i++) {
    cols_[i].col_type = col_types[i];
    cols_[i].ptr = nullptr;
    cols_[i].len = 0;
    cols_[i].is_null = false;
  }
}

CSVScanner::~CSVScanner() {
  if (buffer_ != nullptr) {
    memory_.Free(buffer_);
  }
  if (line_ != nullptr) {
    memory_.Free(line_);
  }
  if (cols_ != nullptr) {
    memory_.Free(cols_);
  }
}

void CSVScanner::Init(CSVScanner &scanner,
                      executor::ExecutorContext &executor_context,
                      const char *file_path,
                      const codegen::type::Type *col_types, uint32_t num_cols,
                      CSVScanner::Callback func, void *opaque_state,
                      char delimiter, char quote, char escape) {
  // Forward to constructor
  new (&scanner)
      CSVScanner(*executor_context.GetPool(), file_path, col_types, num_cols,
                 func, opaque_state, delimiter, quote, escape);
}

void CSVScanner::Destroy(CSVScanner &scanner) {
  // Forward to destructor
  scanner.~CSVScanner();
}

void CSVScanner::Produce() {
  // Initialize
  Initialize();

  // Loop lines
  while (const char *line = NextLine()) {
    ProduceCSV(line);
  }
}

void CSVScanner::Initialize() {
  // Let's first perform a few validity checks
  boost::filesystem::path path{file_path_};

  if (!boost::filesystem::exists(path)) {
    throw ExecutorException{StringUtil::Format("input path '%s' does not exist",
                                               file_path_.c_str())};
  } else if (!boost::filesystem::is_regular_file(file_path_)) {
    throw ExecutorException{
        StringUtil::Format("unable to read file '%s'", file_path_.c_str())};
  }

  // The path looks okay, let's try opening it
  file_.Open(file_path_, peloton::util::File::AccessMode::ReadOnly);

  // Allocate buffer space
  buffer_ = static_cast<char *>(memory_.Allocate(kDefaultBufferSize + 1));

  // Fill read-buffer
  NextBuffer();

  // Allocate space for the full line, if it doesn't fit into the buffer
  line_ = static_cast<char *>(memory_.Allocate(kDefaultBufferSize));
  line_len_ = 0;
  line_maxlen_ = kDefaultBufferSize;
}

bool CSVScanner::NextBuffer() {
  // Do read
  buffer_begin_ = 0;
  buffer_end_ = static_cast<uint32_t>(file_.Read(buffer_, kDefaultBufferSize));

  // Update stats
  stats_.num_reads++;

  return (buffer_end_ != 0);
}

void CSVScanner::AppendToCurrentLine(const char *data, uint32_t len) {
  // Short-circuit if we're not appending any data
  if (len == 0) {
    return;
  }

  if (line_len_ + len > line_maxlen_) {
    // The current line buffer isn't large enough to store the new bytes, so we
    // need to resize it. By default, we double the capacity.
    auto new_maxlen = line_maxlen_ * 2;
    while (new_maxlen < len) {
      new_maxlen *= 2;
    }
    auto *new_line = static_cast<char *>(memory_.Allocate(new_maxlen));

    // Copy the old data
    PELOTON_MEMCPY(new_line, line_, line_len_);

    // Setup pointers and sizes
    line_ = new_line;
    line_maxlen_ = new_maxlen;

    stats_.num_reallocs++;
  }

  // At this point, we've guaranteed that the line is large enough to
  // accommodate the new bytes, so let's go ahead and perform the copy.

  PELOTON_MEMCPY(line_ + line_len_, data, len);

  // Increase the length of the line
  line_len_ += len;

  // Track copy stats
  stats_.num_copies++;
}

// The main purpose of this function is to find the start of the next line in
// the CSV file.
const char *CSVScanner::NextLine() {
  line_len_ = 0;

  bool in_quote = false;
  bool last_was_escape = false;
  bool copied_to_line_buf = false;

  uint32_t line_end = buffer_begin_;

  while (true) {
    if (line_end >= buffer_end_) {
      // We need to read more data from the CSV file. But first, we need to copy
      // all the data in the read-buffer (i.e., [buffer_begin_, buffer_end_] to
      // the line-buffer.

      AppendToCurrentLine(buffer_ + buffer_begin_,
                          static_cast<uint32_t>(buffer_end_ - buffer_begin_));

      // Now, read more data
      if (!NextBuffer()) {
        return nullptr;
      }

      // Reset positions
      line_end = buffer_begin_;
      copied_to_line_buf = true;
    }

    // Read character
    char c = buffer_[line_end];

    if (in_quote && c == escape_) {
      last_was_escape = true;
    }
    if (c == quote_ && !last_was_escape) {
      in_quote = true;
    }
    if (c != escape_) {
      last_was_escape = false;
    }

    // Process the new-line character. If we a new-line and we're not currently
    // in a quoted section, we're done.
    if (c == '\n' && !in_quote) {
      buffer_[line_end] = '\0';
      break;
    }

    // Move along
    line_end++;
  }

  // Increment line number
  line_number_++;

  if (copied_to_line_buf) {
    AppendToCurrentLine(buffer_, line_end);
    buffer_begin_ = line_end + 1;
    return line_;
  } else {
    const char *ret = buffer_ + buffer_begin_;
    buffer_begin_ = line_end + 1;
    return ret;
  }
}

void CSVScanner::ProduceCSV(UNUSED_ATTRIBUTE const char *line) {
  // TODO: me
  func_(opaque_state_);
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton