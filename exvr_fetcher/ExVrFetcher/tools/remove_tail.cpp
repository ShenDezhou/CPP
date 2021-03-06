/*
Copyright (c) 2017-2018 Dezhou Shen, Sogou Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
// file-encoding:utf-8

#include <assert.h>
#include <stdio.h>

#include <string>
#include <bitset>

inline size_t trunc_cn_utf8(std::string &utf8_str, size_t trunc_remain_size) {
  int has_trunc = 0;
  if (utf8_str.size() < trunc_remain_size) {
    goto trunc_done;
  }
  for (size_t i = trunc_remain_size - 2; i < trunc_remain_size; ++i) {
    std::bitset<8> ch_bitset(utf8_str[i]);
    if (!ch_bitset.test(7))
      continue;
    if (ch_bitset.test(7) && ch_bitset.test(6) && ch_bitset.test(5)) {
      utf8_str.erase(utf8_str.begin() + i, utf8_str.end());
      has_trunc = 1;
      break;
    }
  }

  if (!has_trunc) {
    utf8_str.erase(utf8_str.begin() + trunc_remain_size, utf8_str.end());
  }
trunc_done:
  return utf8_str.size();
}


inline size_t remove_tail(std::string &long_field, size_t trunc_remain_size) {
  if (long_field.size() > trunc_remain_size) {
    long_field.erase(long_field.begin() + trunc_remain_size, long_field.end());
    size_t semicolon = long_field.find_last_of(";");
    if (semicolon == std::string::npos) {
      semicolon = long_field.rfind("\xEF\xBC\x9B"/*"；"*/);
    }
    if (semicolon != std::string::npos) {
      long_field.erase(long_field.begin() + semicolon, long_field.end());
    } else {
      trunc_cn_utf8(long_field, trunc_remain_size);
    }
  }
  return long_field.size();
}

int main(int argc, char const *argv[]) {
  std::string optword("康宁心煮艺 2012-康宁心煮艺;康宁心煮艺-2012");

  assert(trunc_cn_utf8(optword, 40) == 40);
  optword = "康宁心煮艺 2012-康宁心煮艺;康宁心煮艺-2012";
  assert(trunc_cn_utf8(optword, 39) == 37);
  optword = "康宁心煮艺 2012-康宁心煮艺;康宁心煮艺-2012";
  assert(trunc_cn_utf8(optword, 38) == 37);
  optword = "康宁心煮艺 2012-康宁心煮艺;康宁心煮艺-2012";
  assert(trunc_cn_utf8(optword, 37) == 37);
  optword = "康宁心煮艺 2012-康宁心煮艺;康宁心煮艺-2012";
  assert(remove_tail(optword, 40) == 36);
  optword = "康宁心煮艺 2012-康宁心煮艺:康宁心煮艺-2012";
  assert(remove_tail(optword, 39) == 37);
  optword = "康宁心煮艺 2012-康宁心煮艺；康宁心煮艺-2012";
  assert(remove_tail(optword, 40) == 36);
  optword = "康宁心煮艺 2012-康宁心煮艺；康宁心煮艺-2012";
  assert(remove_tail(optword, 39) == 36);
  optword = "康宁心煮艺 2012-康宁心煮艺；康宁心煮艺-2012";
  assert(remove_tail(optword, 38) == 36);
  optword = "康宁心煮艺 2012-康宁心煮艺；康宁心煮艺-2012";
  assert(remove_tail(optword, 37) == 36);
  optword = ";康宁心煮艺 2012-康宁心煮艺康宁心煮艺-2012";
  assert(remove_tail(optword, 37) == 0);
  optword = "；康宁心煮艺 2012-康宁心煮艺康宁心煮艺-2012";
  assert(remove_tail(optword, 37) == 0);
  return 0;
}
