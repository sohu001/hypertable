/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_LOADDATASOURCEFILELOCAL_H
#define HYPERTABLE_LOADDATASOURCEFILELOCAL_H

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"
#include "Common/String.h"

#include "DataSource.h"
#include "FixedRandomStringGenerator.h"
#include "LoadDataSource.h"


namespace Hypertable {

  class LoadDataSourceFileLocal : public LoadDataSource {

  public:
    LoadDataSourceFileLocal(const String &fname, const String &header_fname,
                            int row_uniquify_chars = 0, int load_flags = 0);

    ~LoadDataSourceFileLocal() { };

    uint64_t incr_consumed();

  protected:
    void init_src();
    boost::iostreams::file_source m_source;
    String m_fname;
    String m_header_fname;
  };

} // namespace Hypertable

#endif // HYPERTABLE_LOADDATASOURCEFILELOCAL_H
