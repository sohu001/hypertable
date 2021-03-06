/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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

#ifndef HYPERTABLE_APPLICATIONHANDLER_H
#define HYPERTABLE_APPLICATIONHANDLER_H

extern "C" {
#include <time.h>
}

#include <ctime>
#include <boost/shared_ptr.hpp>

#include "Event.h"
#include "ReactorRunner.h"

namespace Hypertable {

  /** Abstract base clase for application request handlers.  Objects of this
   * type are what get added to an ApplicationQueue.  Most application
   * requests are generated via MESSAGE events received from the Comm layer.
   * The application queue supports serialization of requests for specific
   * application objects.  This is achieved by setting the gid field of
   * the message to a unique id associated with the application object
   * (e.g. file handle).  The MESSAGE event object that gets created when
   * a message arrives will create a 64-bit thread_group value that is the
   * combination of the connection ID and the gid field in the message
   * header.  This thread group value is used by the ApplicationQueue to
   * serialize requests.
   */
  class ApplicationHandler {

  public:
    /** Initializes the handler object with the event object that generated
     * the request.
     *
     * @param event_ptr smart pointer to event object that generated the request
     */
    ApplicationHandler(EventPtr &event_ptr) : m_event_ptr(event_ptr) {
      if (m_event_ptr)
        m_urgent = (bool)(m_event_ptr->header.flags & CommHeader::FLAGS_BIT_URGENT);
      else
        m_urgent = false;
    }

    /** Initializes the handler object with NULL event object.
     */
    ApplicationHandler(bool urgent=false) : m_urgent(urgent) { }

    /** Destructor */
    virtual ~ApplicationHandler() { return; }

    /** Abstract method to carry out the request.  Called by an ApplicationQueue
     * worker thread
     */
    virtual void run() = 0;

    /** Returns the thread group that this request belongs to.  This value is
     * taken from the associated event object (see Event#thread_group).
     */
    uint64_t get_thread_group() {
      return (m_event_ptr) ?  m_event_ptr->thread_group : 0;
    }

    /** Returns true of the 'urgent' bit is set in the message header
     */
    bool is_urgent() { return m_urgent; }

    bool expired() {
      if (m_event_ptr && m_event_ptr->type == Event::MESSAGE &&
          ReactorRunner::record_arrival_time &&
          (m_event_ptr->header.flags & CommHeader::FLAGS_BIT_REQUEST)) {
        uint32_t wait_ms;
	time_t now = time(0);
	HT_ASSERT(now != (time_t)-1);
	if (now < m_event_ptr->arrival_time) {
	  HT_WARNF("time() returned %ld which is less than the arrival time of %ld",
		   (long int)now, (long int)m_event_ptr->arrival_time);
	  wait_ms = 0;
	}
	else
	  wait_ms = (now - m_event_ptr->arrival_time) * 1000;
        if (wait_ms >= m_event_ptr->header.timeout_ms) {
          if (m_event_ptr->header.flags & CommHeader::FLAGS_BIT_REQUEST)
            HT_WARNF("Request expired, wait time %u > timeout %u (now=%ld, arrival_time=%ld)",
		     (unsigned)wait_ms, m_event_ptr->header.timeout_ms,
		     (long int)now, (long int)m_event_ptr->arrival_time);
          else
            HT_WARNF("Response expired, wait time %u > timeout %u", (unsigned)wait_ms,
                 m_event_ptr->header.timeout_ms);

          if (m_event_ptr->header.timeout_ms == 0) {
            HT_INFO("Changing zero timeout request to 120000 ms");
            m_event_ptr->header.timeout_ms = 120000;
            return false;
          }

          return true;
        }
      }
      return false;
    }

  protected:
    EventPtr m_event_ptr;
    bool m_urgent;
  };

} // namespace Hypertable

#endif // HYPERTABLE_APPLICATIONHANDLER_H
