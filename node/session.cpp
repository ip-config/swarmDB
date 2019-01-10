// Copyright (C) 2018 Bluzelle
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License, version 3,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#include <node/session.hpp>
#include <sstream>

namespace
{
    //const std::chrono::seconds DEFAULT_WS_TIMEOUT_MS{10};
}


using namespace bzn;


session::session(std::shared_ptr<bzn::asio::io_context_base> io_context, const bzn::session_id session_id, std::shared_ptr<bzn::beast::websocket_stream_base> websocket, std::shared_ptr<bzn::chaos_base> chaos, const std::chrono::milliseconds& /*ws_idle_timeout*/)
    : strand(io_context->make_unique_strand())
    , session_id(session_id)
    , websocket(std::move(websocket))
    , idle_timer(io_context->make_unique_steady_timer())
    , chaos(std::move(chaos))
   // , ws_idle_timeout(ws_idle_timeout.count() ? ws_idle_timeout : DEFAULT_WS_TIMEOUT_MS)
{
}


void
session::start(bzn::message_handler handler, bzn::protobuf_handler proto_handler)
{
    this->handler = std::move(handler);
    this->proto_handler = std::move(proto_handler);

    // If we haven't completed a handshake then we are accepting one...
    if (!this->websocket->is_open())
    {
        std::lock_guard<std::mutex> lock(this->lock);
        this->websocket->async_accept(
            [self = shared_from_this()](boost::system::error_code ec)
            {
                if (ec)
                {
                    LOG(error) << "websocket accept failed: " << ec.message();
                    return;
                }

                // schedule read...
                self->do_read();
            }
        );
    }
    else
    {
        this->do_read();
    }
}


void
session::do_read()
{
    auto buffer = std::make_shared<boost::beast::multi_buffer>();
    std::lock_guard<std::mutex> lock(this->lock);

    //this->start_idle_timeout();

    // todo: strand may not be needed...
    this->websocket->async_read(*buffer,
        [self = shared_from_this(), buffer](boost::system::error_code ec, auto /*bytes_transferred*/)
        {
            //self->idle_timer->cancel();

            if (ec)
            {
                // don't log close of websocket...
                if (ec != boost::beast::websocket::error::closed && ec != boost::asio::error::eof)
                {
                    LOG(error) << "websocket read failed: " << ec.message();
                }
                return;
            }

            // get the message...
            std::stringstream ss;
            ss << boost::beast::buffers(buffer->data());

            Json::Value msg;
            Json::Reader reader;

            bzn_envelope proto_msg;

            if (reader.parse(ss.str(), msg))
            {
                self->handler(msg, self);
            }
            else if (proto_msg.ParseFromIstream(&ss))
            {
                self->proto_handler(proto_msg, self);
            }
            else
            {
                LOG(error) << "Failed to parse: " << reader.getFormattedErrorMessages();
            }

            self->do_read();
        });
}


void
session::send_message(std::shared_ptr<bzn::json_message> msg, const bool end_session)
{
    this->send_message(std::make_shared<bzn::encoded_message>(msg->toStyledString()), end_session);
}


void
session::send_message(std::shared_ptr<bzn::encoded_message> msg, const bool end_session)
{
    if (this->chaos->is_message_delayed())
    {
        this->chaos->reschedule_message(std::bind(static_cast<void(session::*)(std::shared_ptr<std::string>, const bool)>(&session::send_message), shared_from_this(), std::move(msg), end_session));
        return;
    }

    if (this->chaos->is_message_dropped())
    {
        return;
    }

    //this->idle_timer->cancel(); // kill timer for duration of write...

    std::lock_guard<std::mutex> lock(this->lock);
    this->write_queue.push_back(msg);
    if (!this->writing)
    {
        this->do_write();
    }
}

void session::do_write()
{
    // caller holds this->lock
    this->writing = true;

    auto msg = this->write_queue.front();
    this->write_queue.pop_front();

    this->websocket->get_websocket().binary(true);
    this->websocket->async_write(boost::asio::buffer(*msg),
        [self = shared_from_this()](boost::beast::error_code ec, size_t /*bytes*/)
        {
            if (ec)
            {
                LOG(error) << "websocket write failed: " << ec.message();

                self->close();
                return;
            }

            std::lock_guard<std::mutex> lock(self->lock);
            if (self->write_queue.size() > 0)
            {
                // holding lock for do_write to manipulate the queue and call async_write
                self->do_write();
            }
            else
            {
                self->writing = false;
            }


        });
}


void
session::send_datagram(std::shared_ptr<bzn::encoded_message> msg)
{
    this->send_message(msg, false);

//    if (this->chaos->is_message_delayed())
//    {
//        this->chaos->reschedule_message(std::bind(&session::send_datagram, shared_from_this(), std::move(msg)));
//        return;
//    }

//    if (this->chaos->is_message_dropped())
//    {
//        return;
//    }

//    std::lock_guard<std::mutex> lock(this->write_lock);

//    this->websocket->get_websocket().binary(true);

//    boost::beast::error_code ec;
//    this->websocket->write(boost::asio::buffer(*msg), ec);

//    if (ec)
//    {
//        LOG(error) << "websocket write failed: " << ec.message();
//        this->close();
//    }
}


void
session::close()
{
    //this->idle_timer->cancel();
    std::lock_guard<std::mutex> lock(this->lock);

    if (!this->closing && this->websocket->is_open())
    {
        this->closing = true;
        this->websocket->async_close(boost::beast::websocket::close_code::normal,
            [self = shared_from_this()](auto ec)
            {
                if (ec)
                {
                    LOG(error) << "failed to close websocket: " << ec.message();
                }
            });
    }
}


//void
//session::start_idle_timeout()
//{
//    this->idle_timer->cancel();

//    LOG(trace) << "resetting " << this->ws_idle_timeout.count() << "ms idle timer";

//    this->idle_timer->expires_from_now(this->ws_idle_timeout);

//    this->idle_timer->async_wait(
//        [self = shared_from_this()](auto ec)
//        {
//            if (!ec)
//            {
//#if 0
//                LOG(info) << "reached idle timeout -- closing session: " << ec.message();

//                self->websocket->async_close(boost::beast::websocket::close_code::normal,
//                    [self](auto ec)
//                    {
//                        if (ec)
//                        {
//                            LOG(error) << "failed to close websocket: " << ec.message();
//                        }
//                    });
//#else
//                LOG(info) << "reached idle timeout -- closing session disabled: " << ec.message();
//#endif
//                return;
//            }
//        });
//}

bool
session::is_open() const
{
    return this->websocket->is_open();
}
